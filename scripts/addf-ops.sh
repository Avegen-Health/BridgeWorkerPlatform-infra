#!/usr/bin/env bash
#
# ADDF gated operator actions (implementation-plan §3b.4).
#
# Runs ONLY inside the CI runner, which holds the deploy credentials. The plan's
# read-only-CLI rule forbids these writes from a developer laptop, and this script
# is how the "gated CI job" option in §3b.4 is satisfied.
#
# It is never triggered by a normal push. The Travis stage that calls it is guarded
# on an env var that only an API-triggered build supplies:
#
#   travis restart / API build with   ADDF_BACKFILL=uat    -> kick off the backfill
#   travis restart / API build with   ADDF_REPUBLISH=uat   -> force today's re-publish
#
# Add ADDF_DRY_RUN=true to either to preflight permissions and print the intended
# action WITHOUT sending or deleting anything. Do this first: the deploy credentials
# exist for CloudFormation/sceptre, so sqs:SendMessage and s3:DeleteObject are not
# obviously covered, and a dry run answers that in a build instead of half-way
# through a real backfill.
#
# All recognised variables:
#   ADDF_BACKFILL=uat        trigger the whole-app participant-version backfill
#   ADDF_REPUBLISH=uat       delete today's publish marker to force a re-publish
#   ADDF_DRY_RUN=true        preflight only; perform nothing
#   ADDF_FORCE=true          proceed despite a VisibilityTimeout below the floor
#   ADDF_SNAPSHOT_DATE=YYYY-MM-DD   republish a day other than today (UTC)
# Setting both trigger variables in one build is refused -- republish must run only
# after the backfill has finished, and the worker enumerates asynchronously.
#
# NOT gated by AddfExportEnabled. That flag is read only by the BS2 enqueuer; no BWP
# worker consults it (verified: no addf.export.enabled lookup in the accumulate,
# dimension or backfill processors). So uat's AddfExportEnabled: 'false' in
# config/uat/bridgeworker.yaml does NOT make this job a no-op.
#
# IF A RUN MISBEHAVES: an SQS message cannot be un-sent. The backfill only ever
# enqueues AddfParticipantVersionWorker messages, and that worker presence-skips any
# (healthCode, version) it has already written, so a stray or duplicate run wastes
# Bridge API calls but cannot corrupt data. To stop one in flight, purge the ADDF
# request queue or scale the worker down; to confirm what ran, read the WorkerLog DDB
# table (workerId=AddfParticipantVersionBackfillWorker) -- runs record status=complete
# or status=FAILED with the account count reached.
#
# KNOWN LIMITATION -- no re-entrancy guard. Two concurrent triggers (including
# Travis's "restart build", which replays the original env vars) each send their own
# kickoff and run two overlapping enumerations. They share the worker's 10rps limiter,
# so both run slower and are likelier to outlive the visibility window. Data stays
# correct via the presence-skip above. Check for a running backfill before triggering.
#
# WHY THE BACKFILL MATTERS
#   Taking an assessment emits no participant-version event, so a pre-existing
#   participant's NEW uploads carry a participant_version that ADDF has never seen.
#   The publish step defers those rows as orphans indefinitely (§3b.5), so such a
#   participant never reaches ADDF until this backfill has run once.
#
# WHY REPUBLISH EXISTS
#   Publish is idempotent on biaffect-3/_publish/<date>.done and short-circuits
#   while that marker exists, so at most one real publish happens per UTC day.
#   After a backfill you usually want the newly un-orphaned rows shipped now
#   rather than tomorrow -- deleting the marker lets the next tick rebuild.

set -euo pipefail

# Floor for the target queue's VisibilityTimeout, matched to the ADDF request queue's
# own configured value (VisibilityTimeout in config/<env>/addf-export.yaml). Anything
# below this means the kickoff is on a queue not sized for this job -- which is exactly
# what a dry run caught when the kickoff still targeted the 120s general worker queue.
# At the worker's 10 participants/sec, 300s covers ~3000 participants; override with
# ADDF_FORCE=true when the population is known to be comfortably under the window.
MIN_VISIBILITY_SECONDS=300

# ---------------------------------------------------------------- env profiles
env_profile() {
  case "$1" in
    uat)
      AWS_REGION_="us-east-1"
      ACCOUNT="433864969993"
      APP_ID="biaffect-3"
      # The whole-app backfill must finish inside ONE message's visibility window,
      # so the kickoff goes to whichever queue gives the longest one.
      #
      # It used to go to the general worker queue on the assumption that the queue
      # hosting the other long-running jobs would have a generous timeout. A dry run
      # measured it: Bridge-WorkerPlatform-Request-staging is 120s -- roughly 1200
      # participants at the worker's 10/sec. The ADDF request queue is 300s
      # (VisibilityTimeout in config/uat/addf-export.yaml), i.e. 2.5x the headroom,
      # and unlike the general queue it is managed in this repo, so it can be raised
      # deliberately and reviewably if a population ever needs more.
      #
      # Both queues' pollers share BridgeWorkerPlatformSqsCallback and dispatch by
      # service name, so either delivers the message to the same worker.
      TARGET_QUEUE="Bridge-ADDF-Export-Request-uat"
      EXPORT_STORE="org-gvbridge-addf-exportstore-uat"
      ;;
    *)
      echo "ERROR: unsupported or unguarded env '$1'." >&2
      echo "  prod is deliberately excluded: a prod backfill must be a separate," >&2
      echo "  explicitly reviewed change, not a env-var flip on this job." >&2
      exit 1
      ;;
  esac
}

require_aws() {
  command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI not on PATH" >&2; exit 1; }
  CALLER_ARN="$(aws sts get-caller-identity --region "$AWS_REGION_" --query Arn --output text 2>/dev/null)" \
    || { echo "ERROR: no usable AWS credentials in the runner" >&2; exit 1; }
  local actual
  actual="$(aws sts get-caller-identity --region "$AWS_REGION_" --query Account --output text)"
  if [ "$actual" != "$ACCOUNT" ]; then
    echo "ERROR: runner is in account $actual but $TARGET_ENV expects $ACCOUNT." >&2
    echo "  Refusing to act across accounts." >&2
    exit 1
  fi
  echo "Runner identity: $CALLER_ARN (account $actual)"
}

# IAM's simulator answers "would this principal be allowed?" without performing the
# call. The deploy credentials exist for CloudFormation/sceptre, so the two writes
# this job needs -- sqs:SendMessage and s3:DeleteObject -- are not obviously covered.
# Checking here turns a failed production run into a preflight line.
#
# policy-source-arn wants the user/role ARN, not an assumed-role session ARN, so
# collapse sts::...:assumed-role/Role/session -> iam::...:role/Role.
policy_source_arn() {
  case "$CALLER_ARN" in
    arn:aws:sts::*:assumed-role/*)
      local acct role
      acct="$(printf '%s' "$CALLER_ARN" | cut -d: -f5)"
      role="$(printf '%s' "$CALLER_ARN" | cut -d/ -f2)"
      printf 'arn:aws:iam::%s:role/%s' "$acct" "$role"
      ;;
    *) printf '%s' "$CALLER_ARN" ;;
  esac
}

# simulate one action; prints the decision, returns non-zero if not allowed
simulate() {
  local action="$1" resource="$2" decision
  decision="$(aws iam simulate-principal-policy \
      --policy-source-arn "$(policy_source_arn)" \
      --action-names "$action" \
      --resource-arns "$resource" \
      --query 'EvaluationResults[0].EvalDecision' --output text 2>/dev/null)" || {
    echo "  ?  $action on $resource -- could not simulate (principal lacks iam:SimulatePrincipalPolicy)"
    return 0   # inconclusive is not a failure; see the note printed by preflight
  }
  if [ "$decision" = "allowed" ]; then
    echo "  OK $action on $resource"
  else
    echo "  !! $action on $resource -> $decision"
    return 1
  fi
}

# Preflight the writes this job performs. Runs on every invocation: it is read-only
# and cheap, and a denial found here costs a build instead of a half-done backfill.
preflight() {
  local queue_arn="arn:aws:sqs:${AWS_REGION_}:${ACCOUNT}:${TARGET_QUEUE}"
  local marker_arn="arn:aws:s3:::${EXPORT_STORE}/biaffect-3/_publish/*"
  local rc=0

  echo "Preflight (IAM simulation -- nothing is sent or deleted):"
  case "$ACTION" in
    backfill)  simulate "sqs:SendMessage" "$queue_arn"   || rc=1 ;;
    republish) simulate "s3:DeleteObject" "$marker_arn"  || rc=1 ;;
  esac

  if [ "$rc" -ne 0 ]; then
    echo "ERROR: the runner is not permitted to perform this action." >&2
    echo "  Grant it on the Travis deploy principal $(policy_source_arn) and re-run." >&2
    exit 1
  fi
  echo "  (an inconclusive '?' above means the simulation itself was denied, not the action --"
  echo "   in that case ADDF_DRY_RUN=true still cannot prove the write will succeed)"
}

# ---------------------------------------------------------------- backfill
do_backfill() {
  local queue_url="https://sqs.${AWS_REGION_}.amazonaws.com/${ACCOUNT}/${TARGET_QUEUE}"

  # Whole-app mode: s3Key omitted, so the worker enumerates every account itself
  # via getAllAccountSummaries. No health-code list is produced or handled here.
  local body
  body="$(printf '{"service":"AddfParticipantVersionBackfillWorker","body":{"appId":"%s"}}' "$APP_ID")"

  echo "ADDF participant-version backfill"
  echo "  env      : $TARGET_ENV"
  echo "  app      : $APP_ID"
  echo "  queue    : $TARGET_QUEUE"
  echo "  mode     : whole-app (no s3Key -> worker enumerates all accounts)"
  echo "  message  : $body"

  # The run must finish inside one visibility window or SQS redelivers it and a
  # second concurrent enumeration starts; after maxReceiveCount it lands in a DLQ.
  local vis
  vis="$(aws sqs get-queue-attributes --region "$AWS_REGION_" --queue-url "$queue_url" \
          --attribute-names VisibilityTimeout \
          --query 'Attributes.VisibilityTimeout' --output text 2>/dev/null || echo "unknown")"
  echo "  queue VisibilityTimeout: ${vis}s"
  local window_too_small=false
  if [ "$vis" != "unknown" ] && [ "$vis" -lt "$MIN_VISIBILITY_SECONDS" ] 2>/dev/null; then
    window_too_small=true
    echo "  At ~10 participants/sec this window covers only ~$((vis * 10)) participants."
  fi

  # Dry run reports and exits 0 -- including the window verdict. Failing the build here
  # would conflate "you lack permissions" with "the window is short", and a dry run's
  # whole job is to tell you everything without performing anything.
  if [ "${ADDF_DRY_RUN:-false}" = "true" ]; then
    if [ "$window_too_small" = "true" ] && [ "${ADDF_FORCE:-false}" != "true" ]; then
      echo "  NOTE: a real run would REFUSE -- VisibilityTimeout ${vis}s is below"
      echo "        ${MIN_VISIBILITY_SECONDS}s. Re-run with ADDF_FORCE=true once you have"
      echo "        confirmed the app has well under ~$((vis * 10)) participants."
    fi
    echo "  DRY RUN -- not sending."
    return 0
  fi

  if [ "$window_too_small" = "true" ] && [ "${ADDF_FORCE:-false}" != "true" ]; then
    echo "ERROR: VisibilityTimeout ${vis}s is below ${MIN_VISIBILITY_SECONDS}s -- refusing to send." >&2
    echo "  If the enumeration outlives the window, SQS redelivers it and a SECOND" >&2
    echo "  concurrent walk starts; after maxReceiveCount it lands in a DLQ." >&2
    echo "  Either raise VisibilityTimeout on $TARGET_QUEUE, or, if the app is small" >&2
    echo "  enough to finish comfortably inside ${vis}s, re-run with ADDF_FORCE=true." >&2
    exit 1
  fi
  if [ "$window_too_small" = "true" ]; then
    echo "  ADDF_FORCE=true -- proceeding despite the short window."
  fi

  aws sqs send-message --region "$AWS_REGION_" \
    --queue-url "$queue_url" --message-body "$body" >/dev/null
  echo "  sent."
  echo
  echo "Follow progress in the worker log:"
  echo "  'Starting ADDF participant-version backfill for app $APP_ID over ALL accounts'"
  echo "  'Finished ADDF participant-version backfill: ... mode=allAccounts ...'"
  echo "Completion is also recorded in the WorkerLog DDB table"
  echo "  (workerId=AddfParticipantVersionBackfillWorker)."
}

# ---------------------------------------------------------------- republish
do_republish() {
  local date_utc marker
  date_utc="${ADDF_SNAPSHOT_DATE:-$(date -u +%F)}"
  marker="biaffect-3/_publish/${date_utc}.done"

  echo "ADDF force re-publish"
  echo "  env      : $TARGET_ENV"
  echo "  bucket   : $EXPORT_STORE"
  echo "  marker   : $marker"

  # Assert the bucket first. head-object returns 404 both for "marker not published
  # yet" and for "this bucket does not exist", so without this check a stale
  # EXPORT_STORE value would print "nothing to delete" and exit 0 -- a republish that
  # silently never happened, with no failing build to notice it.
  if ! aws s3api head-bucket --region "$AWS_REGION_" --bucket "$EXPORT_STORE" >/dev/null 2>&1; then
    echo "ERROR: export-store bucket '$EXPORT_STORE' is not reachable." >&2
    echo "  Either the name drifted from config/${TARGET_ENV}/bridgeworker.yaml or the" >&2
    echo "  runner cannot see it. Refusing to report 'nothing to delete' for a bucket" >&2
    echo "  that may not exist." >&2
    exit 1
  fi

  if ! aws s3api head-object --region "$AWS_REGION_" \
        --bucket "$EXPORT_STORE" --key "$marker" >/dev/null 2>&1; then
    echo "  marker absent -- nothing to delete; the next tick will publish anyway."
    return 0
  fi

  if [ "${ADDF_DRY_RUN:-false}" = "true" ]; then
    echo "  DRY RUN -- not deleting."
    return 0
  fi

  aws s3api delete-object --region "$AWS_REGION_" \
    --bucket "$EXPORT_STORE" --key "$marker" >/dev/null
  echo "  deleted; the next scheduler tick will rebuild and re-upload the snapshot."
  echo "  Watch for 'ADDF publish succeeded'."
}

# ---------------------------------------------------------------- dispatch
if [ -n "${ADDF_BACKFILL:-}" ] && [ -n "${ADDF_REPUBLISH:-}" ]; then
  echo "ERROR: both ADDF_BACKFILL and ADDF_REPUBLISH are set." >&2
  echo "  These are two separate builds, deliberately. Republish must run only AFTER" >&2
  echo "  the backfill has finished -- the worker enumerates asynchronously, so a" >&2
  echo "  republish fired in the same build would snapshot a half-filled dimension" >&2
  echo "  table. Trigger the backfill, wait for status=complete, then republish." >&2
  exit 1
fi

if [ -n "${ADDF_BACKFILL:-}" ]; then
  TARGET_ENV="$ADDF_BACKFILL"
  ACTION="backfill"
elif [ -n "${ADDF_REPUBLISH:-}" ]; then
  TARGET_ENV="$ADDF_REPUBLISH"
  ACTION="republish"
else
  echo "ERROR: neither ADDF_BACKFILL nor ADDF_REPUBLISH is set." >&2
  echo "  This job is meant to run only from an API-triggered build that sets one." >&2
  exit 1
fi

env_profile "$TARGET_ENV"
require_aws
preflight

case "$ACTION" in
  backfill)  do_backfill ;;
  republish) do_republish ;;
esac
