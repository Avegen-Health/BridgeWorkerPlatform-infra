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

# ---------------------------------------------------------------- env profiles
env_profile() {
  case "$1" in
    uat)
      AWS_REGION_="us-east-1"
      ACCOUNT="433864969993"
      APP_ID="biaffect-3"
      # The whole-app backfill runs for minutes inside ONE message's visibility
      # window. It is sent to the general worker queue (which hosts the other
      # long-running jobs) rather than the ADDF queue, whose VisibilityTimeout is
      # 300s -- too short for a large enumeration, which would redrive and DLQ.
      # uat maps to the -staging general queue, NOT -uat.
      TARGET_QUEUE="Bridge-WorkerPlatform-Request-staging"
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
  aws sts get-caller-identity --region "$AWS_REGION_" >/dev/null \
    || { echo "ERROR: no usable AWS credentials in the runner" >&2; exit 1; }
  local actual
  actual="$(aws sts get-caller-identity --region "$AWS_REGION_" --query Account --output text)"
  if [ "$actual" != "$ACCOUNT" ]; then
    echo "ERROR: runner is in account $actual but $TARGET_ENV expects $ACCOUNT." >&2
    echo "  Refusing to act across accounts." >&2
    exit 1
  fi
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
  if [ "$vis" != "unknown" ] && [ "$vis" -lt 900 ] 2>/dev/null; then
    echo "  WARNING: at ~10 participants/sec this window covers only ~$((vis * 10)) participants."
    echo "           If the app is larger than that, raise VisibilityTimeout on"
    echo "           $TARGET_QUEUE before running, or the job will redrive."
  fi

  if [ "${ADDF_DRY_RUN:-false}" = "true" ]; then
    echo "  DRY RUN -- not sending."
    return 0
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

case "$ACTION" in
  backfill)  do_backfill ;;
  republish) do_republish ;;
esac
