#!/bin/bash
#
# Daily multi-source pipeline wrapper for cron.
#
# Usage (crontab):
#   0 2 * * * /root/dhis2_pipeline/cron_wrapper.sh
#
# Exit codes:
#   0 — pipeline succeeded
#   1 — pipeline failed
#

set -euo pipefail

PIPELINE_DIR="/root/dhis2_pipeline"
LOG_DIR="${PIPELINE_DIR}/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/cron_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"
find "$LOG_DIR" -name "cron_*.log" -mtime +30 -delete 2>/dev/null || true

cd "$PIPELINE_DIR"
source venv/bin/activate

# --- Run the pipeline ---
# NOTE: EXIT_CODE is initialized to 0 so `set -u` doesn't fail
# when the pipeline succeeds on the first try.
EXIT_CODE=0

{
    echo "=== Pipeline started at $(date) ==="
    echo "User: $(whoami)"
    echo "Directory: $PIPELINE_DIR"
    echo ""

    if python run_pipeline_multi.py --incremental >> "${LOG_DIR}/pipeline_multi.log" 2>&1; then
        echo "=== Pipeline SUCCESS at $(date) ==="
    else
        EXIT_CODE=$?
        echo "=== Pipeline FAILED at $(date) (exit=$EXIT_CODE) ==="
    fi

    echo ""
    echo "--- Last 20 lines of pipeline log ---"
    tail -20 "${LOG_DIR}/pipeline_multi.log" || true
} | tee "$LOG_FILE"

# --- Optional notifications on failure ---
if [ "$EXIT_CODE" -ne 0 ]; then
    # Uncomment and configure one of these:
    #
    # 1. Email
    # mail -s "DHIS2 Pipeline FAILED" ops@example.com < "$LOG_FILE"
    #
    # 2. Slack webhook
    # curl -X POST -H 'Content-type: application/json' \
    #   --data "{\"text\":\"DHIS2 Pipeline FAILED — see $LOG_FILE\"}" \
    #   https://hooks.slack.com/services/YOUR/WEBHOOK
    #
    # 3. Discord
    # curl -X POST -H 'Content-type: application/json' \
    #   --data "{\"content\":\"DHIS2 Pipeline FAILED — see $LOG_FILE\"}" \
    #   https://discord.com/api/webhooks/YOUR_WEBHOOK
    :
fi

exit $EXIT_CODE
