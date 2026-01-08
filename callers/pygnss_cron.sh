#!/bin/bash
#------------------------------------------------------------------------------
# PyGNSS-RT Cron Wrapper Script
# Replacement for Perl-based cron_job
#
# Usage in crontab:
#   # Hourly processing at minute 45 (with 3-hour latency for final products)
#   45 * * * * /home/ahunegnaw/Python_IGNSS/i-GNSS/callers/pygnss_cron.sh hourly 3 >> /var/log/pygnss/hourly.log 2>&1
#
#   # Sub-hourly processing every 15 minutes (with 1-hour latency for rapid products)
#   */15 * * * * /home/ahunegnaw/Python_IGNSS/i-GNSS/callers/pygnss_cron.sh subhourly 1 >> /var/log/pygnss/subhourly.log 2>&1
#
#   # Daily processing at 06:00 (with 24-hour latency for final products)
#   0 6 * * * /home/ahunegnaw/Python_IGNSS/i-GNSS/callers/pygnss_cron.sh daily 24 >> /var/log/pygnss/daily.log 2>&1
#------------------------------------------------------------------------------

# Exit on error
set -e

# Configuration
PYGNSS_HOME="/home/ahunegnaw/Python_IGNSS/i-GNSS"
VENV_PATH="${PYGNSS_HOME}/venv"
CONFIG_FILE="${PYGNSS_HOME}/config/settings.yaml"
LOG_DIR="/var/log/pygnss"

# Processing type and latency from arguments
PROC_TYPE="${1:-hourly}"
LATENCY="${2:-3}"

# Create log directory if needed
mkdir -p "${LOG_DIR}"

# Timestamp for logging
echo "=========================================="
echo "PyGNSS-RT Processing Started"
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Type: ${PROC_TYPE}"
echo "Latency: ${LATENCY} hours"
echo "=========================================="

# Activate Python virtual environment
if [ -f "${VENV_PATH}/bin/activate" ]; then
    source "${VENV_PATH}/bin/activate"
else
    echo "ERROR: Virtual environment not found at ${VENV_PATH}"
    exit 1
fi

# Set up environment for Bernese GNSS Software (if needed)
# Uncomment and adjust these paths based on your BSW installation
# export BERN54_HOME="/path/to/BERN54"
# export GPSUSER="/path/to/GPSUSER"
# export LD_LIBRARY_PATH="${BERN54_HOME}/LIB:${LD_LIBRARY_PATH}"

# Change to project directory
cd "${PYGNSS_HOME}"

# Run the processing
echo "Running: pygnss-rt process --cron --type ${PROC_TYPE} --latency ${LATENCY} --config ${CONFIG_FILE}"
pygnss-rt process \
    --cron \
    --type "${PROC_TYPE}" \
    --latency "${LATENCY}" \
    --config "${CONFIG_FILE}"

# Capture exit status
STATUS=$?

echo "=========================================="
echo "Processing Completed"
echo "Exit Status: ${STATUS}"
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

exit ${STATUS}
