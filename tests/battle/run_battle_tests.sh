#!/bin/bash
# Run all battle tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Build release binary
echo "Building release binary..."
cargo build --release --manifest-path ../../Cargo.toml

# Parse arguments
DURATION=${1:-120}  # Default 2 minutes for quick runs
FULL_DURATION=600   # 10 minutes for full test

if [ "$1" == "--full" ]; then
    DURATION=$FULL_DURATION
    echo "Running FULL battle tests (${DURATION}s per test)..."
else
    echo "Running QUICK battle tests (${DURATION}s per test)..."
    echo "Use --full for longer tests"
fi

echo ""
echo "=========================================="
echo "=== 1. Concurrent Write Test ==="
echo "=========================================="
python3 concurrent_write_test.py -w 10 -n 200
CONCURRENT_RESULT=$?

echo ""
echo "=========================================="
echo "=== 2. Multi-Replica Test ==="
echo "=========================================="
python3 multi_replica_test.py -r 3 -n 300
MULTI_RESULT=$?

echo ""
echo "=========================================="
echo "=== 3. Chaos Test ==="
echo "=========================================="
python3 chaos_test.py -d "$DURATION" -i 10 -m 15
CHAOS_RESULT=$?

echo ""
echo "=========================================="
echo "=== 4. Endurance Test ==="
echo "=========================================="
python3 endurance_test.py -d "$DURATION" -i 50
ENDURANCE_RESULT=$?

echo ""
echo "=========================================="
echo "=== 5. Sidecar Restore Scenario ==="
echo "=========================================="
python3 sidecar_restore_test.py
SIDECAR_RESTORE_RESULT=$?

echo ""
echo "=========================================="
echo "=== 6. Sidecar Retry Scenario ==="
echo "=========================================="
python3 sidecar_retry_test.py
SIDECAR_RETRY_RESULT=$?

echo ""
echo "=========================================="
echo "=== 7. Sidecar Recover Scenario ==="
echo "=========================================="
python3 sidecar_recover_test.py
SIDECAR_RECOVER_RESULT=$?

echo ""
echo "=========================================="
echo "BATTLE TEST SUMMARY"
echo "=========================================="
echo "Concurrent Write: $([ $CONCURRENT_RESULT -eq 0 ] && echo 'PASS ✓' || echo 'FAIL ✗')"
echo "Multi-Replica:    $([ $MULTI_RESULT -eq 0 ] && echo 'PASS ✓' || echo 'FAIL ✗')"
echo "Chaos:            $([ $CHAOS_RESULT -eq 0 ] && echo 'PASS ✓' || echo 'FAIL ✗')"
echo "Endurance:        $([ $ENDURANCE_RESULT -eq 0 ] && echo 'PASS ✓' || echo 'FAIL ✗')"
echo "Sidecar Restore:  $([ $SIDECAR_RESTORE_RESULT -eq 0 ] && echo 'PASS ✓' || echo 'FAIL ✗')"
echo "Sidecar Retry:    $([ $SIDECAR_RETRY_RESULT -eq 0 ] && echo 'PASS ✓' || echo 'FAIL ✗')"
echo "Sidecar Recover:  $([ $SIDECAR_RECOVER_RESULT -eq 0 ] && echo 'PASS ✓' || echo 'FAIL ✗')"
echo "=========================================="

# Exit with failure if any test failed
if [ $CONCURRENT_RESULT -ne 0 ] || [ $MULTI_RESULT -ne 0 ] || [ $CHAOS_RESULT -ne 0 ] || [ $ENDURANCE_RESULT -ne 0 ] || [ $SIDECAR_RESTORE_RESULT -ne 0 ] || [ $SIDECAR_RETRY_RESULT -ne 0 ] || [ $SIDECAR_RECOVER_RESULT -ne 0 ]; then
    echo "OVERALL: FAIL ✗"
    exit 1
else
    echo "OVERALL: PASS ✓"
    exit 0
fi
