test=${1:-cardinalityestimator}
members=${2:-2}
duration=${3:-10}

echo "Starting coordinator in interactive mode..."
nohup coordinator &
echo "Coordinator started."

sleep 10s

echo "Starting initial members..."
coordinator-remote worker-start --count ${members}
echo "Starting initial members completed."

echo "Starting test for : " ${test}
test_id=$(coordinator-remote test-start --duration ${duration}m ${test}.properties)
echo "Starting test completed."

sleep 30s

test_status=$(coordinator-remote test-status ${test_id})

while [ "${test_status}" == "run" ]
do
    test_status=$(coordinator-remote test-status ${test_id})
    echo $(date +%T) "Test running with status : " ${test_status}
    sleep 60s
done

echo $(date +%T) " - Stopping Test." 

coordinator-remote test-stop $test_id    

echo $(date +%T) " - Test Completed for : " ${test}    

coordinator-remote stop