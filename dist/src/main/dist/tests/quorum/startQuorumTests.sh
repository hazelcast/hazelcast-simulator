members=2

echo "Starting coordinator in interactive mode..."
nohup coordinator &
echo "Coordinator started."

sleep 10s

echo "Starting initial members..."
coordinator-remote worker-start --count ${members}
echo "Starting initial members completed."

echo "Starting test..."
test_id=$(coordinator-remote test-start --duration 30m test.properties)
echo "Starting test completed. Sleeping for 60s"
sleep 60s

test_status=$(coordinator-remote test-status ${test_id})
echo $(date +%T) "Test Status : " ${test_status}

for i in {1..10}
do
    
    test_status=$(coordinator-remote test-status ${test_id})
    
    if [ "${test_status}" == "run" ]
    then
       echo $(date +%T) " - QUORUM MEMBER JOINING"
       worker_address=$(coordinator-remote worker-start)
       echo $(date +%T) " - QUORUM MEMBER JOINED WITH ADDRESS : " ${worker_address} 

       sleep 60s

       echo $(date +%T) " - QUORUM MEMBER LEAVING WITH ADDRESS : " ${worker_address}
       coordinator-remote worker-kill --workers ${worker_address}
       echo $(date +%T) " - QUORUM MEMBER LEFT WITH ADDRESS : " ${worker_address}

       sleep 60s
    fi
done

test_status=$(coordinator-remote test-status ${test_id})

echo $(date +%T) " - Test Status : " ${test_status}
    
    while [ "${test_status}" == "run" ]
    do
        echo $(date +%T) " - Test in Progress with status : " ${test_status}
        sleep 10s
        test_status=$(coordinator-remote test-status ${test_id})
    done
    
echo $(date +%T) " - Test Completed."    

coordinator-remote stop