members=2
clients=1

echo "Starting coordinator in interactive mode..."
nohup coordinator &
echo "Coordinator started."

sleep 10s

echo "Starting initial members..."
coordinator-remote worker-start --count ${members}
echo "Starting initial members completed."

echo "Starting client members..."
coordinator-remote worker-start --count ${clients} --workerType javaclient
echo "Starting initial members completed."

echo "Starting test..."
coordinator-remote test-start --duration 30m test.properties
echo "Starting test completed. Sleeping for 60s"
sleep 60s


for i in {1..10}
do
   echo $(date +%T) " - QUORUM MEMBER JOINING"
   worker_address=$(coordinator-remote worker-start)
   echo $(date +%T) " - QUORUM MEMBER JOINED WITH ADDRESS : " ${worker_address} 
   
   sleep 60s
   
   echo $(date +%T) " - QUORUM MEMBER LEAVING WITH ADDRESS : " ${worker_address}
   coordinator-remote worker-kill --workers ${worker_address}
   echo $(date +%T) " - QUORUM MEMBER LEFT WITH ADDRESS : " ${worker_address}

   sleep 60s
done

coordinator-remote stop