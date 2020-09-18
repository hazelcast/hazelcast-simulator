#!/bin/bash

agents=$1

upload_driver() {
  agent=$1
  rsync --checksum -avv -L -e "ssh $SSH_OPTIONS" \
    $SIMULATOR_HOME/$driver_dir/* $SIMULATOR_USER@$agent:hazelcast-simulator-$SIMULATOR_VERSION/$driver_dir/
}

if [ "$DRIVER" = "hazelcast-enterprise4" ]; then
  driver_dir=drivers/driver-hazelcast4
elif [ "$DRIVER" = "hazelcast-enterprise3" ]; then
  driver_dir=drivers/driver-hazelcast3
else
  driver_dir=drivers/driver-$DRIVER
fi

echo "[INFO]Uploading driver $DRIVER"

for agent in ${agents//,/ }; do
  echo "[INFO]     Uploading driver to $agent"
  upload_driver ${agent}
done

echo "[INFO]Finished Uploading driver $DRIVER"