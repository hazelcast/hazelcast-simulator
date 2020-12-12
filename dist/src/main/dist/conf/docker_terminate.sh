#!/bin/bash

# comma separated list of private ip's of the agents to destroy
private_ips=$1
# comma separated list of public ip's of the agents to destroy.
public_ips=$2

for ip in ${private_ips//,/ }; do
  all_containers=$(docker ps --filter "label=simulator" -q)
  to_terminate=$(docker inspect -f '{{.ID}},{{.NetworkSettings.IPAddress }}' $all_containers|grep $ip)
  container=$(cut -d',' -f1 <<< "$to_terminate")
  echo "[INFO]     Terminating Container $container"
  docker kill "$container"
done