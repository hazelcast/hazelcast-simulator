#!/bin/bash

set -e

# all the libs we need we copy to the upload directory. They will automatically be uploaded and put on the classpath
# of the worker
mkdir -p upload
cp ../target/*.jar upload

coordinator     --members 2 \
                --memberArgs "-Xms2G -Xmx2G" \
                --clients 2 \
                --clientArgs "-Xms2G -Xmx2G" \
                --duration 5m \
                test.properties