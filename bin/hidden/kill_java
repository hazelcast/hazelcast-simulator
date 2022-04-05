#!/bin/bash

# exit on failure
set -e

if hash killall 2>/dev/null; then
  killall -9 -q java || true
else
  pkill -9 java || true
fi
