#!/bin/bash

# Hazelcast Simulator Container Build Script
# This script builds the Hazelcast Simulator container using pre-built artifacts

set -e

# Color output functions
red() { echo -e "\033[31m$1\033[0m"; }
green() { echo -e "\033[32m$1\033[0m"; }
yellow() { echo -e "\033[33m$1\033[0m"; }
blue() { echo -e "\033[34m$1\033[0m"; }

# Configuration
IMAGE_NAME="hazelcast/simulator"
IMAGE_TAG="${1:-latest}"
DOCKERFILE="./Dockerfile"
BUILD_CONTEXT="."

echo "$(blue '==========================================')"
echo "$(blue 'Hazelcast Simulator Container Builder')"
echo "$(blue '==========================================')"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    red "Error: Docker is not installed or not in PATH"
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f "$DOCKERFILE" ]]; then
    red "Error: $DOCKERFILE not found in current directory"
    red "Please run this script from the hazelcast-simulator root directory"
    exit 1
fi

# Check if pre-built artifacts exist
if [[ ! -d "lib" ]] || [[ ! -d "drivers" ]]; then
    yellow "Warning: Pre-built artifacts (lib/ or drivers/) not found"
    yellow "Building Java components first..."
    
    # Try to build Java components
    if [[ -f "./build" ]]; then
        chmod +x ./build
        echo "$(yellow 'Running local build script...')"
        if ! ./build; then
            red "Error: Local build failed. Please build the Java components manually."
            red "You can try:"
            red "  1. Run './build' with proper Maven credentials"
            exit 1
        fi
    else
        red "Error: No build script found and no pre-built artifacts available"
        exit 1
    fi
fi

echo "$(yellow 'Building container image...')"
echo "Image: $IMAGE_NAME:$IMAGE_TAG"
echo "Dockerfile: $DOCKERFILE"
echo "Build context: $BUILD_CONTEXT"
echo

# Build the container
echo "$(blue 'Starting Docker build...')"

# Build with BuildKit for better performance
DOCKER_BUILDKIT=1 docker build \
    -t "$IMAGE_NAME:$IMAGE_TAG" \
    -f "$DOCKERFILE" \
    .

if [[ $? -eq 0 ]]; then
    echo
    green "Container build completed successfully!"
    echo
    echo "$(blue 'Image details:')"
    docker images "$IMAGE_NAME:$IMAGE_TAG"
    echo
    echo "$(blue 'Next steps:')"
    echo "  Test the container: docker run --rm -it $IMAGE_NAME:$IMAGE_TAG"
    echo "  Run with workspace: docker run --rm -it -v \$(pwd):/workspace $IMAGE_NAME:$IMAGE_TAG"
    echo "  Use docker-compose: docker-compose run --rm simulator"
    echo
    echo "$(blue 'Verify simulator commands:')"
    echo "  docker run --rm $IMAGE_NAME:$IMAGE_TAG perftest --help"
    echo "  docker run --rm $IMAGE_NAME:$IMAGE_TAG inventory --help"
else
    red "Container build failed!"
    exit 1
fi