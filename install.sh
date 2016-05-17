#
# When invoked from the hazelcast-simulator source root installs the dist/target/hazelcast-simulator*tar.gz
# to the specified target folder.
# If there's already the same version installed in the target folder it is backed up.
#

#!/bin/bash
set -e

echo "Running simulator installer"

if [ $# -ne 1 ]; then
    echo "Usage: install.sh <target_folder>"
    exit 1
fi

target="$1"
timestamp="$( date +%Y%m%d_%H%M%S )"
temp="$target/temp.$timestamp"

if [ ! -d "$target" ]; then
    echo "[ERROR] Target folder does not exist!"
    exit 1
fi


function cleanup {
    echo "Cleaning up"
    rm -rf "$temp"
}
trap cleanup EXIT


package=$( ls dist/target/*.tar.gz )

if [ ! -f "$package" ]; then
    echo "[ERROR] Could not find tar.gz dist to install!"
    exit 1
fi

echo "Extracting $package"
mkdir "$temp"
tar -xf "$package" -C "$temp"

dist=$( ls -d "$temp"/hazelcast-simulator-* )
distName=${dist#${temp}/}

if [ -d "$target/$distName" ]; then
    echo "Backing up $target/$distName"
    mv "$target/$distName" "$target/$distName.$timestamp"
fi

echo "Installing $distName"
cp -R "$dist" "$target/$distName"

echo "Success!"