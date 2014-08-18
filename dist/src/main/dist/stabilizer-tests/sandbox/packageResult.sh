#!/bin/bash

list=$(find . -name "failures-*.txt")

IFS=$'\n'

for item in $list
do
	dir=$(echo "${item}" | cut -d '-' -f2 | cut -d '.' -f1)

	mv ${item} workers/${dir}/
done

cp *.properties workers/

zip -r results.zip workers/*