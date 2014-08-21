#!/bin/bash

list=$(find . -name "nohup-[0-9]*.out" -o -name "failures-[0-9]*.txt")

IFS=$'\n'

for item in $list
do
	dir=$(echo "${item}" | cut -d '-' -f2 | cut -d '.' -f1)

	mv ${item} workers/${dir}/
done

cp *.properties workers/

zip -r results.zip workers/*
