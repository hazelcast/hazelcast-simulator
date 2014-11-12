#!/bin/bash

list=$(find . -maxdepth 1 -name "*[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]__[0-9][0-9]_[0-9][0-9]_[0-9][0-9]*")

IFS=$'\n'

for item in $list
do
    dir=$(echo "${item}" | grep -o [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]__[0-9][0-9]_[0-9][0-9]_[0-9][0-9])

    fullPath=$(find -maxdepth 9 -type d -name ${dir} | head -n1)

	mv ${item} ${fullPath}
done


#dir=$(pwd)
#base=$(basename ${dir})
#cd ..

#zip results.zip ${base} -r -x agents.txt *.hprof
#mv results.zip ${base}/
