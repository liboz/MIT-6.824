#!/bin/bash
counter=1
while [ $counter -le 100 ]
do
echo "Iteration: $counter"
sh ./test-mr.sh
if [ $? -eq 1 ]; then 
    exit 0
fi
((counter++))
done