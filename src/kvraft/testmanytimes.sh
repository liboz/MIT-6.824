#!/bin/bash
counter=1
while [ $counter -le 100 ]
do
echo "Iteration: $counter"
go test -run 3A -race
if [ $? -eq 1 ]; then 
    exit 0
fi
((counter++))
done