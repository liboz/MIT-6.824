#!/bin/bash
counter=1
while [ $counter -le 100 ]
do
echo $counter
go test -run 2C -race
if [ $? -eq 1 ]; then 
    exit 0
fi
((counter++))
done