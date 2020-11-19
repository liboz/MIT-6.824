#!/bin/bash
counter=1
while [ $counter -le 1000 ]
do
echo "Iteration: $counter"
go test -race -run JoinLeave > b.txt
if [ $? -eq 1 ]; then 
    exit 0
fi
((counter++))
done 