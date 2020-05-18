#!/bin/bash
counter=1
while [ $counter -le 100 ]
do
echo $counter
sh ./test-mr.sh
((counter++))
done