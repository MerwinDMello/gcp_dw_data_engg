#!/bin/bash
###########################################
### Remove ASCII 0 from the file passed ###
###########################################
filenm=$1
prefix="$(echo $filenm | cut -d'.' -f 1)"
cat $filenm | tr -d '\000' > "${prefix}_temp.txt"
mv "${prefix}_temp.txt" $filenm
sleep 15
