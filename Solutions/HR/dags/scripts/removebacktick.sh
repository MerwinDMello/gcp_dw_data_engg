#!/bin/bash
###########################################
### Remove back tick(`) from the file passed ###
###########################################
filenm=$1
prefix="$(echo $filenm | cut -d'.' -f 1)"
cat $filenm | tr -d '`' > "${prefix}_temp.txt"
mv "${prefix}_temp.txt" $filenm
sleep 15