#!/bin/bash
#########################################################
### Variable Declaration and Parameter Initialization ###
#########################################################
CURR_DT=$(date +%Y%m%d)
CURR_DTTM=$(date +%Y%m%d%H%M%S)
environment=$1
srcsys=$2
frequency=$3
done_file=$4
retry_count=$5
wait_time=$6

envupper=${environment^^}
key="GOOGLE_APPLICATION_CREDENTIALS_${envupper}"
COMMON_MOUNT="/opt/tdgcp/edwhr"
SRCDIR="$COMMON_MOUNT/$srcsys"
TEMPDIR="$COMMON_MOUNT/$srcsys/temp"
if [[ "$environment" == "prd" ]]; then
	environ="prod"
fi
GCSPATH="gs://eim-cs-da-gmek-5764-$environ/edwhrdata/srcfiles/$srcsys"
project="hca-hin-$environ-proc-hr"
filelist="$COMMON_MOUNT/scripts/${srcsys}_filelist.txt"
JSON_FRMT='{"date":"%s","environment":"%s","system":"VM-xrdclpapphin02a","script":"kronos_copyfiletogcs.sh","message":"%s","sourcesys":"%s","frequency":"%s"}'
rc=0

echo "keyfile=$key"
echo "gcspath=$GCSPATH"
echo "project=$project"

#####################################
### Scripts for File modification ###
#####################################
quotefixerscript="$COMMON_MOUNT/scripts/fixmissingdoublequotes.py"
filereorderscript="$COMMON_MOUNT/scripts/filereorderscript.py"
removeascii0script="$COMMON_MOUNT/scripts/removeascii0.sh"
removebacktickscript="$COMMON_MOUNT/scripts/removebacktick.sh"
removeasteriskscript="$COMMON_MOUNT/scripts/removeasterisk.sh"

GOOGLE_APPLICATION_CREDENTIALS_PRD="/etc/.keys/hca-at-cao-sa-mgmt-5787d7db850f.json" 
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS_PRD
message="Kronos\tfile\ttransfer\tscript\tstarted\texecution"
gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO

###########################  
### Check files arrived ###
###########################
grep -v '#' $filelist |
	while read line; do
		file_frequency="$(echo $line | cut -d'|' -f1)"
		if [[ "$file_frequency" == "$frequency" ]]; then
			filenm="$(echo $line | cut -d'|' -f2)"
				if [[ -f "$SRCDIR/$filenm" ]]; then
					for (( ; ; )); do
						bfr=$(stat -c%s "$SRCDIR/$filenm")
						sleep 60
						aftr=$(stat -c%s "$SRCDIR/$filenm")
						if [ $? -ne 0 ];then echo "File not present, looping again";break;fi 
							echo "File size at present: $aftr"
						if [ $bfr -eq $aftr ];then
							echo "**File arrived and file size matched exiting the loop, before vs after size : $bfr vs $aftr**"
							message="$SRCDIR/$filenm\t-\tfiles\tarrived"
							gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
						break 
						fi
						echo "File arrived but file size doesn't match looping again: Before vs after : $bfr vs $aftr"
					done
    			else
            		retry_count="$((retry_count - 1))"
					if [[ $retry_count -eq 0 ]]; then
						message="Files\tdid\tnot\tarrive\tfor\tKronos"
						gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=ERROR
						echo "Files did not arrive for Kronos"
					else
						message="Waiting\tfor\tfiles.\tRetry\tafter\t$wait_time\tsecs"	
						gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
						echo "Waiting for files. Retry after $wait_time sec"
						sleep $wait_time
						sh $COMMON_MOUNT/scripts/$(basename $0 .sh).sh $environment $srcsys $frequency $done_file $retry_count $wait_time
					fi
        			continue
    			fi
		fi
done

echo "All files arrived begin processing"
done_file="$(echo $done_file | sed -e "s/YYYYMMDD/$CURR_DT/g")"
touch  "$SRCDIR/$done_file"

############################
### Processing the files ###
############################
grep -v '#' $filelist |
	while read line; do
			file_frequency="$(echo $line | cut -d'|' -f1)"
			if [[ "$file_frequency" == "$frequency" ]]; then
				filenm="$(echo $line | cut -d'|' -f2)"

				if [[ -f "$SRCDIR/$filenm" ]]; then
					echo "File processed currently - $filenm"
					
					wc -l "$SRCDIR/$filenm" >> "$SRCDIR/$done_file"

					message="File\tprocessed\tcurrently\t-\t$filenm"
					gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
					#Copy file for processing
					cp $SRCDIR/$filenm $TEMPDIR/$filenm

					#Remove ASCII 0
					sh $removeascii0script $TEMPDIR/$filenm
					wait
					echo -e "Removed ASCII0 $filenm"
					message="Removed\tASCII0\tfrom\tfile\t$filenm"
					gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO

					#Remove backtick(`)
					sh $removebacktickscript $TEMPDIR/$filenm
					wait
					echo -e "Removed backtick $filenm"
					message="Removed\tbacktick\tfrom\tfile\t$filenm"
					gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO

					#Remove asterisk(*)
					sh $removeasteriskscript $TEMPDIR/$filenm
					wait
					echo -e "Removed asterisk $filenm"
					message="Removed\tasterisk\tfrom\tfile\t$filenm"
					gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO


					#Reorder file for Combined_EDW_Employee
					if [[ $filenm == "Combined_EDW_Employee.txt" ]]; then
						python3 $filereorderscript -i $TEMPDIR/$filenm -o $TEMPDIR/$filenm
						wait
						echo -e "Reordered file $filenm"
					message="Reordered\tfile\t$filenm"
					gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
					fi

					#Move files to GCS Bucket
					gsutil -m cp $TEMPDIR/$filenm $GCSPATH/$filenm
					#rm $SRCDIR/$filenm
					rm $TEMPDIR/$filenm
					message="$filenm\t-\tfile\tmoved\tto\tGCS\tbucket"
					gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
					


				fi
			fi
	done
	
	#Move done file
	gsutil -m cp $SRCDIR/$done_file $GCSPATH/$done_file
	rm $SRCDIR/$done_file
	message="All\tfiles\tmoved\tsucessfully"
	gcloud logging write kronos-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
					