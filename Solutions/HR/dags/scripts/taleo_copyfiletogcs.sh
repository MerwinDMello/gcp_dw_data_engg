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

log=$(basename $0 .sh).log
COMMON_MOUNT="/opt/tdgcp/edwhr"
SRCDIR="$COMMON_MOUNT/$srcsys"
if [[ "$environment" != "prd" ]] && [[ "$environment" != "prod" ]]; then
	temp_suffix="_$environment"
fi
TEMPDIR="$COMMON_MOUNT/$srcsys/temp$temp_suffix"
if [[ "$environment" == "prd" ]]; then
	environment="prod"
fi
GCSPATH="gs://eim-cs-da-gmek-5764-$environment/edwhrdata/srcfiles/$srcsys"
project="hca-hin-$environment-proc-hr"
filelist="$COMMON_MOUNT/scripts/${srcsys}_filelist.txt"
Source_System="$(echo $srcsys | sed -E "s/[[:alnum:]_'-]+/\u&/g")"
JSON_FRMT='{"date":"%s","environment":"%s","system":"VM-xrdclpapphin02a","script":"taleo_copyfiletogcs.sh","message":"%s","sourcesys":"%s","frequency":"%s"}'
rc=0

#####################################
### Scripts for File modification ###
#####################################
quotefixerscript="$COMMON_MOUNT/scripts/fixmissingdoublequotes.py"
filereorderscript="$COMMON_MOUNT/scripts/filereorderscript.py"
removeascii0script="$COMMON_MOUNT/scripts/removeascii0.sh"
removebacktickscript="$COMMON_MOUNT/scripts/removebacktick.sh"
message="Taleo/tfile/ttransfer/tscript/tstarted/texecution"
gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$Source_System" "$frequency") --project=$project --payload-type=json --severity=INFO

if [[ "$frequency" == "weekly_1_01.00" ]]; then
	EFF_DT=$(date +%Y%m%d -d "yesterday")
else
	EFF_DT=$(date +%Y%m%d)
fi

###########################
### Check files arrived ###
###########################
done_file="$(echo $done_file | sed -e "s/YYYYMMDD/$EFF_DT/g")"
if [[ -f "$SRCDIR/$done_file" ]]; then
	message="All/t$Source_System/tfiles/tarrived/tbegin/ttransfer/twith/tthe/tDone/tFile/t$done_file"
	gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
	echo "$done_file - All $Source_System files arrived begin transfer"

	#####################
	### Get File List ###
	#####################
	grep -v '#' $filelist |
		while read line; do
			file_frequency="$(echo $line | cut -d'|' -f1)"
			if [[ "$file_frequency" == "$frequency" ]]; then
				filenm="$(echo $line | cut -d'|' -f2 | sed -e "s/YYYYMMDD/$EFF_DT/g")"
				filenm_nodatepart="$(echo $line | cut -d'|' -f2 | sed -e "s/_YYYYMMDD//g" | tr '[:upper:]' '[:lower:]')"

				##############################
				### Start processing files ###
				##############################
				if ls $SRCDIR/$filenm >/dev/null 2>&1; then
					echo "File processed currently - $filenm"
					message="File/tprocessed/tcurrently/t-/t$filenm"
					gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO

					#Copy file for processing
					cp $SRCDIR/$filenm $TEMPDIR/$filenm_nodatepart

					#Remove ASCII 0
					sh $removeascii0script $TEMPDIR/$filenm_nodatepart
					echo -e "Removed ASCII0 $filenm_nodatepart"
					message="Removed/tASCII0/tfrom/tfile/t$filenm_nodatepart"
					gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO

					#Remove backtick(`)
					sh $removebacktickscript $TEMPDIR/$filenm_nodatepart
					echo -e "Removed backtick $filenm_nodatepart"
					message="Removed/tbacktick/tfrom/tfile/t$filenm_nodatepart"
					gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO

					#Move files to GCS Bucket
					gsutil -m cp $TEMPDIR/$filenm_nodatepart $GCSPATH/$filenm_nodatepart
					message="$filenm_nodatepart/tfile/tmoved/tto/tGCS/tbucket"
					gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO

					if [[ "$filenm_nodatepart" == "recruitmentsource.txt" ]]; then
						filenm_nodatepart_new="$(echo $filenm_nodatepart | sed -e "s/recruitmentsource.txt/recruitmentsource_stg.txt/g")"
						gsutil -m cp $TEMPDIR/$filenm_nodatepart $GCSPATH/$filenm_nodatepart_new
						message="$filenm_nodatepart_new/tfile/tmoved/tto/tGCS/tbucket"
						gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
					fi

					rm $TEMPDIR/$filenm_nodatepart

				else
					echo -e "$(date) --$COMMON_MOUNT/$srcsys/$filenm not found"
					message="$filenm/tnot/tfound"
					gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=ERROR

				fi

			fi
		done
	
	#Move done file
	done_file_new="$(echo $done_file | tr '[:upper:]' '[:lower:]')"
	gsutil -m cp $SRCDIR/$done_file $GCSPATH/$done_file_new
	message="$done_file_new/tfile/tmoved/tto/tGCS/tbucket"
	gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
else

	retry_count="$((retry_count - 1))"
	if [[ $retry_count -eq 0 ]]; then
		echo "Files did not arrive for $Source_System"
		message="Files/tdid/tnot/tarrive/tfor/t$Source_System"
		gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=ERROR
	else
		echo "$done_file - Waiting for files. Retry after $wait_time seconds"
		message="Waiting/tfor/t$done_file/t-/tRetry/tafter/t$wait_time/tseconds"
		gcloud logging write taleo-file-transfer-${frequency}-${CURR_DTTM} $(printf "$JSON_FRMT" "$CURR_DTTM" "$environment" "$message" "$srcsys" "$frequency") --project=$project --payload-type=json --severity=INFO
		sleep $wait_time
		sh $COMMON_MOUNT/scripts/$(basename $0 .sh).sh $environment $srcsys $frequency $done_file $retry_count $wait_time
	fi
fi
