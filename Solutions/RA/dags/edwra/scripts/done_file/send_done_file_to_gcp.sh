#!/bin/bash

# Check if file pattern, src_sys, env  are provided as arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <env> <source> <done_file>"
    exit 1
fi

env="$1"
source="$2"
done_file="$3"

script_name=$(basename "$0")
script_name="${script_name%.*}"
basedir=/opt/tdgcp/edwra
scrdir=$basedir/scripts
logdir=$basedir/logs
CURR_DT=$(date +%Y%m%d)

log_file="${logdir}/${script_name}.$env.$done_file.$CURR_DT.log"
gcloud_log_file="${logdir}/gcloud.${script_name}.$env.$done_file.$CURR_DT.log"
cloudlogfilename=ra-file-transfer-$env-$done_file-$CURR_DT

export  HOME=/home/td_ra
cd $HOME

function log_to_file_and_cloud {
    log_message="$1"
    json_payload="{\"message\": \"$log_message\", \
                   \"logging_project\": \"$gcp_logging_project\",\
                   \"script_name\": \"$script_name\",\
                   \"env\": \"$env\",\
                   \"source\": \"$source\",\
                   \"done_file\": \"$done_file\" \
                    }"
    #gcloud logging write "$cloudlogfilename" "$json_payload" --project "$gcp_logging_project" --payload-type="json"
    echo "$log_message" >> "${log_file}"
}


echo "==============$(date)=====================" > ${log_file}

# Load environment-specific properties from the properties file
properties_file=$scrdir/ra_gcp_${env}.properties
if [ ! -f "$properties_file" ]; then
    log_to_file_and_cloud "Properties file not found: $properties_file"
    exit 1
fi
while IFS='=' read -r key value; do
    key=$(echo "$key" | tr '.' '_')
    eval "${key}=${value}"
done < "$properties_file"


# Validate if environment-specific properties are loaded
if [ -z "$gcp_service_account_key_file" ] || [ -z "$gcp_landing_bucket" ] || [ -z "$gcs_destination_directory" ] || [ -z "$gcp_logging_project" ] ; then
    log_to_file_and_cloud  "Invalid or missing properties in the properties file. $gcp_service_account_key_file , $gcp_landing_bucket , $gcs_destination_directory, $gcp_logging_project"
    exit 1
fi

# Activate the service account for authentication
gcloud auth activate-service-account --key-file=$gcp_service_account_key_file
if [[ "$?" -eq 0 ]]; then
   log_to_file_and_cloud "Authenticated to Service Account Succesfully"
else
   log_to_file_and_cloud "Authentication to Service Account Failed" 
   exit 1
fi

#  create done file and copy to gcs
echo "$(date +%Y%m%d%H%M%S)" > ${scrdir}/${done_file}_$CURR_DT.done
gsutil -m mv -L ${gcloud_log_file} "${scrdir}/${done_file}_$CURR_DT.done" "gs://$gcp_landing_bucket/edwradata/srcfiles/$source/"
if [ "$?" -eq 0 ]; then
 log_to_file_and_cloud "copied ${scrdir}/${done_file}_$CURR_DT.done to gs://$gcp_landing_bucket/edwradata/srcfiles/$source/"
else
 log_to_file_and_cloud "Error while moving files to GCS - see gcloud logs at ${gcloud_log_file}"
 exit 1
fi

chmod 644 $log_file

#Ramu Pydimarri 04/19/2024- sending the done file to dev while sending to qa
if [ $env == "qa" ]; then
   log_to_file_and_cloud "send a copy of done file to dev"
  . /opt/tdgcp/edwra/scripts/send_done_file_to_gcp.sh.bkup dev $source $done_file
fi
