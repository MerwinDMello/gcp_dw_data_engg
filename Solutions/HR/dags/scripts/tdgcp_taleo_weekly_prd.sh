GOOGLE_APPLICATION_CREDENTIALS_PRD="/etc/.keys/hca-at-cao-sa-mgmt-5787d7db850f.json" 
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS_PRD
/opt/tdgcp/edwhr/scripts/taleo_copyfiletogcs.sh prd taleo weekly_1_01.00 Taleo_WeeklyFiles_YYYYMMDD.txt 25 300