#!/bin/bash

# Check input td schema comma separated list provided as argument
#if [ "$#" -ne 1 ]; then
#    echo "Usage: $0 <comma separated schemalist edwcdm,edwcdm_staging,edwcdm_base_schema"
#    exit 1
#fi

cd /opt/dwh-migration-tools-v1.0.27/bin


./dwh-migration-dumper  --connector teradata --url "jdbc:teradata://edwprod.dw.medcity.net/SSLMODE=DISABLE"  --user CORPSVC_EIM_GCP_READ   --password TAVKUIOSYCK  --driver /opt/dwh-migration-tools-v1.0.27/bin/terajdbc4.jar  --assessment
#./dwh-migration-dumper  --connector teradata-logs --url "jdbc:teradata://edwprod.dw.medcity.net/SSLMODE=DISABLE"  --user CORPSVC_EIM_GCP_READ   --password TAVKUIOSYCK  --driver /opt/dwh-migration-tools-v1.0.27/bin/terajdbc4.jar --assessment


gcloud auth activate-service-account --key-file=/etc/.keys/hin-td-pub-migrate-dev.json
gsutil -m cp dwh-migration-teradata-metadata.zip gs://hin-pub-bqms-translation-dev-f00680/
gsutil -m cp *yaml gs://hin-pub-bqms-translation-dev-f00680/

gcloud auth activate-service-account --key-file=/etc/.keys/hin-td-pub-migrate-qa.json
gsutil -m cp dwh-migration-teradata-metadata.zip gs://hin-pub-bqms-translation-qa-8d6360/
gsutil -m cp *yaml gs://hin-pub-bqms-translation-qa-8d6360/

gcloud auth activate-service-account --key-file=/etc/.keys/hin-td-pub-migrate-prod.json
gsutil -m cp dwh-migration-teradata-metadata.zip gs://hin-pub-bqms-translation-prod-a8496d/
gsutil -m cp *yaml  gs://hin-pub-bqms-translation-prod-a8496d/