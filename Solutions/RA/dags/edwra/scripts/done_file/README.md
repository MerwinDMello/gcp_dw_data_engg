# Done File to GCS bucket process
The files in this bucket are the placed on the ETL server to be called by zeke jobs to indicate the completion of a zeke job. The corresponding GCP job senses the presence of these files and then begins. This ensures dependencies are met across systems. See job CTDN1001 for an example implementation and what arguments to pass. These files are to be placed in the following location:
- server: xrdclpapphin02a.unix.medcity.net
- path: /opt/tdgcp/edwra/scripts

Logs from the execution of this script are kept on the same server in the following location:
- /opt/tdgcp/edwra/logs

There is no CI/CD process associated with these files, so any changes must be manually uploaded, or a separate process must be developed.