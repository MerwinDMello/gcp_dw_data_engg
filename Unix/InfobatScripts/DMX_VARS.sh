#########################################################
### This script is used to define all the 
### Environment variables
#########################################################


HOME_DIR_HDW=/etl/jfmd/EDWHR
LOGDIR=/etl/ST/EDWHR/LogFiles
SRCDIR=/etl/ST/EDWHR/SrcFiles
SRCDR=/etl/ST/EDWHR/SrcFiles
SOURCE_DIR=/etl/ST/EDWHR/SrcFiles
TGTDIR=/etl/ST/EDWHR/TgtFiles
JOBDIR=$HOME_DIR_HDW/WorkFlows
BATDIR=$HOME_DIR_HDW/InfobatScripts
SCRIPTDIR=$HOME_DIR_HDW/Scripts
LOGONDIR=/etl/ST/EDWHR/LOGON
PARAMFILEDIR=$HOME_DIR_HDW/ParmFiles
MISMATCHDIR=$HOME_DIR_HDW/Mismatch
DMXDataDirectory=$HOME_DIR_HDW/WorkFlows
DMXTeradataDirectory=/etl/ST/EDWHR/CtlFiles
WORKSPACEDIR=/etl/WF/EDWHR
WORKSPACE_DIR=/etl/WF/EDWHR
IDF_DIR=/etl/IDF/EDWHR
export IDFDIR=/etl/IDF/EDWHR
ARCHIVE_DIR=/etl/ARCHIVE/EDWHR
SHAREDJOBDIR=$HOME_DIR_HDW/SharedJobs
#export error_notify="Mayur.Panpaliya@hcahealthcare.com"
#export email="Mayur.Panpaliya@hcahealthcare.com"
export error_notify=CorpIS.DLPDESClinicalEntRptgAlert@HCAHealthcare.com
export email=CorpIS.DLPDESClinicalEntRptgAlert@HCAHealthcare.com
JOBNAME=$Job_Name
TEMPDIR=/etl/ST/EDWHR/Temp
export REJ_DIR_HDW=/etl/ST/EDWHR/TgtFiles/RejectFiles
export Taleo_SrcFiles=/etl/ST/EDWHR/SrcFiles/Taleo


cd $HOME_DIR_HDW/InfobatScripts 

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0

##ODBCSYSINI=/etl/ST/EDWHR/LOGON
##ODBCINI=$ODBCSYSINI/odbc.ini


###
###Below are variables needed for reusable A/C Jobs
###

export MISMATCHDIR TEMPDIR HOME_DIR_HDW JOBNAME LOGDIR SRCDIR TGTDIR JOBDIR BATDIR SCRIPTDIR LOGONDIR PARAMFILEDIR DMXDataDirectory WORKSPACEDIR IDF_DIR ARCHIVE_DIR IDFDIR SHAREDJOBDIR WORKSPACE_DIR mismatchdir DMXTeradataDirectory SOURCE_DIR
