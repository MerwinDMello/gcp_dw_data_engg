##########################
## Variable Declaration ##
##########################

export JOBNAME='J_HDW_TMS_Employee_Info_Stg'
export SRCDR=/etl/ST/EDWHR/SrcFiles
export SOURCE_DIR=/etl/ST/EDWHR/SrcFiles
cd $SOURCE_DIR
export fileNm=EDWHR_EMP_INFO_RPT.csv

#######Expected Parameter###################

export P_EXP_Control_Total_Field='1,'
export P_EXP_Number_of_Fields='51'
export P_EXP_Control_Total_Type='1,'
export AC_EXP_INPUT_FILE="$fileNm"
export P_EXP_Delimiter=','
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0

##########Actual Parameter#################

export AC_ACT_SQL_STATEMENT="select 'J_HDW_TMS_Employee_Info_Stg'||','|| Coalesce(cast( sum(ee.cnt) as varchar(20)), 0)  ||','as SOURCE_STRING from (sel coalesce(count(*),0) as cnt from EDWHR_STAGING.Employee_Info ) ee"

