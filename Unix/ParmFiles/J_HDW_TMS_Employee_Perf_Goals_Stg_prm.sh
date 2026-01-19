
##########################
## Variable Declaration ##
##########################

export JOBNAME='J_HDW_TMS_Employee_Perf_Goals_Stg'
export SRCDR=/etl/ST/EDWHR/SrcFiles
export SOURCE_DIR=/etl/ST/EDWHR/SrcFiles
cd $SOURCE_DIR
export fileNm=EDWHR_EMP_PERF_GOALS_RPT-REVISED_CY.csv



########Expected Parameter##########

export P_EXP_Control_Total_Field='1,'
export P_EXP_Number_of_Fields='20'
export P_EXP_Control_Total_Type='1,'
export SRCDR=/etl/ST/EDWHR/SrcFiles
export SOURCE_DIR=/etl/ST/EDWHR/SrcFiles
export AC_EXP_INPUT_FILE="$fileNm"
export P_EXP_Delimiter=','
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0


#######Actual Parameter############



export AC_ACT_SQL_STATEMENT="select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(50))||',' as SOURCE_STRING FROM $NCR_STG_SCHEMA.Employee_Perf_Goals"