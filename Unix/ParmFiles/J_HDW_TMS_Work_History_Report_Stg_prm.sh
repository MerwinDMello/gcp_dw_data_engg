##########################
## Variable Declaration ##
##########################

export Job_Name='J_HDW_TMS_Work_History_Report_Stg'

######Expected Statment#######

export P_EXP_Control_Total_Field='1,'
export P_EXP_Number_of_Fields='12'
export P_EXP_Control_Total_Type='1,'
export SRCDR=/etl/ST/EDWHR/SrcFiles
export SOURCE_DIR=/etl/ST/EDWHR/SrcFiles
export AC_EXP_INPUT_FILE=EDWHR_WORK_HIST_RPT.csv
export P_EXP_Delimiter=','



######Actual Statment#########

export AC_ACT_SQL_STATEMENT="select '$JOBNAME' ||','|| cast(count(*) as varchar(50))  ||','as SOURCE_STRING 
FROM $NCR_STG_SCHEMA.Work_history_report"