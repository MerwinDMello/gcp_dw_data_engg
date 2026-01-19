##########################
## Variable Declaration ##
##########################
export AC_EXP_TOLERANCE_PERCENT=5

export JOBNAME='J_HDW_TMS_Employee_Goal_Detail'


export AC_EXP_SQL_STATEMENT="
select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM ${NCR_STG_SCHEMA}.Employee_Perf_Goals
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time 
FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name ='J_HDW_TMS_Employee_Perf_Goals')";



export AC_ACT_SQL_STATEMENT="select 'J_HDW_TMS_Employee_Goal_Detail' ||','|| Coalesce(cast(count(*) as varchar(20)), 0)  ||','as SOURCE_STRING
FROM ${NCR_TGT_SCHEMA}.Employee_Goal_Detail
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_VIEW}.ETL_JOB_RUN where Job_Name = 'J_HDW_TMS_Employee_Goal_Detail')"


      