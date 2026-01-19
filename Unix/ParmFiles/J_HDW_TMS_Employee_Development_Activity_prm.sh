
##########################
## Variable Declaration ##
##########################

export JOBNAME='J_HDW_TMS_Employee_Development_Activity'
export Table_Name='Employee_Development_Activity'
export AC_EXP_TOLERANCE_PERCENT=5



export AC_EXP_SQL_STATEMENT="
select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWHR_Staging.Development_Activities_Report WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time 
FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name ='J_HDW_TMS_Development_Activities_Report_Stg')";


export AC_ACT_SQL_STATEMENT="select 'J_HDW_TMS_Employee_Development_Activity'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWHR.Employee_Development_Activity
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time 
FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name ='J_HDW_TMS_Employee_Development_Activity')";

