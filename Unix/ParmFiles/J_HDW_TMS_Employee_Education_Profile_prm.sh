
##########################
## Variable Declaration ##
##########################

export JOBNAME='J_HDW_TMS_Employee_Education_Profile'
export Table_Name='Employee_Education_Profile'
export AC_EXP_TOLERANCE_PERCENT=5



export AC_EXP_SQL_STATEMENT="
select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWHR_Staging.Employee_Education_Profile_Wrk WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time 
FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name ='J_HDW_TMS_Employee_Education_Profile')";


export AC_ACT_SQL_STATEMENT="select 'J_HDW_TMS_Employee_Education_Profile'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWHR.Employee_Education_Profile
WHERE DW_Last_Update_Date_Time <= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time 
FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_HDW_TMS_Employee_Education_Profile')";

