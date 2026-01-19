##########################
## Variable Declaration ##
##########################

export JOBNAME='J_HDW_TMS_Ref_Performance_Plan'

export AC_EXP_SQL_STATEMENT="select 'J_HDW_TMS_Ref_Performance_Plan' ||','|| Coalesce(cast(count(*) as varchar(20)), 0)  ||','as SOURCE_STRING 
FROM (SELECT Performance_Plan_Desc
FROM EDWHR_Staging.Ref_Performance_Plan_WRK WRK
WHERE WRK.Performance_Plan_Desc 
NOT IN (SELECT Performance_Plan_Desc FROM EDWHR.Ref_Performance_Plan)
) SRC
"

export AC_ACT_SQL_STATEMENT="select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Ref_Performance_Plan 
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) 
as Job_Start_Date_Time 
FROM ${NCR_AC_VIEW}.ETL_JOB_RUN where Job_Name = 'J_HDW_TMS_Ref_Performance_Plan')
"