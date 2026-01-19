export JOBNAME='J_REF_THERAPY_TYPE'


export AC_EXP_SQL_STATEMENT="
SELECT	'J_REF_THERAPY_TYPE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Therapy_Type_Desc,
Source_System_Code
FROM EDWCR_Staging.Therapy_Type_stg
where trim(Therapy_Type_Desc) not in (sel trim(Therapy_Type_Desc) from EdwCR.REF_THERAPY_TYPE )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_THERAPY_TYPE')
) A;"


export AC_ACT_SQL_STATEMENT="select 'J_REF_THERAPY_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EdwCR.REF_THERAPY_TYPE WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_THERAPY_TYPE');"
