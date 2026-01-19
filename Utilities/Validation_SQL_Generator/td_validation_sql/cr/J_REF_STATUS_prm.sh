export JOBNAME='J_REF_STATUS'

export AC_EXP_SQL_STATEMENT="
SELECT	'J_REF_STATUS'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
trim(Status_Desc) as Status_Desc,
trim(Status_Type_Desc) as Status_Type_Desc,
Source_System_Code as Source_System_Code
FROM EDWCR_Staging.Ref_Status_Stg   
where (trim(Status_Desc),trim(Status_Type_Desc)) not in (sel trim(Status_Desc),trim(Status_Type_Desc) from EDWCR_Base_Views.REF_STATUS )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_STATUS')

) A;"


export AC_ACT_SQL_STATEMENT="select 'J_REF_STATUS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_Base_Views.REF_Status WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_STATUS');"

