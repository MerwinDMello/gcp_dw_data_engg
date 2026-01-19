
SELECT	'J_REF_RESULT'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
trim(Nav_Result_Desc) as Nav_Result_Desc,
Source_System_Code as Source_System_Code
FROM EDWCR_Staging.REF_RESULT_Stg   
where trim(Nav_Result_Desc) not in (sel trim(Nav_Result_Desc) from EDWCR.REF_RESULT )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_RESULT')
) A;