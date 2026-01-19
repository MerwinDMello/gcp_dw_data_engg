
SELECT	'J_CR_REF_TEST_TYPE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
trim(TEST_TYPE_DESC) as TEST_TYPE_DESC,
trim(TEST_SUB_TYPE_DESC) as TEST_SUB_TYPE_DESC,
Source_System_Code as Source_System_Code
FROM EDWCR_staging.REF_TEST_TYPE_STG   
where (trim(TEST_TYPE_DESC),trim(TEST_SUB_TYPE_DESC)) not in (sel trim(TEST_TYPE_DESC),trim(TEST_SUB_TYPE_DESC) from EDWCR.REF_TEST_TYPE )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CR_REF_TEST_TYPE')
) A;