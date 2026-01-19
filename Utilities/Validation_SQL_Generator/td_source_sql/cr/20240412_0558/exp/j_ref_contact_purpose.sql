SELECT	'J_REF_CONTACT_PURPOSE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Contact_Purpose_Desc,
Source_System_Code
FROM EDWCR_Staging.Contact_Purpose_stg
where trim(Contact_Purpose_Desc) not in (sel trim(Contact_Purpose_Desc) from EdwCR.REF_CONTACT_PURPOSE )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_CONTACT_PURPOSE')
) A;