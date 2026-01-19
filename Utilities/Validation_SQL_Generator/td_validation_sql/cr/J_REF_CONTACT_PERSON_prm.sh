export JOBNAME='J_REF_CONTACT_PERSON'


export AC_EXP_SQL_STATEMENT="SELECT	'J_REF_CONTACT_PERSON'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Contact_Person_Desc,
Source_System_Code
FROM EDWCR_Staging.Contact_Person_stg
where trim(Contact_Person_Desc) not in (sel trim(Contact_Person_Desc) from EdwCR.REF_CONTACT_PERSON )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_CONTACT_PERSON')
) A;"

export AC_ACT_SQL_STATEMENT="select 'J_REF_CONTACT_PERSON'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EdwCR.REF_CONTACT_PERSON WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_CONTACT_PERSON');"
