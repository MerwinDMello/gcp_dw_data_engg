export JOBNAME='J_REF_CONTACT_METHOD'
export AC_EXP_SQL_STATEMENT="SELECT	'J_REF_CONTACT_METHOD'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
ROW_NUMBER() over (order by trim(Type_Stg.Contact_Method_Desc)) + (SEL COALESCE(MAX(Contact_Method_Id), 0) AS ID1 FROM  EDWCR.REF_CONTACT_METHOD) AS  Contact_Method_Id,
trim(Type_Stg.Contact_Method_Desc) as Contact_Method_Desc,
Type_Stg.Source_System_Code as Source_System_Code,
Current_timestamp(0) as DW_Last_Update_Date_Time
FROM (
SELECT 
Contact_Method_Desc,
Source_System_Code
FROM EDWCR_Staging.REF_CONTACT_METHOD_STG
where trim(Contact_Method_Desc) not in (sel trim(Contact_Method_Desc) from EDWCR.REF_CONTACT_METHOD where Contact_Method_Desc IS NOT NULL)
)Type_Stg
WHERE Contact_Method_Desc IS NOT NULL and trim(Contact_Method_Desc) <> ''
) A;"


export AC_ACT_SQL_STATEMENT="select 'J_REF_CONTACT_METHOD'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EdwCR.REF_CONTACT_METHOD WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_CONTACT_METHOD');"
