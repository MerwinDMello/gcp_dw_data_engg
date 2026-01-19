export JOBNAME='J_REF_SITE_LOCATION'


export AC_EXP_SQL_STATEMENT="
SELECT	'J_REF_SITE_LOCATION'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Site_Location_Desc            
,Source_System_Code            
FROM EDWCR_STAGING.Ref_Site_Location_STG
WHERE trim(Site_Location_Desc) NOT IN (SEL TRIM(Site_Location_Desc) FROM EDWCR.Ref_Site_Location WHERE Site_Location_Desc IS NOT NULL)
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_SITE_LOCATION')
) A;"


export AC_ACT_SQL_STATEMENT="select 'J_REF_SITE_LOCATION'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Site_Location WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_SITE_LOCATION');"
