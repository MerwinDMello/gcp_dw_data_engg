export JOBNAME='J_REF_SIDE'

export AC_EXP_SQL_STATEMENT="
SELECT	'J_REF_SIDE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT
ROW_NUMBER() over (order by trim(SRC.Side_Desc)) +(SEL COALESCE(MAX(Side_Id), 0) AS ID1 FROM EDWCR.Ref_Side) AS  Side_Id
,TRIM(SRC.Side_Desc) AS Side_Desc
,SRC.Source_System_Code            
,CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM (
SELECT 
Side_Desc                     
,Source_System_Code            
FROM EDWCR_STAGING.Ref_Side_STG
WHERE trim(Side_Desc) NOT IN (SELECT TRIM(Side_Desc) FROM EDWCR.Ref_Side WHERE Side_Desc IS NOT NULL)
) SRC
WHERE  Side_Desc IS NOT NULL 
AND Side_Desc <> ''
) A;"


export AC_ACT_SQL_STATEMENT="select 'J_REF_SIDE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Side WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_REF_SIDE');"

