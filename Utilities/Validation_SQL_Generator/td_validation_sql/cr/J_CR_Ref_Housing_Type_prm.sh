export JOBNAME='J_CR_Ref_Housing_Type'

export AC_EXP_SQL_STATEMENT="
SELECT	'J_CR_Ref_Housing_Type'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(

SELECT DISTINCT
	Row_Number() OVER( ORDER BY Housing_Type_Name )
		+ COALESCE((SELECT MAX(COALESCE(a.Housing_Type_Id,0)) AS MAX_KEY FROM EDWCR_base_views.Ref_Housing_Type a),0) AS Housing_Type_Id
	,trim(Housing_Type_Name)  as Housing_Type_Name
	,'N' AS Source_System_Code
	,CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM (SELECT DISTINCT trim(ST.Housing_Type_Name) AS Housing_Type_Name
		FROM EDWCR_STAGING.Ref_Housing_Type_stg ST
		WHERE ST.Housing_Type_Name IS NOT NULL
	 ) DIS
WHERE NOT EXISTS (SELECT 1 FROM EDWCR_base_views.Ref_Housing_Type RCDC 
					WHERE TRIM(RCDC.Housing_Type_Name) = TRIM(DIS.Housing_Type_Name) 
				 )) A;"


export AC_ACT_SQL_STATEMENT="select 'J_CR_Ref_Housing_Type'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Housing_Type WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CR_Ref_Housing_Type');"

