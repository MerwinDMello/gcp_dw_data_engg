##########################
## Variable Declaration ##
##########################

CRNT_TS=`date +"%Y-%m-%d %H:%M:%S"`

export AC_EXP_SQL_STATEMENT="select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING
FROM (SELECT 
(SELECT (COALESCE(MAX(Leadership_Level_SID),0)) FROM EDWHR_BASE_VIEWS.Ref_Leadership_Level) + 
		ROW_NUMBER() OVER (ORDER BY Y.Leadership_Level_Desc) AS Flight_Risk_SID
		, Y.Leadership_Level_Desc
		,'M' AS Source_System_Code
		,CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
	FROM	
	(
			SELECT  
				DISTINCT STG.Leadership_Level_Desc
			FROM
			(
					SELECT 
				  	TRIM(Future_Role2_Leadership_Level) AS Leadership_Level_Desc
					FROM 
					$NCR_STG_SCHEMA.EMPLOYEE_INFO 
					WHERE TRIM(Future_Role2_Leadership_Level) IS NOT NULL 
					AND	TRIM(Future_Role2_Leadership_Level) NOT= ''
			
					UNION ALL
			
					SELECT 
			 	 	TRIM(Future_Role1_Leadership_Level) AS Leadership_Level_Desc
			 	 	FROM 
					$NCR_STG_SCHEMA.EMPLOYEE_INFO 
					WHERE TRIM(Future_Role1_Leadership_Level) IS NOT NULL 
					AND	TRIM(Future_Role1_Leadership_Level) NOT= ''
			) STG
			
		LEFT OUTER JOIN EDWHR_BASE_VIEWS.Ref_Leadership_Level TGT
		ON STG.Leadership_Level_Desc = TGT.Leadership_Level_Desc

		WHERE TGT.Leadership_Level_Desc IS NULL	
	) Y
)R
"


export AC_ACT_SQL_STATEMENT="select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Ref_Leadership_Level WHERE DW_Last_Update_Date_Time >='$CRNT_TS'";