SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT DISTINCT CASE
 WHEN Trim(DHD.ActivityPriority)='' THEN NULL
 ELSE Trim(DHD.ActivityPriority)
 END AS Activity_Priority_Desc,
 'R' AS Source_System_Code,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM EDWCR_STAGING.stg_DimActivityTransaction DHD)src
WHERE src.Activity_Priority_Desc IS NOT NULL