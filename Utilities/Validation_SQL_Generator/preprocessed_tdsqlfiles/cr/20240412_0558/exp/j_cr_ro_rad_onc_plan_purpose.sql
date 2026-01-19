SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT DISTINCT CASE
 WHEN Trim(DHD.PlanIntent)='' THEN NULL
 ELSE Trim(DHD.PlanIntent)
 END AS Plan_Purpose_Name,
 'R' AS Source_System_Code,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM EDWCR_STAGING.stg_DimPlan DHD)src