
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT DISTINCT Row_Number() OVER(
 ORDER BY Housing_Type_Name) + COALESCE(
 (SELECT MAX(COALESCE(a.Housing_Type_Id, 0)) AS MAX_KEY
 FROM EDWCR_base_views.Ref_Housing_Type a),0) AS Housing_Type_Id ,
 trim(Housing_Type_Name) AS Housing_Type_Name ,
 'N' AS Source_System_Code ,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT DISTINCT trim(ST.Housing_Type_Name) AS Housing_Type_Name
 FROM EDWCR_STAGING.Ref_Housing_Type_stg ST
 WHERE ST.Housing_Type_Name IS NOT NULL ) DIS
 WHERE NOT EXISTS
 (SELECT 1
 FROM EDWCR_base_views.Ref_Housing_Type RCDC
 WHERE TRIM(RCDC.Housing_Type_Name) = TRIM(DIS.Housing_Type_Name) )) A;