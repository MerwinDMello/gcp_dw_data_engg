SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT ROW_NUMBER() OVER (
 ORDER BY trim(Type_Stg.Facility_Name)) + (SEL COALESCE(MAX(Facility_Id), 0) AS ID1
 FROM EDWCR.REF_FACILITY) AS Facility_Id,
 trim(Type_Stg.Facility_Name) AS Facility_Name,
 Type_Stg.Source_System_Code AS Source_System_Code,
 Current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT Facility_Name,
 Source_System_Code
 FROM EDWCR_Staging.Ref_Facility_Stg
 WHERE Facility_Name NOT IN (sel trim(Facility_Name)
 FROM EDWCR.REF_FACILITY
 WHERE Facility_Name IS NOT NULL ) )Type_Stg
 WHERE Facility_Name IS NOT NULL
 AND trim(Facility_Name) <> '' ) A