SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT ROW_NUMBER() OVER (
 ORDER BY trim(Type_Stg.Contact_Method_Desc)) + (SEL COALESCE(MAX(Contact_Method_Id), 0) AS ID1
 FROM EDWCR.REF_CONTACT_METHOD) AS Contact_Method_Id,
 trim(Type_Stg.Contact_Method_Desc) AS Contact_Method_Desc,
 Type_Stg.Source_System_Code AS Source_System_Code,
 Current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT Contact_Method_Desc,
 Source_System_Code
 FROM EDWCR_Staging.REF_CONTACT_METHOD_STG
 WHERE trim(Contact_Method_Desc) NOT IN (sel trim(Contact_Method_Desc)
 FROM EDWCR.REF_CONTACT_METHOD
 WHERE Contact_Method_Desc IS NOT NULL) )Type_Stg
 WHERE Contact_Method_Desc IS NOT NULL
 AND trim(Contact_Method_Desc) <> '' ) A;