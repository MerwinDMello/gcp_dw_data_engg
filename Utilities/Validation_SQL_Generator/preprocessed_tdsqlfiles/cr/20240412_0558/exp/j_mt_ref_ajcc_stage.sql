SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr_staging.Ref_Lookup_Code_Stg
WHERE LookUp_ID = 4040
 AND Lookup_Desc IS NOT NULL
 AND Trim(Lookup_Desc) NOT = ''