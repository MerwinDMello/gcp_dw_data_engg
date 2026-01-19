select 'J_CR_RO_DIMRESOURCE_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_Staging.stg_DimResource