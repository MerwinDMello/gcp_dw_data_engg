select 'J_CR_RO_DIMPATIENT_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_DimLocation