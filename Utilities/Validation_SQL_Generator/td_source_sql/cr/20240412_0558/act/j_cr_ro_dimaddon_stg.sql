select 'J_CR_RO_DIMADDON_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_Staging.stg_DimAddOn