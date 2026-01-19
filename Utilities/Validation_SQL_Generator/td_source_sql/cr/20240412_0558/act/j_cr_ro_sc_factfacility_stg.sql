select 'J_CR_RO_SC_FactFacility_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_Staging.STG_SC_FactFacility