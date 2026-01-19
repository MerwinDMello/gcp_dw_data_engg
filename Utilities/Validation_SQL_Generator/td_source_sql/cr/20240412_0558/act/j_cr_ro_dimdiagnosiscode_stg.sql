select 'J_CR_RO_DIMDIAGNOSISCODE_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_Staging.DimDiagnosisCode_STG