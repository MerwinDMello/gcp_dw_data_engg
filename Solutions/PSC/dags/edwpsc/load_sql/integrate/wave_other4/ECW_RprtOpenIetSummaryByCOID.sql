
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RprtOpenIetSummaryByCOID ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtOpenIetSummaryByCOID (SnapShotDate, GroupName, DivisionName, MarketName, COID, COIDName, ClaimNumber, ClaimBalance, ErrorCount, ClaimKey)
SELECT source.SnapShotDate, TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.COID), TRIM(source.COIDName), source.ClaimNumber, source.ClaimBalance, source.ErrorCount, source.ClaimKey
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RprtOpenIetSummaryByCOID as source;
