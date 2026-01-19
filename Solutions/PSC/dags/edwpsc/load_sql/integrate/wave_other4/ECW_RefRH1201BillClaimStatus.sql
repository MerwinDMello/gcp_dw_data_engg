
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefRH1201BillClaimStatus ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefRH1201BillClaimStatus (RH1201BillClaimStatusKey, RH1201BillClaimStatusName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
SELECT TRIM(source.RH1201BillClaimStatusKey), TRIM(source.RH1201BillClaimStatusName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefRH1201BillClaimStatus as source;
