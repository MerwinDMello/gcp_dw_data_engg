
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPayerResponseCategory ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPayerResponseCategory (DenialCategory, PayerResponseCategory, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PriorityRank, DWLastUpdateDateTime)
SELECT TRIM(source.DenialCategory), TRIM(source.PayerResponseCategory), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PriorityRank, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPayerResponseCategory as source;
