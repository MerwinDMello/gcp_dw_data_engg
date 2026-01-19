
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefNBTLargePractice ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefNBTLargePractice (HCAPS_Group_Practice, CoID, CoIDName, CoIDNo, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT TRIM(source.HCAPS_Group_Practice), TRIM(source.CoID), TRIM(source.CoIDName), source.CoIDNo, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefNBTLargePractice as source;
