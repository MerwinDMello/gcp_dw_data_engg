
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefNBTAppSpecialtyCategory ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefNBTAppSpecialtyCategory (APPSpecialtyCategoryId, APPSpecialtyCategoryDescription, IsActive, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT source.APPSpecialtyCategoryId, TRIM(source.APPSpecialtyCategoryDescription), source.IsActive, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefNBTAppSpecialtyCategory as source;
