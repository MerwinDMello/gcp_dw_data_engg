
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefNBTCoidProgramType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefNBTCoidProgramType (CoID, ProgramTypeId, ProgramTypeDescription, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT source.CoID, source.ProgramTypeId, TRIM(source.ProgramTypeDescription), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefNBTCoidProgramType as source;
