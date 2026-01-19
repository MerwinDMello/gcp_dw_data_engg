
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefNBTPhysicianStatusList ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefNBTPhysicianStatusList (NBTPhysicianStatusKey, PhysicianStatusKey, PhysicianStatusDesc, PhysicianStatusDetailId, PhysicianStatusDetailDesc, StatusYear, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.NBTPhysicianStatusKey, TRIM(source.PhysicianStatusKey), TRIM(source.PhysicianStatusDesc), TRIM(source.PhysicianStatusDetailId), TRIM(source.PhysicianStatusDetailDesc), TRIM(source.StatusYear), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefNBTPhysicianStatusList as source;
