TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefNBTAllocatedPhysicianStatus ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefNBTAllocatedPhysicianStatus
(All_id, All_caption, PhysicianStatusKey, PhysicianStatusDesc, PhysicianStatusDetailId, PhysicianStatusDetailDesc, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT TRIM(source.All_id), TRIM(source.All_caption), TRIM(source.PhysicianStatusKey), TRIM(source.PhysicianStatusDesc), TRIM(source.PhysicianStatusDetailId), TRIM(source.PhysicianStatusDetailDesc), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefNBTAllocatedPhysicianStatus as source;
