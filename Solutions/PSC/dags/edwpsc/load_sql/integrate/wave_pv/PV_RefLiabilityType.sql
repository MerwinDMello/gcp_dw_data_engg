
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_RefLiabilityType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_RefLiabilityType (LiabilityOwnerType, LiabilityOwnerDesc, LiabilityOwnerDesc2, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.LiabilityOwnerType, TRIM(source.LiabilityOwnerDesc), TRIM(source.LiabilityOwnerDesc2), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.PV_RefLiabilityType as source;
