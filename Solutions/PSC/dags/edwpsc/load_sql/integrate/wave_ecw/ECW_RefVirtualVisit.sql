
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefVirtualVisit ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefVirtualVisit (VirtualVisitType, VirtualVisitDesc, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.VirtualVisitType, TRIM(source.VirtualVisitDesc), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefVirtualVisit as source;
