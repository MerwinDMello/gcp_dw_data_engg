
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefBillType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefBillType (BillTypeKey, BillTypeName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.BillTypeKey, TRIM(source.BillTypeName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefBillType as source;
