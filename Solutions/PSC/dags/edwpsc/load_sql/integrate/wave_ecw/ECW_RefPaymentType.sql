
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPaymentType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPaymentType (PaymentTypeKey, PaymentTypeName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.PaymentTypeKey, TRIM(source.PaymentTypeName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPaymentType as source;
