
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefUTFDays ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefUTFDays (IplanKey, CoidState, IplanID, AppealDays, InitialSubmissionDays, AppealType, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, LastVerificationDate)
SELECT source.IplanKey, TRIM(source.CoidState), source.IplanID, source.AppealDays, source.InitialSubmissionDays, TRIM(source.AppealType), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.LastVerificationDate
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefUTFDays as source;
