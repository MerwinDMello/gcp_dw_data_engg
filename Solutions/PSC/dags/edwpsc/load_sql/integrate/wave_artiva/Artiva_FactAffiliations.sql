
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_FactAffiliations ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_FactAffiliations (ArtivaAffiliationsKey, ArtivaProviderKey, ArtivaGroupKey, ArtivaInstitutionKey, ProviderAffiliationLevel, HospitalAffiliation, AffiliationStartDate, AffiliationTermDate, AffiliationType, AffiliationState, HospitalAffiliationCategory, AffiliationSpecialty, AffiliationActiveFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.ArtivaAffiliationsKey, TRIM(source.ArtivaProviderKey), TRIM(source.ArtivaGroupKey), TRIM(source.ArtivaInstitutionKey), TRIM(source.ProviderAffiliationLevel), TRIM(source.HospitalAffiliation), source.AffiliationStartDate, source.AffiliationTermDate, TRIM(source.AffiliationType), TRIM(source.AffiliationState), TRIM(source.HospitalAffiliationCategory), TRIM(source.AffiliationSpecialty), TRIM(source.AffiliationActiveFlag), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_FactAffiliations as source;
