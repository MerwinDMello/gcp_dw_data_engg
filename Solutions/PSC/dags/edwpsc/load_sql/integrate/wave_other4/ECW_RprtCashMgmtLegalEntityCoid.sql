
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtLegalEntityCoid ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtLegalEntityCoid (LegalEntityName, TaxID, LegalEntityEndDate, ARSystem, IsLegalEntityThirdParty, LegalEntityThirdPartyType, SpecialistLastName, SpecialistFirstName, LegalEntityMappedCOID, COID, BankName, BankAccountNumber, LockboxNumber, TreasuryValueUnit, SelectedCOIDUser, IsOpenConnect, LegalEntityCOIDEndDate, BankAccountEndDate, BankEndDate)
SELECT TRIM(source.LegalEntityName), source.TaxID, source.LegalEntityEndDate, TRIM(source.ARSystem), source.IsLegalEntityThirdParty, TRIM(source.LegalEntityThirdPartyType), TRIM(source.SpecialistLastName), TRIM(source.SpecialistFirstName), TRIM(source.LegalEntityMappedCOID), TRIM(source.COID), TRIM(source.BankName), TRIM(source.BankAccountNumber), source.LockboxNumber, TRIM(source.TreasuryValueUnit), TRIM(source.SelectedCOIDUser), source.IsOpenConnect, source.LegalEntityCOIDEndDate, source.BankAccountEndDate, source.BankEndDate
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RprtCashMgmtLegalEntityCoid as source;
