
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtLocationBasedCoidLegalEntity ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtLocationBasedCoidLegalEntity (LegalEntityName, TaxID, LegalEntityEndDate, ARSystem, IsLegalEntityThirdParty, LegalEntityThirdPartyType, SpecialistLastName, SpecialistFirstName, LocationMappedCOID, LocationName, BankName, BankAccountNumber, LockboxNumber, SiteDepositAccount, TreasuryValueUnit)
SELECT TRIM(source.LegalEntityName), source.TaxID, source.LegalEntityEndDate, TRIM(source.ARSystem), source.IsLegalEntityThirdParty, TRIM(source.LegalEntityThirdPartyType), TRIM(source.SpecialistLastName), TRIM(source.SpecialistFirstName), TRIM(source.LocationMappedCOID), TRIM(source.LocationName), TRIM(source.BankName), TRIM(source.BankAccountNumber), source.LockboxNumber, source.SiteDepositAccount, TRIM(source.TreasuryValueUnit)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RprtCashMgmtLocationBasedCoidLegalEntity as source;
