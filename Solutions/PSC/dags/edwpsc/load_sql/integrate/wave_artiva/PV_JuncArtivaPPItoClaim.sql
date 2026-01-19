
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_JuncArtivaPPItoClaim ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_JuncArtivaPPItoClaim (ArtivaPPItoClaimKey, ClaimKey, ClaimNumber, RegionKey, COID, PracticeFederalTaxID, CPID, ProviderID, ProviderNPI, FacilityID, PayerFinancialClass, PPIKey, PPIEffectiveDate, HoldRuleID, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, Payer1PracticeFederalTaxID, Payer1FacilityID, Payer1CPID, Payer1PPIKey, Payer1PPIEffectiveDate, Payer1FinancialClass)
SELECT source.ArtivaPPItoClaimKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.COID), TRIM(source.PracticeFederalTaxID), TRIM(source.CPID), TRIM(source.ProviderID), TRIM(source.ProviderNPI), TRIM(source.FacilityID), TRIM(source.PayerFinancialClass), TRIM(source.PPIKey), source.PPIEffectiveDate, source.HoldRuleID, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.Payer1PracticeFederalTaxID), TRIM(source.Payer1FacilityID), TRIM(source.Payer1CPID), TRIM(source.Payer1PPIKey), source.Payer1PPIEffectiveDate, TRIM(source.Payer1FinancialClass)
FROM {{ params.param_psc_stage_dataset_name }}.PV_JuncArtivaPPItoClaim as source;
