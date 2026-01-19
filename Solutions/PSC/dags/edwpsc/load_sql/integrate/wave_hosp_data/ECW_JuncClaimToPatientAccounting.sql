
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_JuncClaimToPatientAccounting ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncClaimToPatientAccounting (claimkey, MeditechCOCID, PatientAccountNumber, Patient_DW_ID, HospitalCOIDName, HospitalCOID, MeditechVisitNumber, ECW_ClaimNumber, ECW_ClaimServiceDate, ECW_Region, ECW_PrimaryFC, ECW_ClaimDeleteFlag, ECW_ClaimVoidFlag, ECW_TotalBalanceAmt, ECW_TotalInsurancePaymentsAmt, ECW_TotalPatientPaymentsAmt, PA_EMPI, ECW_EMPI, FoundInType)
SELECT source.claimkey, TRIM(source.MeditechCOCID), source.PatientAccountNumber, source.Patient_DW_ID, TRIM(source.HospitalCOIDName), TRIM(source.HospitalCOID), TRIM(source.MeditechVisitNumber), source.ECW_ClaimNumber, source.ECW_ClaimServiceDate, source.ECW_Region, source.ECW_PrimaryFC, source.ECW_ClaimDeleteFlag, source.ECW_ClaimVoidFlag, source.ECW_TotalBalanceAmt, source.ECW_TotalInsurancePaymentsAmt, source.ECW_TotalPatientPaymentsAmt, source.PA_EMPI, TRIM(source.ECW_EMPI), source.FoundInType
FROM {{ params.param_psc_stage_dataset_name }}.ECW_JuncClaimToPatientAccounting as source;
