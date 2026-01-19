
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactNHXS AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactNHXS AS source
ON target.NHXSKey = source.NHXSKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimKey = source.ClaimKey,
 target.PatientKey = source.PatientKey,
 target.GuarantorPatientKey = source.GuarantorPatientKey,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.FacilityKey = source.FacilityKey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTUnits = source.CPTUnits,
 target.CPTModifiers = TRIM(source.CPTModifiers),
 target.ECWIplan = TRIM(source.ECWIplan),
 target.ECWIplanName = TRIM(source.ECWIplanName),
 target.NHXSClaimSubscriberID = TRIM(source.NHXSClaimSubscriberID),
 target.NHXSPayerPatientID = TRIM(source.NHXSPayerPatientID),
 target.NHXSClaimID = TRIM(source.NHXSClaimID),
 target.NHXSClaimNumber = TRIM(source.NHXSClaimNumber),
 target.NHXSPayor = TRIM(source.NHXSPayor),
 target.NHXSRenderingProviderNpi = TRIM(source.NHXSRenderingProviderNpi),
 target.NHXSPatientAccountNumber = TRIM(source.NHXSPatientAccountNumber),
 target.NHXSServiceFromDate = source.NHXSServiceFromDate,
 target.NHXSServiceToDate = source.NHXSServiceToDate,
 target.NHXSProcedureCode = TRIM(source.NHXSProcedureCode),
 target.NHXSModifierCodes = TRIM(source.NHXSModifierCodes),
 target.NHXSUnitType = TRIM(source.NHXSUnitType),
 target.NHXSPayerAllowedAmount = source.NHXSPayerAllowedAmount,
 target.NHXSPaymentAmount = source.NHXSPaymentAmount,
 target.NHXSEobPayerName = TRIM(source.NHXSEobPayerName),
 target.NHXSPayerClaimReceivedDate = source.NHXSPayerClaimReceivedDate,
 target.NHXSEobPaymentDate = source.NHXSEobPaymentDate,
 target.NHXSProductType = TRIM(source.NHXSProductType),
 target.NHXSReasonCodes = TRIM(source.NHXSReasonCodes),
 target.NHXSReasonCodeGroups = TRIM(source.NHXSReasonCodeGroups),
 target.NHXSRemarkCodes = TRIM(source.NHXSRemarkCodes),
 target.NHXSHeaderRemarkCodes = TRIM(source.NHXSHeaderRemarkCodes),
 target.NHXSFeeScheduleAmount = source.NHXSFeeScheduleAmount,
 target.NHXSMedicareAllowedAmount = source.NHXSMedicareAllowedAmount,
 target.NHXSAllowedAmount = source.NHXSAllowedAmount,
 target.NHXSVariance = source.NHXSVariance,
 target.NHXSLatePaymentInterest = source.NHXSLatePaymentInterest,
 target.NHXSAuditType = TRIM(source.NHXSAuditType),
 target.NHXSNotAuditableType = TRIM(source.NHXSNotAuditableType),
 target.NHXSCodingAlertType = TRIM(source.NHXSCodingAlertType),
 target.NHXSEorDate = source.NHXSEorDate,
 target.NHXSAppealPrintStatus = TRIM(source.NHXSAppealPrintStatus),
 target.NHXSPrintDate = TRIM(source.NHXSPrintDate),
 target.NHXSPayerClaimNumber = TRIM(source.NHXSPayerClaimNumber),
 target.NHXSClaimLineID = TRIM(source.NHXSClaimLineID),
 target.NHXSTag = TRIM(source.NHXSTag),
 target.NHXSBillingProviderName = TRIM(source.NHXSBillingProviderName),
 target.NHXSClaimPayer = TRIM(source.NHXSClaimPayer),
 target.NHXSFeeScheduleNumber = TRIM(source.NHXSFeeScheduleNumber),
 target.NHXSMedicareLocality = TRIM(source.NHXSMedicareLocality),
 target.NHXSPatientResponsibilityAmount = source.NHXSPatientResponsibilityAmount,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.NHXSKey = source.NHXSKey
WHEN NOT MATCHED THEN
  INSERT (ClaimKey, PatientKey, GuarantorPatientKey, RenderingProviderKey, FacilityKey, CPTCodeKey, CPTUnits, CPTModifiers, ECWIplan, ECWIplanName, NHXSClaimSubscriberID, NHXSPayerPatientID, NHXSClaimID, NHXSClaimNumber, NHXSPayor, NHXSRenderingProviderNpi, NHXSPatientAccountNumber, NHXSServiceFromDate, NHXSServiceToDate, NHXSProcedureCode, NHXSModifierCodes, NHXSUnitType, NHXSPayerAllowedAmount, NHXSPaymentAmount, NHXSEobPayerName, NHXSPayerClaimReceivedDate, NHXSEobPaymentDate, NHXSProductType, NHXSReasonCodes, NHXSReasonCodeGroups, NHXSRemarkCodes, NHXSHeaderRemarkCodes, NHXSFeeScheduleAmount, NHXSMedicareAllowedAmount, NHXSAllowedAmount, NHXSVariance, NHXSLatePaymentInterest, NHXSAuditType, NHXSNotAuditableType, NHXSCodingAlertType, NHXSEorDate, NHXSAppealPrintStatus, NHXSPrintDate, NHXSPayerClaimNumber, NHXSClaimLineID, NHXSTag, NHXSBillingProviderName, NHXSClaimPayer, NHXSFeeScheduleNumber, NHXSMedicareLocality, NHXSPatientResponsibilityAmount, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, NHXSKey)
  VALUES (source.ClaimKey, source.PatientKey, source.GuarantorPatientKey, source.RenderingProviderKey, source.FacilityKey, source.CPTCodeKey, source.CPTUnits, TRIM(source.CPTModifiers), TRIM(source.ECWIplan), TRIM(source.ECWIplanName), TRIM(source.NHXSClaimSubscriberID), TRIM(source.NHXSPayerPatientID), TRIM(source.NHXSClaimID), TRIM(source.NHXSClaimNumber), TRIM(source.NHXSPayor), TRIM(source.NHXSRenderingProviderNpi), TRIM(source.NHXSPatientAccountNumber), source.NHXSServiceFromDate, source.NHXSServiceToDate, TRIM(source.NHXSProcedureCode), TRIM(source.NHXSModifierCodes), TRIM(source.NHXSUnitType), source.NHXSPayerAllowedAmount, source.NHXSPaymentAmount, TRIM(source.NHXSEobPayerName), source.NHXSPayerClaimReceivedDate, source.NHXSEobPaymentDate, TRIM(source.NHXSProductType), TRIM(source.NHXSReasonCodes), TRIM(source.NHXSReasonCodeGroups), TRIM(source.NHXSRemarkCodes), TRIM(source.NHXSHeaderRemarkCodes), source.NHXSFeeScheduleAmount, source.NHXSMedicareAllowedAmount, source.NHXSAllowedAmount, source.NHXSVariance, source.NHXSLatePaymentInterest, TRIM(source.NHXSAuditType), TRIM(source.NHXSNotAuditableType), TRIM(source.NHXSCodingAlertType), source.NHXSEorDate, TRIM(source.NHXSAppealPrintStatus), TRIM(source.NHXSPrintDate), TRIM(source.NHXSPayerClaimNumber), TRIM(source.NHXSClaimLineID), TRIM(source.NHXSTag), TRIM(source.NHXSBillingProviderName), TRIM(source.NHXSClaimPayer), TRIM(source.NHXSFeeScheduleNumber), TRIM(source.NHXSMedicareLocality), source.NHXSPatientResponsibilityAmount, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.NHXSKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NHXSKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactNHXS
      GROUP BY NHXSKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactNHXS');
ELSE
  COMMIT TRANSACTION;
END IF;
