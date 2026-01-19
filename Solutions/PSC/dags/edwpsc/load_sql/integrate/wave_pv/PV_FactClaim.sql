
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaim AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaim AS source
ON target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.PatientKey = source.PatientKey,
 target.GuarantorPatientKey = source.GuarantorPatientKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.PayToProviderKey = source.PayToProviderKey,
 target.SupervisorProviderKey = source.SupervisorProviderKey,
 target.AddProvider1ProviderKey = source.AddProvider1ProviderKey,
 target.AddProvider2ProviderKey = source.AddProvider2ProviderKey,
 target.LiabilityDateKey = source.LiabilityDateKey,
 target.BilledToID = TRIM(source.BilledToID),
 target.BilledToType = source.BilledToType,
 target.FacilityKey = source.FacilityKey,
 target.ClaimStatusKey = source.ClaimStatusKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.TotalChargesAmt = source.TotalChargesAmt,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.TotalPatientBalanceAmt = source.TotalPatientBalanceAmt,
 target.TotalInsuranceBalanceAmt = source.TotalInsuranceBalanceAmt,
 target.TotalPatientBalanceMinusPatientIplansAmt = source.TotalPatientBalanceMinusPatientIplansAmt,
 target.TotalPaymentsAmt = source.TotalPaymentsAmt,
 target.TotalInsurancePaymentsAmt = source.TotalInsurancePaymentsAmt,
 target.TotalPatientPaymentsAmt = source.TotalPatientPaymentsAmt,
 target.TotalAdjustmentsAmt = source.TotalAdjustmentsAmt,
 target.TotalUncoveredAmt = source.TotalUncoveredAmt,
 target.TotalAllowedAmt = source.TotalAllowedAmt,
 target.TotalPatientResponsibilityAmt = source.TotalPatientResponsibilityAmt,
 target.TotalEstimatedPatientBalanceAmt = source.TotalEstimatedPatientBalanceAmt,
 target.TotalPaymentsAndAdjustmentsAmt = source.TotalPaymentsAndAdjustmentsAmt,
 target.VoidFlag = source.VoidFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ClaimCreatedByUserKey = source.ClaimCreatedByUserKey,
 target.ClaimQueueID = source.ClaimQueueID,
 target.ReferringProviderKey = source.ReferringProviderKey,
 target.ReferringProviderID = TRIM(source.ReferringProviderID),
 target.ReferringProviderType = TRIM(source.ReferringProviderType),
 target.Copay = source.Copay,
 target.ARClaimFlag = source.ARClaimFlag,
 target.Payer1IplanKey = source.Payer1IplanKey,
 target.VoidedClaimReversedClaimNumber = source.VoidedClaimReversedClaimNumber,
 target.VoidedClaimCopyClaimNumber = source.VoidedClaimCopyClaimNumber,
 target.VoidedClaimOriginalClaimNumber = source.VoidedClaimOriginalClaimNumber,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceAChangedFlag = source.SourceAChangedFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ClaimLock = TRIM(source.ClaimLock),
 target.ClaimTypeFlag = TRIM(source.ClaimTypeFlag),
 target.AccidentIndFlag = TRIM(source.AccidentIndFlag),
 target.AccidentState = TRIM(source.AccidentState),
 target.AccidentHour = TRIM(source.AccidentHour),
 target.AccidentRel = TRIM(source.AccidentRel),
 target.SymIndFlag = TRIM(source.SymIndFlag),
 target.SymDateKey = source.SymDateKey,
 target.EmpRelated = TRIM(source.EmpRelated),
 target.AdmitDateKey = source.AdmitDateKey,
 target.DischargeDateKey = source.DischargeDateKey,
 target.SplPgmId = TRIM(source.SplPgmId),
 target.EPSDTReferral = TRIM(source.EPSDTReferral),
 target.EPSDTRefSts = TRIM(source.EPSDTRefSts),
 target.CrimeRelatedFlag = TRIM(source.CrimeRelatedFlag),
 target.DocSentDateKey = source.DocSentDateKey,
 target.PWK06 = TRIM(source.PWK06),
 target.PregIndctorFlag = TRIM(source.PregIndctorFlag),
 target.RelInfoDateKey = source.RelInfoDateKey,
 target.RelInfoInd = TRIM(source.RelInfoInd),
 target.LabChargesAmt = source.LabChargesAmt,
 target.LabIndFlag = TRIM(source.LabIndFlag),
 target.PatientID = source.PatientID,
 target.ServicingProviderID = source.ServicingProviderID,
 target.RenderingProviderID = source.RenderingProviderID,
 target.PayToProviderID = source.PayToProviderID,
 target.SupervisorProviderID = source.SupervisorProviderID,
 target.AddProvider1ProviderID = source.AddProvider1ProviderID,
 target.AddProvider2ProviderID = source.AddProvider2ProviderID,
 target.FacilityID = source.FacilityID,
 target.PracticeID = TRIM(source.PracticeID),
 target.ClaimCreatedByID = source.ClaimCreatedByID,
 target.DeptCode = TRIM(source.DeptCode),
 target.Payer1IplanID = source.Payer1IplanID,
 target.Payer1FinancialClassKey = source.Payer1FinancialClassKey,
 target.Payer2IplanKey = source.Payer2IplanKey,
 target.Payer2IplanID = source.Payer2IplanID,
 target.Payer2FinancialClassKey = source.Payer2FinancialClassKey,
 target.Payer3IplanKey = source.Payer3IplanKey,
 target.Payer3IplanID = source.Payer3IplanID,
 target.Payer3FinancialClassKey = source.Payer3FinancialClassKey,
 target.CollectionStatus = TRIM(source.CollectionStatus),
 target.CollectionCodeChangeDate = TRIM(source.CollectionCodeChangeDate),
 target.CollectionCode = TRIM(source.CollectionCode),
 target.CollectionCodeChangeDateDATE = source.CollectionCodeChangeDateDATE,
 target.CollectionStatusAging = source.CollectionStatusAging,
 target.ClaimPractice = TRIM(source.ClaimPractice),
 target.ClaimBilledFlag = source.ClaimBilledFlag,
 target.ClaimLogBookID = source.ClaimLogBookID,
 target.LineBalanceAmt = source.LineBalanceAmt,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.HoldCode = TRIM(source.HoldCode),
 target.Payer1PVFinancialClass = TRIM(source.Payer1PVFinancialClass),
 target.Payer1PVFinancialClassDesc = TRIM(source.Payer1PVFinancialClassDesc),
 target.Payer1PVFinancialClassGrouped = TRIM(source.Payer1PVFinancialClassGrouped),
 target.LiabilityIplanKey = source.LiabilityIplanKey,
 target.RelayHealthFlag = TRIM(source.RelayHealthFlag),
 target.ProtocolName = TRIM(source.ProtocolName),
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (ClaimKey, ClaimNumber, Coid, RegionKey, ClaimDateKey, ServiceDateKey, PatientKey, GuarantorPatientKey, ServicingProviderKey, RenderingProviderKey, PayToProviderKey, SupervisorProviderKey, AddProvider1ProviderKey, AddProvider2ProviderKey, LiabilityDateKey, BilledToID, BilledToType, FacilityKey, ClaimStatusKey, EncounterKey, EncounterID, TotalChargesAmt, TotalBalanceAmt, TotalPatientBalanceAmt, TotalInsuranceBalanceAmt, TotalPatientBalanceMinusPatientIplansAmt, TotalPaymentsAmt, TotalInsurancePaymentsAmt, TotalPatientPaymentsAmt, TotalAdjustmentsAmt, TotalUncoveredAmt, TotalAllowedAmt, TotalPatientResponsibilityAmt, TotalEstimatedPatientBalanceAmt, TotalPaymentsAndAdjustmentsAmt, VoidFlag, DeleteFlag, CoidConfigurationKey, ClaimCreatedByUserKey, ClaimQueueID, ReferringProviderKey, ReferringProviderID, ReferringProviderType, Copay, ARClaimFlag, Payer1IplanKey, VoidedClaimReversedClaimNumber, VoidedClaimCopyClaimNumber, VoidedClaimOriginalClaimNumber, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceAChangedFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ClaimLock, ClaimTypeFlag, AccidentIndFlag, AccidentState, AccidentHour, AccidentRel, SymIndFlag, SymDateKey, EmpRelated, AdmitDateKey, DischargeDateKey, SplPgmId, EPSDTReferral, EPSDTRefSts, CrimeRelatedFlag, DocSentDateKey, PWK06, PregIndctorFlag, RelInfoDateKey, RelInfoInd, LabChargesAmt, LabIndFlag, PatientID, ServicingProviderID, RenderingProviderID, PayToProviderID, SupervisorProviderID, AddProvider1ProviderID, AddProvider2ProviderID, FacilityID, PracticeID, ClaimCreatedByID, DeptCode, Payer1IplanID, Payer1FinancialClassKey, Payer2IplanKey, Payer2IplanID, Payer2FinancialClassKey, Payer3IplanKey, Payer3IplanID, Payer3FinancialClassKey, CollectionStatus, CollectionCodeChangeDate, CollectionCode, CollectionCodeChangeDateDATE, CollectionStatusAging, ClaimPractice, ClaimBilledFlag, ClaimLogBookID, LineBalanceAmt, SourceBPrimaryKeyValue, HoldCode, Payer1PVFinancialClass, Payer1PVFinancialClassDesc, Payer1PVFinancialClassGrouped, LiabilityIplanKey, RelayHealthFlag, ProtocolName, SysStartTime, SysEndTime)
  VALUES (source.ClaimKey, source.ClaimNumber, TRIM(source.Coid), source.RegionKey, source.ClaimDateKey, source.ServiceDateKey, source.PatientKey, source.GuarantorPatientKey, source.ServicingProviderKey, source.RenderingProviderKey, source.PayToProviderKey, source.SupervisorProviderKey, source.AddProvider1ProviderKey, source.AddProvider2ProviderKey, source.LiabilityDateKey, TRIM(source.BilledToID), source.BilledToType, source.FacilityKey, source.ClaimStatusKey, source.EncounterKey, source.EncounterID, source.TotalChargesAmt, source.TotalBalanceAmt, source.TotalPatientBalanceAmt, source.TotalInsuranceBalanceAmt, source.TotalPatientBalanceMinusPatientIplansAmt, source.TotalPaymentsAmt, source.TotalInsurancePaymentsAmt, source.TotalPatientPaymentsAmt, source.TotalAdjustmentsAmt, source.TotalUncoveredAmt, source.TotalAllowedAmt, source.TotalPatientResponsibilityAmt, source.TotalEstimatedPatientBalanceAmt, source.TotalPaymentsAndAdjustmentsAmt, source.VoidFlag, source.DeleteFlag, source.CoidConfigurationKey, source.ClaimCreatedByUserKey, source.ClaimQueueID, source.ReferringProviderKey, TRIM(source.ReferringProviderID), TRIM(source.ReferringProviderType), source.Copay, source.ARClaimFlag, source.Payer1IplanKey, source.VoidedClaimReversedClaimNumber, source.VoidedClaimCopyClaimNumber, source.VoidedClaimOriginalClaimNumber, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceAChangedFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ClaimLock), TRIM(source.ClaimTypeFlag), TRIM(source.AccidentIndFlag), TRIM(source.AccidentState), TRIM(source.AccidentHour), TRIM(source.AccidentRel), TRIM(source.SymIndFlag), source.SymDateKey, TRIM(source.EmpRelated), source.AdmitDateKey, source.DischargeDateKey, TRIM(source.SplPgmId), TRIM(source.EPSDTReferral), TRIM(source.EPSDTRefSts), TRIM(source.CrimeRelatedFlag), source.DocSentDateKey, TRIM(source.PWK06), TRIM(source.PregIndctorFlag), source.RelInfoDateKey, TRIM(source.RelInfoInd), source.LabChargesAmt, TRIM(source.LabIndFlag), source.PatientID, source.ServicingProviderID, source.RenderingProviderID, source.PayToProviderID, source.SupervisorProviderID, source.AddProvider1ProviderID, source.AddProvider2ProviderID, source.FacilityID, TRIM(source.PracticeID), source.ClaimCreatedByID, TRIM(source.DeptCode), source.Payer1IplanID, source.Payer1FinancialClassKey, source.Payer2IplanKey, source.Payer2IplanID, source.Payer2FinancialClassKey, source.Payer3IplanKey, source.Payer3IplanID, source.Payer3FinancialClassKey, TRIM(source.CollectionStatus), TRIM(source.CollectionCodeChangeDate), TRIM(source.CollectionCode), source.CollectionCodeChangeDateDATE, source.CollectionStatusAging, TRIM(source.ClaimPractice), source.ClaimBilledFlag, source.ClaimLogBookID, source.LineBalanceAmt, TRIM(source.SourceBPrimaryKeyValue), TRIM(source.HoldCode), TRIM(source.Payer1PVFinancialClass), TRIM(source.Payer1PVFinancialClassDesc), TRIM(source.Payer1PVFinancialClassGrouped), source.LiabilityIplanKey, TRIM(source.RelayHealthFlag), TRIM(source.ProtocolName), source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaim
      GROUP BY ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaim');
ELSE
  COMMIT TRANSACTION;
END IF;
