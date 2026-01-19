
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotAR AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotAR AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ARKey = source.ARKey
WHEN MATCHED THEN
  UPDATE SET
  target.ARKey = source.ARKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.GLDepartment = TRIM(source.GLDepartment),
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = TRIM(source.ServicingProviderID),
 target.PatientKey = source.PatientKey,
 target.PatientID = source.PatientID,
 target.PatientInternalId = source.PatientInternalId,
 target.AccountId = source.AccountId,
 target.EncounterID = source.EncounterID,
 target.EncounterForm = TRIM(source.EncounterForm),
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = TRIM(source.RenderingProviderID),
 target.ClaimDateKey = source.ClaimDateKey,
 target.ClaimDateMonthID = source.ClaimDateMonthID,
 target.ServiceDateKey = source.ServiceDateKey,
 target.ServiceDateMonthID = source.ServiceDateMonthID,
 target.PracticeKey = source.PracticeKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.ClaimStatusKey = source.ClaimStatusKey,
 target.ClaimStatusDesc = TRIM(source.ClaimStatusDesc),
 target.FinancialClassKey = source.FinancialClassKey,
 target.LiabilityFinancialClass = source.LiabilityFinancialClass,
 target.BilledFirstSubmittedDateKey = source.BilledFirstSubmittedDateKey,
 target.BilledLastSubmittedDateKey = source.BilledLastSubmittedDateKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.Iplan2IplanKey = source.Iplan2IplanKey,
 target.Iplan2ID = TRIM(source.Iplan2ID),
 target.Iplan3IplanKey = source.Iplan3IplanKey,
 target.Iplan3ID = TRIM(source.Iplan3ID),
 target.LiabilityIplanKey = source.LiabilityIplanKey,
 target.LiabilityID = TRIM(source.LiabilityID),
 target.LiabilityDate = source.LiabilityDate,
 target.LiabilityOwnerType = source.LiabilityOwnerType,
 target.LastPaymentDateKey = source.LastPaymentDateKey,
 target.FirstInsurancePaymentDateKey = source.FirstInsurancePaymentDateKey,
 target.LastInsurancePaymentDateKey = source.LastInsurancePaymentDateKey,
 target.LastAdjustmentDateKey = source.LastAdjustmentDateKey,
 target.LastStatusDateKey = source.LastStatusDateKey,
 target.TotalChargesAmt = source.TotalChargesAmt,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.TotalPatientBalanceAmt = source.TotalPatientBalanceAmt,
 target.TotalInsuranceBalanceAmt = source.TotalInsuranceBalanceAmt,
 target.TotalPaymentsAmt = source.TotalPaymentsAmt,
 target.TotalPatientPaymentsAmt = source.TotalPatientPaymentsAmt,
 target.TotalInsurancePaymentsAmt = source.TotalInsurancePaymentsAmt,
 target.TotalAdjustmentsAmt = source.TotalAdjustmentsAmt,
 target.BilledToIDType = source.BilledToIDType,
 target.ClaimBilledFlag = source.ClaimBilledFlag,
 target.ClaimARFlag = source.ClaimARFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.VoidFlag = source.VoidFlag,
 target.AddProvider1ProviderKey = source.AddProvider1ProviderKey,
 target.AddProvider1ProviderID = TRIM(source.AddProvider1ProviderID),
 target.AddProvider2ProviderKey = source.AddProvider2ProviderKey,
 target.AddProvider2ProviderID = TRIM(source.AddProvider2ProviderID),
 target.SupervisorProviderKey = source.SupervisorProviderKey,
 target.SupervisorProviderID = TRIM(source.SupervisorProviderID),
 target.PayToProviderKey = source.PayToProviderKey,
 target.PayToProviderID = TRIM(source.PayToProviderID),
 target.CollectionStatus = TRIM(source.CollectionStatus),
 target.CollectionCode = TRIM(source.CollectionCode),
 target.CollectionCodeChangeDate = source.CollectionCodeChangeDate,
 target.HoldCode = TRIM(source.HoldCode),
 target.BillAreaKey = source.BillAreaKey,
 target.ServiceAreaKey = source.ServiceAreaKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.FirstPatientLiabilityDateKey = source.FirstPatientLiabilityDateKey,
 target.FirstZeroBalanceDateKey = source.FirstZeroBalanceDateKey,
 target.FirstChargeDebitDateKey = source.FirstChargeDebitDateKey
WHEN NOT MATCHED THEN
  INSERT (ARKey, MonthId, SnapShotDate, ClaimKey, ClaimNumber, VisitNumber, RegionKey, Coid, GLDepartment, ServicingProviderKey, ServicingProviderID, PatientKey, PatientID, PatientInternalId, AccountId, EncounterID, EncounterForm, FacilityKey, FacilityID, RenderingProviderKey, RenderingProviderID, ClaimDateKey, ClaimDateMonthID, ServiceDateKey, ServiceDateMonthID, PracticeKey, PracticeID, ClaimStatusKey, ClaimStatusDesc, FinancialClassKey, LiabilityFinancialClass, BilledFirstSubmittedDateKey, BilledLastSubmittedDateKey, Iplan1IplanKey, Iplan1ID, Iplan2IplanKey, Iplan2ID, Iplan3IplanKey, Iplan3ID, LiabilityIplanKey, LiabilityID, LiabilityDate, LiabilityOwnerType, LastPaymentDateKey, FirstInsurancePaymentDateKey, LastInsurancePaymentDateKey, LastAdjustmentDateKey, LastStatusDateKey, TotalChargesAmt, TotalBalanceAmt, TotalPatientBalanceAmt, TotalInsuranceBalanceAmt, TotalPaymentsAmt, TotalPatientPaymentsAmt, TotalInsurancePaymentsAmt, TotalAdjustmentsAmt, BilledToIDType, ClaimBilledFlag, ClaimARFlag, DeleteFlag, VoidFlag, AddProvider1ProviderKey, AddProvider1ProviderID, AddProvider2ProviderKey, AddProvider2ProviderID, SupervisorProviderKey, SupervisorProviderID, PayToProviderKey, PayToProviderID, CollectionStatus, CollectionCode, CollectionCodeChangeDate, HoldCode, BillAreaKey, ServiceAreaKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FirstPatientLiabilityDateKey, FirstZeroBalanceDateKey, FirstChargeDebitDateKey)
  VALUES (source.ARKey, source.MonthId, source.SnapShotDate, source.ClaimKey, source.ClaimNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), TRIM(source.GLDepartment), source.ServicingProviderKey, TRIM(source.ServicingProviderID), source.PatientKey, source.PatientID, source.PatientInternalId, source.AccountId, source.EncounterID, TRIM(source.EncounterForm), source.FacilityKey, source.FacilityID, source.RenderingProviderKey, TRIM(source.RenderingProviderID), source.ClaimDateKey, source.ClaimDateMonthID, source.ServiceDateKey, source.ServiceDateMonthID, source.PracticeKey, TRIM(source.PracticeID), source.ClaimStatusKey, TRIM(source.ClaimStatusDesc), source.FinancialClassKey, source.LiabilityFinancialClass, source.BilledFirstSubmittedDateKey, source.BilledLastSubmittedDateKey, source.Iplan1IplanKey, TRIM(source.Iplan1ID), source.Iplan2IplanKey, TRIM(source.Iplan2ID), source.Iplan3IplanKey, TRIM(source.Iplan3ID), source.LiabilityIplanKey, TRIM(source.LiabilityID), source.LiabilityDate, source.LiabilityOwnerType, source.LastPaymentDateKey, source.FirstInsurancePaymentDateKey, source.LastInsurancePaymentDateKey, source.LastAdjustmentDateKey, source.LastStatusDateKey, source.TotalChargesAmt, source.TotalBalanceAmt, source.TotalPatientBalanceAmt, source.TotalInsuranceBalanceAmt, source.TotalPaymentsAmt, source.TotalPatientPaymentsAmt, source.TotalInsurancePaymentsAmt, source.TotalAdjustmentsAmt, source.BilledToIDType, source.ClaimBilledFlag, source.ClaimARFlag, source.DeleteFlag, source.VoidFlag, source.AddProvider1ProviderKey, TRIM(source.AddProvider1ProviderID), source.AddProvider2ProviderKey, TRIM(source.AddProvider2ProviderID), source.SupervisorProviderKey, TRIM(source.SupervisorProviderID), source.PayToProviderKey, TRIM(source.PayToProviderID), TRIM(source.CollectionStatus), TRIM(source.CollectionCode), source.CollectionCodeChangeDate, TRIM(source.HoldCode), source.BillAreaKey, source.ServiceAreaKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.FirstPatientLiabilityDateKey, source.FirstZeroBalanceDateKey, source.FirstChargeDebitDateKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ARKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotAR
      GROUP BY SnapShotDate, ARKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotAR');
ELSE
  COMMIT TRANSACTION;
END IF;
