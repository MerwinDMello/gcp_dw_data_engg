
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotAR AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotAR AS source
ON target.Snapshotdate = source.Snapshotdate AND target.arkey = source.arkey
WHEN MATCHED THEN
  UPDATE SET
  target.ARKey = source.ARKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.GLDepartment = TRIM(source.GLDepartment),
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = source.ServicingProviderID,
 target.PatientKey = source.PatientKey,
 target.PatientID = source.PatientID,
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = source.RenderingProviderID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ClaimDateMonthID = source.ClaimDateMonthID,
 target.ServiceDateKey = source.ServiceDateKey,
 target.ServiceDateMonthID = source.ServiceDateMonthID,
 target.PracticeKey = source.PracticeKey,
 target.PracticeID = source.PracticeID,
 target.ClaimStatusKey = source.ClaimStatusKey,
 target.ClaimStatusDesc = TRIM(source.ClaimStatusDesc),
 target.FinancialClassKey = source.FinancialClassKey,
 target.LiabilityFinancialClass = source.LiabilityFinancialClass,
 target.BilledFirstSubmittedDateKey = source.BilledFirstSubmittedDateKey,
 target.BilledLastSubmittedDateKey = source.BilledLastSubmittedDateKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = source.Iplan1ID,
 target.Iplan2IplanKey = source.Iplan2IplanKey,
 target.Iplan2ID = source.Iplan2ID,
 target.Iplan3IplanKey = source.Iplan3IplanKey,
 target.Iplan3ID = source.Iplan3ID,
 target.LiabilityIplanKey = source.LiabilityIplanKey,
 target.LiabilityID = source.LiabilityID,
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
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.BilledToIDType = source.BilledToIDType,
 target.ClaimARFlag = source.ClaimARFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.VoidFlag = source.VoidFlag,
 target.AddProvider1ProviderKey = source.AddProvider1ProviderKey,
 target.AddProvider2ProviderKey = source.AddProvider2ProviderKey,
 target.SupervisorProviderKey = source.SupervisorProviderKey,
 target.PayToProviderKey = source.PayToProviderKey,
 target.AddProvider1ProviderID = source.AddProvider1ProviderID,
 target.AddProvider2ProviderID = source.AddProvider2ProviderID,
 target.SupervisorProviderID = source.SupervisorProviderID,
 target.PayToProviderID = source.PayToProviderID,
 target.CollectionStatus = TRIM(source.CollectionStatus),
 target.CollectionCode = TRIM(source.CollectionCode),
 target.CollectionCodeChangeDate = source.CollectionCodeChangeDate,
 target.FirstPatientLiabilityDateKey = source.FirstPatientLiabilityDateKey,
 target.FirstZeroBalanceDateKey = source.FirstZeroBalanceDateKey,
 target.FirstChargeDebitDateKey = source.FirstChargeDebitDateKey
WHEN NOT MATCHED THEN
  INSERT (ARKey, MonthId, SnapShotDate, ClaimKey, ClaimNumber, RegionKey, Coid, GLDepartment, ServicingProviderKey, ServicingProviderID, PatientKey, PatientID, FacilityKey, FacilityID, RenderingProviderKey, RenderingProviderID, ClaimDateKey, ClaimDateMonthID, ServiceDateKey, ServiceDateMonthID, PracticeKey, PracticeID, ClaimStatusKey, ClaimStatusDesc, FinancialClassKey, LiabilityFinancialClass, BilledFirstSubmittedDateKey, BilledLastSubmittedDateKey, Iplan1IplanKey, Iplan1ID, Iplan2IplanKey, Iplan2ID, Iplan3IplanKey, Iplan3ID, LiabilityIplanKey, LiabilityID, LiabilityDate, LiabilityOwnerType, LastPaymentDateKey, FirstInsurancePaymentDateKey, LastInsurancePaymentDateKey, LastAdjustmentDateKey, LastStatusDateKey, TotalChargesAmt, TotalBalanceAmt, TotalPatientBalanceAmt, TotalInsuranceBalanceAmt, TotalPaymentsAmt, TotalPatientPaymentsAmt, TotalInsurancePaymentsAmt, TotalAdjustmentsAmt, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, BilledToIDType, ClaimARFlag, DeleteFlag, VoidFlag, AddProvider1ProviderKey, AddProvider2ProviderKey, SupervisorProviderKey, PayToProviderKey, AddProvider1ProviderID, AddProvider2ProviderID, SupervisorProviderID, PayToProviderID, CollectionStatus, CollectionCode, CollectionCodeChangeDate, FirstPatientLiabilityDateKey, FirstZeroBalanceDateKey, FirstChargeDebitDateKey)
  VALUES (source.ARKey, source.MonthId, source.SnapShotDate, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), TRIM(source.GLDepartment), source.ServicingProviderKey, source.ServicingProviderID, source.PatientKey, source.PatientID, source.FacilityKey, source.FacilityID, source.RenderingProviderKey, source.RenderingProviderID, source.ClaimDateKey, source.ClaimDateMonthID, source.ServiceDateKey, source.ServiceDateMonthID, source.PracticeKey, source.PracticeID, source.ClaimStatusKey, TRIM(source.ClaimStatusDesc), source.FinancialClassKey, source.LiabilityFinancialClass, source.BilledFirstSubmittedDateKey, source.BilledLastSubmittedDateKey, source.Iplan1IplanKey, source.Iplan1ID, source.Iplan2IplanKey, source.Iplan2ID, source.Iplan3IplanKey, source.Iplan3ID, source.LiabilityIplanKey, source.LiabilityID, source.LiabilityDate, source.LiabilityOwnerType, source.LastPaymentDateKey, source.FirstInsurancePaymentDateKey, source.LastInsurancePaymentDateKey, source.LastAdjustmentDateKey, source.LastStatusDateKey, source.TotalChargesAmt, source.TotalBalanceAmt, source.TotalPatientBalanceAmt, source.TotalInsuranceBalanceAmt, source.TotalPaymentsAmt, source.TotalPatientPaymentsAmt, source.TotalInsurancePaymentsAmt, source.TotalAdjustmentsAmt, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.BilledToIDType, source.ClaimARFlag, source.DeleteFlag, source.VoidFlag, source.AddProvider1ProviderKey, source.AddProvider2ProviderKey, source.SupervisorProviderKey, source.PayToProviderKey, source.AddProvider1ProviderID, source.AddProvider2ProviderID, source.SupervisorProviderID, source.PayToProviderID, TRIM(source.CollectionStatus), TRIM(source.CollectionCode), source.CollectionCodeChangeDate, source.FirstPatientLiabilityDateKey, source.FirstZeroBalanceDateKey, source.FirstChargeDebitDateKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT Snapshotdate, arkey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotAR
      GROUP BY Snapshotdate, arkey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotAR');
ELSE
  COMMIT TRANSACTION;
END IF;
