
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotTransactionFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotTransactionFinancialAdjustment AS source
ON target.SnapShotDate = source.SnapShotDate AND target.TransactionFinancialAdjustmentKey = source.TransactionFinancialAdjustmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionFinancialAdjustmentKey = source.TransactionFinancialAdjustmentKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.RegionKey = source.RegionKey,
 target.PracticeKey = source.PracticeKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.Coid = TRIM(source.Coid),
 target.GLDepartment = TRIM(source.GLDepartment),
 target.Claimkey = source.Claimkey,
 target.ClaimNumber = source.ClaimNumber,
 target.PatientKey = source.PatientKey,
 target.PatientID = source.PatientID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = source.ServicingProviderID,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = source.RenderingProviderID,
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ClaimDateMonthID = source.ClaimDateMonthID,
 target.ServiceDateKey = source.ServiceDateKey,
 target.ServiceDateMonthID = source.ServiceDateMonthID,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.FinancialClassKey = source.FinancialClassKey,
 target.TransactionID = TRIM(source.TransactionID),
 target.TransactionAmt = source.TransactionAmt,
 target.TransactionDateKey = source.TransactionDateKey,
 target.TransactionDateMonthID = source.TransactionDateMonthID,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.AdjustmentCode = TRIM(source.AdjustmentCode),
 target.TransactionType = TRIM(source.TransactionType),
 target.AdjustmentID = TRIM(source.AdjustmentID),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (TransactionFinancialAdjustmentKey, MonthId, SnapShotDate, RegionKey, PracticeKey, PracticeID, Coid, GLDepartment, Claimkey, ClaimNumber, PatientKey, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ClaimDateMonthID, ServiceDateKey, ServiceDateMonthID, Iplan1IplanKey, Iplan1ID, FinancialClassKey, TransactionID, TransactionAmt, TransactionDateKey, TransactionDateMonthID, AdjustmentCodeKey, AdjustmentCode, TransactionType, AdjustmentID, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.TransactionFinancialAdjustmentKey, source.MonthId, source.SnapShotDate, source.RegionKey, source.PracticeKey, TRIM(source.PracticeID), TRIM(source.Coid), TRIM(source.GLDepartment), source.Claimkey, source.ClaimNumber, source.PatientKey, source.PatientID, source.ServicingProviderKey, source.ServicingProviderID, source.RenderingProviderKey, source.RenderingProviderID, source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ClaimDateMonthID, source.ServiceDateKey, source.ServiceDateMonthID, source.Iplan1IplanKey, TRIM(source.Iplan1ID), source.FinancialClassKey, TRIM(source.TransactionID), source.TransactionAmt, source.TransactionDateKey, source.TransactionDateMonthID, source.AdjustmentCodeKey, TRIM(source.AdjustmentCode), TRIM(source.TransactionType), TRIM(source.AdjustmentID), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, TransactionFinancialAdjustmentKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotTransactionFinancialAdjustment
      GROUP BY SnapShotDate, TransactionFinancialAdjustmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotTransactionFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
