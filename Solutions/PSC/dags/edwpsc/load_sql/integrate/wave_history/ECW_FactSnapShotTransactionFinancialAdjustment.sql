
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotTransactionFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotTransactionFinancialAdjustment AS source
ON target.SnapShotDate = source.SnapShotDate AND target.TransactionFinancialAdjustmentKey = source.TransactionFinancialAdjustmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionFinancialAdjustmentKey = source.TransactionFinancialAdjustmentKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.RegionKey = source.RegionKey,
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
 target.Iplan1ID = source.Iplan1ID,
 target.FinancialClassKey = source.FinancialClassKey,
 target.TransactionID = source.TransactionID,
 target.TransactionAmt = source.TransactionAmt,
 target.TransactionDateKey = source.TransactionDateKey,
 target.TransactionDateMonthID = source.TransactionDateMonthID,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.AdjustmentCode = TRIM(source.AdjustmentCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.TransactionType = TRIM(source.TransactionType),
 target.AdjustmentID = source.AdjustmentID,
 target.TransactionSameMonthFlag = TRIM(source.TransactionSameMonthFlag)
WHEN NOT MATCHED THEN
  INSERT (TransactionFinancialAdjustmentKey, MonthId, SnapShotDate, RegionKey, Coid, GLDepartment, Claimkey, ClaimNumber, PatientKey, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ClaimDateMonthID, ServiceDateKey, ServiceDateMonthID, Iplan1IplanKey, Iplan1ID, FinancialClassKey, TransactionID, TransactionAmt, TransactionDateKey, TransactionDateMonthID, AdjustmentCodeKey, AdjustmentCode, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, TransactionType, AdjustmentID, TransactionSameMonthFlag)
  VALUES (source.TransactionFinancialAdjustmentKey, source.MonthId, source.SnapShotDate, source.RegionKey, TRIM(source.Coid), TRIM(source.GLDepartment), source.Claimkey, source.ClaimNumber, source.PatientKey, source.PatientID, source.ServicingProviderKey, source.ServicingProviderID, source.RenderingProviderKey, source.RenderingProviderID, source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ClaimDateMonthID, source.ServiceDateKey, source.ServiceDateMonthID, source.Iplan1IplanKey, source.Iplan1ID, source.FinancialClassKey, source.TransactionID, source.TransactionAmt, source.TransactionDateKey, source.TransactionDateMonthID, source.AdjustmentCodeKey, TRIM(source.AdjustmentCode), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.TransactionType), source.AdjustmentID, TRIM(source.TransactionSameMonthFlag));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, TransactionFinancialAdjustmentKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotTransactionFinancialAdjustment
      GROUP BY SnapShotDate, TransactionFinancialAdjustmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotTransactionFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
