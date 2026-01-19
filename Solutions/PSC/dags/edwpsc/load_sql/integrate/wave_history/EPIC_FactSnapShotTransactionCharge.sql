
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotTransactionCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotTransactionCharge AS source
ON target.SnapShotDate = source.SnapShotDate AND target.TransactionChargeSmryKey = source.TransactionChargeSmryKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionChargeSmryKey = source.TransactionChargeSmryKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.RegionKey = source.RegionKey,
 target.PracticeKey = source.PracticeKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.Coid = TRIM(source.Coid),
 target.GLDepartment = TRIM(source.GLDepartment),
 target.Claimkey = source.Claimkey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.PatientKey = source.PatientKey,
 target.PatientID = source.PatientID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = TRIM(source.ServicingProviderID),
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = TRIM(source.RenderingProviderID),
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.FinancialClassKey = source.FinancialClassKey,
 target.TransactionAmt = source.TransactionAmt,
 target.TransactionDateMonthID = source.TransactionDateMonthID,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTUnitChangeAmt = source.CPTUnitChangeAmt,
 target.TransactionType = TRIM(source.TransactionType),
 target.CPTID = TRIM(source.CPTID),
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.CPTModifier3 = TRIM(source.CPTModifier3),
 target.CPTModifier4 = TRIM(source.CPTModifier4),
 target.CPTUnitsOnLine = source.CPTUnitsOnLine,
 target.CPTTOS = TRIM(source.CPTTOS),
 target.CPTChargesChangeAmt = source.CPTChargesChangeAmt,
 target.CPTStartDOS = source.CPTStartDOS,
 target.CPTEndDOS = source.CPTEndDOS,
 target.CPTSeqNum = source.CPTSeqNum,
 target.TransactionID = TRIM(source.TransactionID),
 target.TransactionDateKey = source.TransactionDateKey,
 target.ClaimDateMonthID = source.ClaimDateMonthID,
 target.ServiceDateMonthID = source.ServiceDateMonthID,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.TransactionSameMonthFlag = TRIM(source.TransactionSameMonthFlag)
WHEN NOT MATCHED THEN
  INSERT (TransactionChargeSmryKey, MonthId, SnapShotDate, RegionKey, PracticeKey, PracticeID, Coid, GLDepartment, Claimkey, ClaimNumber, VisitNumber, TransactionNumber, PatientKey, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ServiceDateKey, Iplan1IplanKey, Iplan1ID, FinancialClassKey, TransactionAmt, TransactionDateMonthID, CPTCodeKey, CPTCode, CPTUnitChangeAmt, TransactionType, CPTID, CPTModifier1, CPTModifier2, CPTModifier3, CPTModifier4, CPTUnitsOnLine, CPTTOS, CPTChargesChangeAmt, CPTStartDOS, CPTEndDOS, CPTSeqNum, TransactionID, TransactionDateKey, ClaimDateMonthID, ServiceDateMonthID, SourceSystemCode, DWLastUpdateDateTime, TransactionSameMonthFlag)
  VALUES (source.TransactionChargeSmryKey, source.MonthId, source.SnapShotDate, source.RegionKey, source.PracticeKey, TRIM(source.PracticeID), TRIM(source.Coid), TRIM(source.GLDepartment), source.Claimkey, source.ClaimNumber, source.VisitNumber, source.TransactionNumber, source.PatientKey, source.PatientID, source.ServicingProviderKey, TRIM(source.ServicingProviderID), source.RenderingProviderKey, TRIM(source.RenderingProviderID), source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1IplanKey, TRIM(source.Iplan1ID), source.FinancialClassKey, source.TransactionAmt, source.TransactionDateMonthID, source.CPTCodeKey, TRIM(source.CPTCode), source.CPTUnitChangeAmt, TRIM(source.TransactionType), TRIM(source.CPTID), TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), TRIM(source.CPTModifier3), TRIM(source.CPTModifier4), source.CPTUnitsOnLine, TRIM(source.CPTTOS), source.CPTChargesChangeAmt, source.CPTStartDOS, source.CPTEndDOS, source.CPTSeqNum, TRIM(source.TransactionID), source.TransactionDateKey, source.ClaimDateMonthID, source.ServiceDateMonthID, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.TransactionSameMonthFlag));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, TransactionChargeSmryKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotTransactionCharge
      GROUP BY SnapShotDate, TransactionChargeSmryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotTransactionCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
