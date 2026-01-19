
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotFinancialAdjustmentLine AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotFinancialAdjustmentLine AS source
ON target.SnapShotDate = source.SnapShotDate AND target.AdjustmentLineKey = source.AdjustmentLineKey
WHEN MATCHED THEN
  UPDATE SET
  target.AdjustmentLineKey = source.AdjustmentLineKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.FacilityKey = source.FacilityKey,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Practicekey = source.Practicekey,
 target.PracticeID = TRIM(source.PracticeID),
 target.CPTCodeKey = source.CPTCodeKey,
 target.GLDepartmentNum = TRIM(source.GLDepartmentNum),
 target.PatientID = source.PatientID,
 target.ServicingProviderID = TRIM(source.ServicingProviderID),
 target.RenderingProviderID = TRIM(source.RenderingProviderID),
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.FinancialClassKey = TRIM(source.FinancialClassKey),
 target.EncounterID = source.EncounterID,
 target.AdjustmentLineID = TRIM(source.AdjustmentLineID),
 target.AdjustmentID = TRIM(source.AdjustmentID),
 target.CPTID = TRIM(source.CPTID),
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTStartServiceDateKey = source.CPTStartServiceDateKey,
 target.CPTEndServiceDateKey = source.CPTEndServiceDateKey,
 target.CPTOrder = source.CPTOrder,
 target.CPTDeleletedLine = source.CPTDeleletedLine,
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.AdjustmentLineCreateDate = source.AdjustmentLineCreateDate,
 target.AdjustmentLineModifiedDate = source.AdjustmentLineModifiedDate,
 target.AdjustmentClaimCreateDate = source.AdjustmentClaimCreateDate,
 target.AdjustmentClaimModifiedDate = source.AdjustmentClaimModifiedDate,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.AdjustmentCode = TRIM(source.AdjustmentCode),
 target.AdjustmentClaimAmt = source.AdjustmentClaimAmt,
 target.AdjustmentClaimUnpostedCPTAmt = source.AdjustmentClaimUnpostedCPTAmt,
 target.AdjustmentClaimUnpostedAmt = source.AdjustmentClaimUnpostedAmt,
 target.CPTUnits = source.CPTUnits,
 target.CPTChargesAmt = source.CPTChargesAmt,
 target.AdjustmentLineAmt = source.AdjustmentLineAmt,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (AdjustmentLineKey, MonthID, SnapShotDate, ClaimKey, ClaimNumber, VisitNumber, TransactionNumber, ServicingProviderKey, RenderingProviderKey, FacilityKey, AdjustmentCodeKey, Iplan1IplanKey, Practicekey, PracticeID, CPTCodeKey, GLDepartmentNum, PatientID, ServicingProviderID, RenderingProviderID, FacilityID, ClaimDateKey, ServiceDateKey, Iplan1ID, FinancialClassKey, EncounterID, AdjustmentLineID, AdjustmentID, CPTID, CPTCode, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTOrder, CPTDeleletedLine, CPTModifier1, CPTModifier2, AdjustmentLineCreateDate, AdjustmentLineModifiedDate, AdjustmentClaimCreateDate, AdjustmentClaimModifiedDate, RegionKey, Coid, AdjustmentCode, AdjustmentClaimAmt, AdjustmentClaimUnpostedCPTAmt, AdjustmentClaimUnpostedAmt, CPTUnits, CPTChargesAmt, AdjustmentLineAmt, DWLastUpdateDateTime)
  VALUES (source.AdjustmentLineKey, source.MonthID, source.SnapShotDate, source.ClaimKey, source.ClaimNumber, source.VisitNumber, source.TransactionNumber, source.ServicingProviderKey, source.RenderingProviderKey, source.FacilityKey, source.AdjustmentCodeKey, source.Iplan1IplanKey, source.Practicekey, TRIM(source.PracticeID), source.CPTCodeKey, TRIM(source.GLDepartmentNum), source.PatientID, TRIM(source.ServicingProviderID), TRIM(source.RenderingProviderID), source.FacilityID, source.ClaimDateKey, source.ServiceDateKey, TRIM(source.Iplan1ID), TRIM(source.FinancialClassKey), source.EncounterID, TRIM(source.AdjustmentLineID), TRIM(source.AdjustmentID), TRIM(source.CPTID), TRIM(source.CPTCode), source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, source.CPTOrder, source.CPTDeleletedLine, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), source.AdjustmentLineCreateDate, source.AdjustmentLineModifiedDate, source.AdjustmentClaimCreateDate, source.AdjustmentClaimModifiedDate, source.RegionKey, TRIM(source.Coid), TRIM(source.AdjustmentCode), source.AdjustmentClaimAmt, source.AdjustmentClaimUnpostedCPTAmt, source.AdjustmentClaimUnpostedAmt, source.CPTUnits, source.CPTChargesAmt, source.AdjustmentLineAmt, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, AdjustmentLineKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotFinancialAdjustmentLine
      GROUP BY SnapShotDate, AdjustmentLineKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotFinancialAdjustmentLine');
ELSE
  COMMIT TRANSACTION;
END IF;
