
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentLine AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentLine AS source
ON target.SnapShotDate = source.SnapShotDate AND target.AdjustmentLineKey = source.AdjustmentLineKey
WHEN MATCHED THEN
  UPDATE SET
  target.AdjustmentLineKey = source.AdjustmentLineKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.claimKey = source.claimKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.FacilityKey = source.FacilityKey,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Practicekey = source.Practicekey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.claim_number = source.claim_number,
 target.GLDepartmentNum = TRIM(source.GLDepartmentNum),
 target.PatientID = source.PatientID,
 target.ServicingProviderID = source.ServicingProviderID,
 target.RenderingProviderID = source.RenderingProviderID,
 target.FacilityID = source.FacilityID,
 target.PracticeID = source.PracticeID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1ID = source.Iplan1ID,
 target.FinancialClassKey = TRIM(source.FinancialClassKey),
 target.EncounterID = source.EncounterID,
 target.AdjustmentLineID = source.AdjustmentLineID,
 target.AdjustmentID = source.AdjustmentID,
 target.CPTID = source.CPTID,
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
 target.coid = TRIM(source.coid),
 target.AdjustmentCode = TRIM(source.AdjustmentCode),
 target.AdjustmentClaimAmt = source.AdjustmentClaimAmt,
 target.AdjustmentClaimUnpostedCPTAmt = source.AdjustmentClaimUnpostedCPTAmt,
 target.AdjustmentClaimUnpostedAmt = source.AdjustmentClaimUnpostedAmt,
 target.CPTUnits = source.CPTUnits,
 target.CPTChargesAmt = source.CPTChargesAmt,
 target.AdjustmentLineAmt = source.AdjustmentLineAmt,
 target.ClaimLineChargeKey = source.ClaimLineChargeKey
WHEN NOT MATCHED THEN
  INSERT (AdjustmentLineKey, MonthID, SnapShotDate, claimKey, ServicingProviderKey, RenderingProviderKey, FacilityKey, AdjustmentCodeKey, Iplan1IplanKey, Practicekey, CPTCodeKey, claim_number, GLDepartmentNum, PatientID, ServicingProviderID, RenderingProviderID, FacilityID, PracticeID, ClaimDateKey, ServiceDateKey, Iplan1ID, FinancialClassKey, EncounterID, AdjustmentLineID, AdjustmentID, CPTID, CPTCode, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTOrder, CPTDeleletedLine, CPTModifier1, CPTModifier2, AdjustmentLineCreateDate, AdjustmentLineModifiedDate, AdjustmentClaimCreateDate, AdjustmentClaimModifiedDate, RegionKey, coid, AdjustmentCode, AdjustmentClaimAmt, AdjustmentClaimUnpostedCPTAmt, AdjustmentClaimUnpostedAmt, CPTUnits, CPTChargesAmt, AdjustmentLineAmt, ClaimLineChargeKey)
  VALUES (source.AdjustmentLineKey, source.MonthID, source.SnapShotDate, source.claimKey, source.ServicingProviderKey, source.RenderingProviderKey, source.FacilityKey, source.AdjustmentCodeKey, source.Iplan1IplanKey, source.Practicekey, source.CPTCodeKey, source.claim_number, TRIM(source.GLDepartmentNum), source.PatientID, source.ServicingProviderID, source.RenderingProviderID, source.FacilityID, source.PracticeID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1ID, TRIM(source.FinancialClassKey), source.EncounterID, source.AdjustmentLineID, source.AdjustmentID, source.CPTID, TRIM(source.CPTCode), source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, source.CPTOrder, source.CPTDeleletedLine, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), source.AdjustmentLineCreateDate, source.AdjustmentLineModifiedDate, source.AdjustmentClaimCreateDate, source.AdjustmentClaimModifiedDate, source.RegionKey, TRIM(source.coid), TRIM(source.AdjustmentCode), source.AdjustmentClaimAmt, source.AdjustmentClaimUnpostedCPTAmt, source.AdjustmentClaimUnpostedAmt, source.CPTUnits, source.CPTChargesAmt, source.AdjustmentLineAmt, source.ClaimLineChargeKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, AdjustmentLineKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentLine
      GROUP BY SnapShotDate, AdjustmentLineKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotFinancialAdjustmentLine');
ELSE
  COMMIT TRANSACTION;
END IF;
