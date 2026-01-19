
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotPaymentLine AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotPaymentLine AS source
ON target.SnapShotDate = source.SnapShotDate AND target.PaymentLineKey = source.PaymentLineKey
WHEN MATCHED THEN
  UPDATE SET
  target.PaymentLineKey = source.PaymentLineKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.claimKey = source.claimKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.FacilityKey = source.FacilityKey,
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
 target.CPTID = source.CPTID,
 target.PaymentLineId = source.PaymentLineId,
 target.PaymentDetailId = source.PaymentDetailId,
 target.PaymentID = source.PaymentID,
 target.CheckNumber = TRIM(source.CheckNumber),
 target.CheckType = TRIM(source.CheckType),
 target.CheckDate = source.CheckDate,
 target.PostedDateKey = source.PostedDateKey,
 target.AllowedAmt = source.AllowedAmt,
 target.DeductableAmt = source.DeductableAmt,
 target.CoInsuranceAmt = source.CoInsuranceAmt,
 target.MemRespAmt = source.MemRespAmt,
 target.PaymentAmt = source.PaymentAmt,
 target.ContractualAdjustmentAmt = source.ContractualAdjustmentAmt,
 target.WithHeldAmt = source.WithHeldAmt,
 target.MsgCode = TRIM(source.MsgCode),
 target.EraAdjustmentAmt = source.EraAdjustmentAmt,
 target.LineTaxAmt = source.LineTaxAmt,
 target.PaymentFromID = source.PaymentFromID,
 target.PaymentTypeKey = source.PaymentTypeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTStartServiceDateKey = source.CPTStartServiceDateKey,
 target.CPTEndServiceDateKey = source.CPTEndServiceDateKey,
 target.CPTOrder = source.CPTOrder,
 target.CPTDeleletedLine = source.CPTDeleletedLine,
 target.CPTUnits = source.CPTUnits,
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.CPTChargesAmt = source.CPTChargesAmt,
 target.PaymentLineCreateDate = source.PaymentLineCreateDate,
 target.PaymentLineModifiedDate = source.PaymentLineModifiedDate,
 target.PaymentClaimCreateDate = source.PaymentClaimCreateDate,
 target.PaymentClaimModifiedDate = source.PaymentClaimModifiedDate,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.ClaimLineChargeKey = source.ClaimLineChargeKey
WHEN NOT MATCHED THEN
  INSERT (PaymentLineKey, MonthID, SnapShotDate, claimKey, ServicingProviderKey, RenderingProviderKey, FacilityKey, Iplan1IplanKey, Practicekey, CPTCodeKey, claim_number, GLDepartmentNum, PatientID, ServicingProviderID, RenderingProviderID, FacilityID, PracticeID, ClaimDateKey, ServiceDateKey, Iplan1ID, FinancialClassKey, EncounterID, CPTID, PaymentLineId, PaymentDetailId, PaymentID, CheckNumber, CheckType, CheckDate, PostedDateKey, AllowedAmt, DeductableAmt, CoInsuranceAmt, MemRespAmt, PaymentAmt, ContractualAdjustmentAmt, WithHeldAmt, MsgCode, EraAdjustmentAmt, LineTaxAmt, PaymentFromID, PaymentTypeKey, CPTCode, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTOrder, CPTDeleletedLine, CPTUnits, CPTModifier1, CPTModifier2, CPTChargesAmt, PaymentLineCreateDate, PaymentLineModifiedDate, PaymentClaimCreateDate, PaymentClaimModifiedDate, RegionKey, Coid, ClaimLineChargeKey)
  VALUES (source.PaymentLineKey, source.MonthID, source.SnapShotDate, source.claimKey, source.ServicingProviderKey, source.RenderingProviderKey, source.FacilityKey, source.Iplan1IplanKey, source.Practicekey, source.CPTCodeKey, source.claim_number, TRIM(source.GLDepartmentNum), source.PatientID, source.ServicingProviderID, source.RenderingProviderID, source.FacilityID, source.PracticeID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1ID, TRIM(source.FinancialClassKey), source.EncounterID, source.CPTID, source.PaymentLineId, source.PaymentDetailId, source.PaymentID, TRIM(source.CheckNumber), TRIM(source.CheckType), source.CheckDate, source.PostedDateKey, source.AllowedAmt, source.DeductableAmt, source.CoInsuranceAmt, source.MemRespAmt, source.PaymentAmt, source.ContractualAdjustmentAmt, source.WithHeldAmt, TRIM(source.MsgCode), source.EraAdjustmentAmt, source.LineTaxAmt, source.PaymentFromID, source.PaymentTypeKey, TRIM(source.CPTCode), source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, source.CPTOrder, source.CPTDeleletedLine, source.CPTUnits, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), source.CPTChargesAmt, source.PaymentLineCreateDate, source.PaymentLineModifiedDate, source.PaymentClaimCreateDate, source.PaymentClaimModifiedDate, source.RegionKey, TRIM(source.Coid), source.ClaimLineChargeKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, PaymentLineKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotPaymentLine
      GROUP BY SnapShotDate, PaymentLineKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotPaymentLine');
ELSE
  COMMIT TRANSACTION;
END IF;
