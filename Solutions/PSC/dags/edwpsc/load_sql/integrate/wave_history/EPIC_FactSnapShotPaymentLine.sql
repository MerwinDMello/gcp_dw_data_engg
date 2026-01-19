
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotPaymentLine AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotPaymentLine AS source
ON target.SnapShotDate = source.SnapShotDate AND target.PaymentLineKey = source.PaymentLineKey
WHEN MATCHED THEN
  UPDATE SET
  target.PaymentLineKey = source.PaymentLineKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.FacilityKey = source.FacilityKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Practicekey = source.Practicekey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.GLDepartmentNum = TRIM(source.GLDepartmentNum),
 target.PatientID = source.PatientID,
 target.ServicingProviderID = TRIM(source.ServicingProviderID),
 target.RenderingProviderID = TRIM(source.RenderingProviderID),
 target.FacilityID = source.FacilityID,
 target.PracticeID = TRIM(source.PracticeID),
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.FinancialClassKey = TRIM(source.FinancialClassKey),
 target.EncounterID = source.EncounterID,
 target.CPTID = TRIM(source.CPTID),
 target.PaymentLineId = TRIM(source.PaymentLineId),
 target.PaymentDetailId = source.PaymentDetailId,
 target.PaymentID = TRIM(source.PaymentID),
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
 target.Coid = TRIM(source.Coid)
WHEN NOT MATCHED THEN
  INSERT (PaymentLineKey, MonthID, SnapShotDate, ClaimKey, ClaimNumber, VisitNumber, TransactionNumber, ServicingProviderKey, RenderingProviderKey, FacilityKey, Iplan1IplanKey, Practicekey, CPTCodeKey, GLDepartmentNum, PatientID, ServicingProviderID, RenderingProviderID, FacilityID, PracticeID, ClaimDateKey, ServiceDateKey, Iplan1ID, FinancialClassKey, EncounterID, CPTID, PaymentLineId, PaymentDetailId, PaymentID, CheckNumber, CheckType, CheckDate, PostedDateKey, AllowedAmt, DeductableAmt, CoInsuranceAmt, MemRespAmt, PaymentAmt, ContractualAdjustmentAmt, WithHeldAmt, MsgCode, EraAdjustmentAmt, LineTaxAmt, PaymentFromID, PaymentTypeKey, CPTCode, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTOrder, CPTDeleletedLine, CPTUnits, CPTModifier1, CPTModifier2, CPTChargesAmt, PaymentLineCreateDate, PaymentLineModifiedDate, PaymentClaimCreateDate, PaymentClaimModifiedDate, RegionKey, Coid)
  VALUES (source.PaymentLineKey, source.MonthID, source.SnapShotDate, source.ClaimKey, source.ClaimNumber, source.VisitNumber, source.TransactionNumber, source.ServicingProviderKey, source.RenderingProviderKey, source.FacilityKey, source.Iplan1IplanKey, source.Practicekey, source.CPTCodeKey, TRIM(source.GLDepartmentNum), source.PatientID, TRIM(source.ServicingProviderID), TRIM(source.RenderingProviderID), source.FacilityID, TRIM(source.PracticeID), source.ClaimDateKey, source.ServiceDateKey, TRIM(source.Iplan1ID), TRIM(source.FinancialClassKey), source.EncounterID, TRIM(source.CPTID), TRIM(source.PaymentLineId), source.PaymentDetailId, TRIM(source.PaymentID), TRIM(source.CheckNumber), TRIM(source.CheckType), source.CheckDate, source.PostedDateKey, source.AllowedAmt, source.DeductableAmt, source.CoInsuranceAmt, source.MemRespAmt, source.PaymentAmt, source.ContractualAdjustmentAmt, source.WithHeldAmt, TRIM(source.MsgCode), source.EraAdjustmentAmt, source.LineTaxAmt, source.PaymentFromID, source.PaymentTypeKey, TRIM(source.CPTCode), source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, source.CPTOrder, source.CPTDeleletedLine, source.CPTUnits, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), source.CPTChargesAmt, source.PaymentLineCreateDate, source.PaymentLineModifiedDate, source.PaymentClaimCreateDate, source.PaymentClaimModifiedDate, source.RegionKey, TRIM(source.Coid));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, PaymentLineKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotPaymentLine
      GROUP BY SnapShotDate, PaymentLineKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotPaymentLine');
ELSE
  COMMIT TRANSACTION;
END IF;
