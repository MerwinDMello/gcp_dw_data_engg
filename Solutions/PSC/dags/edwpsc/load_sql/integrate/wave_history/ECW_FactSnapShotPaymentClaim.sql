
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotPaymentClaim AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotPaymentClaim AS source
ON target.SnapShotDate = source.SnapShotDate AND target.PaymentClaimKey = source.PaymentClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.PaymentClaimKey = source.PaymentClaimKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.GLDepartment = TRIM(source.GLDepartment),
 target.PatientID = source.PatientID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = source.ServicingProviderID,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = source.RenderingProviderID,
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = source.Iplan1ID,
 target.FinancialClassKey = source.FinancialClassKey,
 target.EncounterID = source.EncounterID,
 target.ContractualAdjustmentAmt = source.ContractualAdjustmentAmt,
 target.AllowedAmt = source.AllowedAmt,
 target.DeductibleAmt = source.DeductibleAmt,
 target.CoinsuranceAmt = source.CoinsuranceAmt,
 target.MemberBalanceAmt = source.MemberBalanceAmt,
 target.PaymentAmt = source.PaymentAmt,
 target.WithHeldAmt = source.WithHeldAmt,
 target.CheckAmt = source.CheckAmt,
 target.PaymentCreatedDateKey = source.PaymentCreatedDateKey,
 target.PaymentModifiedDateKey = source.PaymentModifiedDateKey,
 target.PaymentTypeKey = source.PaymentTypeKey,
 target.PaymentID = source.PaymentID,
 target.PaymentDetailID = source.PaymentDetailID,
 target.CheckDateKey = source.CheckDateKey,
 target.CheckNumber = TRIM(source.CheckNumber),
 target.CheckType = TRIM(source.CheckType),
 target.PayerID = source.PayerID,
 target.PostedDateKey = source.PostedDateKey,
 target.PaymentFromIplanKey = source.PaymentFromIplanKey,
 target.PaymentFromIplanID = source.PaymentFromIplanID,
 target.PaymentMemo = TRIM(source.PaymentMemo),
 target.PaymentLock = source.PaymentLock,
 target.PaymentDetailModifiedDateKey = source.PaymentDetailModifiedDateKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PaymentClaimKey, MonthID, SnapShotDate, Coid, RegionKey, ClaimKey, ClaimNumber, GLDepartment, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ServiceDateKey, Iplan1IplanKey, Iplan1ID, FinancialClassKey, EncounterID, ContractualAdjustmentAmt, AllowedAmt, DeductibleAmt, CoinsuranceAmt, MemberBalanceAmt, PaymentAmt, WithHeldAmt, CheckAmt, PaymentCreatedDateKey, PaymentModifiedDateKey, PaymentTypeKey, PaymentID, PaymentDetailID, CheckDateKey, CheckNumber, CheckType, PayerID, PostedDateKey, PaymentFromIplanKey, PaymentFromIplanID, PaymentMemo, PaymentLock, PaymentDetailModifiedDateKey, DWLastUpdateDateTime)
  VALUES (source.PaymentClaimKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.RegionKey, source.ClaimKey, source.ClaimNumber, TRIM(source.GLDepartment), source.PatientID, source.ServicingProviderKey, source.ServicingProviderID, source.RenderingProviderKey, source.RenderingProviderID, source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1IplanKey, source.Iplan1ID, source.FinancialClassKey, source.EncounterID, source.ContractualAdjustmentAmt, source.AllowedAmt, source.DeductibleAmt, source.CoinsuranceAmt, source.MemberBalanceAmt, source.PaymentAmt, source.WithHeldAmt, source.CheckAmt, source.PaymentCreatedDateKey, source.PaymentModifiedDateKey, source.PaymentTypeKey, source.PaymentID, source.PaymentDetailID, source.CheckDateKey, TRIM(source.CheckNumber), TRIM(source.CheckType), source.PayerID, source.PostedDateKey, source.PaymentFromIplanKey, source.PaymentFromIplanID, TRIM(source.PaymentMemo), source.PaymentLock, source.PaymentDetailModifiedDateKey, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, PaymentClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotPaymentClaim
      GROUP BY SnapShotDate, PaymentClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotPaymentClaim');
ELSE
  COMMIT TRANSACTION;
END IF;
