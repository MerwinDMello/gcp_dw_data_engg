
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimLinePayment AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimLinePayment AS source
ON target.ClaimLinePaymentKey = source.ClaimLinePaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLinePaymentKey = source.ClaimLinePaymentKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.ClaimLineLiabilityIplanKey = source.ClaimLineLiabilityIplanKey,
 target.FacilityKey = source.FacilityKey,
 target.ClaimLineChargesKey = source.ClaimLineChargesKey,
 target.ClaimPaymentsKey = source.ClaimPaymentsKey,
 target.PaymentDetailID = source.PaymentDetailID,
 target.InvCptId = source.InvCptId,
 target.PaymentAmt = source.PaymentAmt,
 target.ContractualAdjustmentAmt = source.ContractualAdjustmentAmt,
 target.WithHeldAmt = source.WithHeldAmt,
 target.DeductibleAmt = source.DeductibleAmt,
 target.CoInsuranceAmt = source.CoInsuranceAmt,
 target.AllowedAmt = source.AllowedAmt,
 target.MemberResponsibilityAmt = source.MemberResponsibilityAmt,
 target.MsgCode = TRIM(source.MsgCode),
 target.LineTaxAmt = source.LineTaxAmt,
 target.ERAAdjustmentAmt = source.ERAAdjustmentAmt,
 target.PaymentTypeKey = source.PaymentTypeKey,
 target.CreatedDateKey = source.CreatedDateKey,
 target.CreatedTime = source.CreatedTime,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.ModifiedDateKey = source.ModifiedDateKey,
 target.ModifiedTime = source.ModifiedTime,
 target.ModifiedByUserKey = source.ModifiedByUserKey,
 target.CPTDeleteFlag = source.CPTDeleteFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimLinePaymentKey, ClaimKey, ClaimNumber, TransactionNumber, VisitNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, ClaimLineLiabilityIplanKey, FacilityKey, ClaimLineChargesKey, ClaimPaymentsKey, PaymentDetailID, InvCptId, PaymentAmt, ContractualAdjustmentAmt, WithHeldAmt, DeductibleAmt, CoInsuranceAmt, AllowedAmt, MemberResponsibilityAmt, MsgCode, LineTaxAmt, ERAAdjustmentAmt, PaymentTypeKey, CreatedDateKey, CreatedTime, CreatedByUserKey, ModifiedDateKey, ModifiedTime, ModifiedByUserKey, CPTDeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimLinePaymentKey, source.ClaimKey, source.ClaimNumber, source.TransactionNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.ClaimLineLiabilityIplanKey, source.FacilityKey, source.ClaimLineChargesKey, source.ClaimPaymentsKey, source.PaymentDetailID, source.InvCptId, source.PaymentAmt, source.ContractualAdjustmentAmt, source.WithHeldAmt, source.DeductibleAmt, source.CoInsuranceAmt, source.AllowedAmt, source.MemberResponsibilityAmt, TRIM(source.MsgCode), source.LineTaxAmt, source.ERAAdjustmentAmt, source.PaymentTypeKey, source.CreatedDateKey, source.CreatedTime, source.CreatedByUserKey, source.ModifiedDateKey, source.ModifiedTime, source.ModifiedByUserKey, source.CPTDeleteFlag, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLinePaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimLinePayment
      GROUP BY ClaimLinePaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimLinePayment');
ELSE
  COMMIT TRANSACTION;
END IF;
