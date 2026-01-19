
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimLinePayment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimLinePayment AS source
ON target.ClaimLinePaymentKey = source.ClaimLinePaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLinePaymentKey = source.ClaimLinePaymentKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.ClaimLineChargesKey = source.ClaimLineChargesKey,
 target.ClaimPaymentsKey = source.ClaimPaymentsKey,
 target.PaymentDetailID = source.PaymentDetailID,
 target.AllowedAmt = source.AllowedAmt,
 target.DeductibleAmt = source.DeductibleAmt,
 target.CoInsuranceAmt = source.CoInsuranceAmt,
 target.MemberResponsibilityAmt = source.MemberResponsibilityAmt,
 target.PaymentAmt = source.PaymentAmt,
 target.ContractualAdjustmentAmt = source.ContractualAdjustmentAmt,
 target.WithHeldAmt = source.WithHeldAmt,
 target.MsgCode = TRIM(source.MsgCode),
 target.LineTaxAmt = source.LineTaxAmt,
 target.ERAAdjustmentAmt = source.ERAAdjustmentAmt,
 target.CreatedDateKey = source.CreatedDateKey,
 target.CreatedTime = source.CreatedTime,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.TimeStamp = source.TimeStamp,
 target.ModifiedDateKey = source.ModifiedDateKey,
 target.ModifiedTime = source.ModifiedTime,
 target.ModifiedByUserKey = source.ModifiedByUserKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.InvCptId = source.InvCptId,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (ClaimLinePaymentKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, ClaimLineChargesKey, ClaimPaymentsKey, PaymentDetailID, AllowedAmt, DeductibleAmt, CoInsuranceAmt, MemberResponsibilityAmt, PaymentAmt, ContractualAdjustmentAmt, WithHeldAmt, MsgCode, LineTaxAmt, ERAAdjustmentAmt, CreatedDateKey, CreatedTime, CreatedByUserKey, TimeStamp, ModifiedDateKey, ModifiedTime, ModifiedByUserKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, InvCptId, ArchivedRecord)
  VALUES (source.ClaimLinePaymentKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.ClaimLineChargesKey, source.ClaimPaymentsKey, source.PaymentDetailID, source.AllowedAmt, source.DeductibleAmt, source.CoInsuranceAmt, source.MemberResponsibilityAmt, source.PaymentAmt, source.ContractualAdjustmentAmt, source.WithHeldAmt, TRIM(source.MsgCode), source.LineTaxAmt, source.ERAAdjustmentAmt, source.CreatedDateKey, source.CreatedTime, source.CreatedByUserKey, source.TimeStamp, source.ModifiedDateKey, source.ModifiedTime, source.ModifiedByUserKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.InvCptId, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLinePaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactClaimLinePayment
      GROUP BY ClaimLinePaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactClaimLinePayment');
ELSE
  COMMIT TRANSACTION;
END IF;
