
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiInsPayment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncEdiInsPayment AS source
ON target.ECWEdiInsPaymentKey = source.ECWEdiInsPaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWEdiInsPaymentKey = source.ECWEdiInsPaymentKey,
 target.RegionKey = source.RegionKey,
 target.PayorType = source.PayorType,
 target.PayorID = source.PayorID,
 target.PayorIplanKey = source.PayorIplanKey,
 target.ProviderID = source.ProviderID,
 target.ProviderKey = source.ProviderKey,
 target.PaymentAmount = source.PaymentAmount,
 target.CheckNo = TRIM(source.CheckNo),
 target.PaymentType = TRIM(source.PaymentType),
 target.Paymentdate = source.Paymentdate,
 target.CheckDate = source.CheckDate,
 target.Memo = TRIM(source.Memo),
 target.PostedBy = source.PostedBy,
 target.PostedByUserKey = source.PostedByUserKey,
 target.PostedDt = source.PostedDt,
 target.PostedTime = source.PostedTime,
 target.ModifiedDate = source.ModifiedDate,
 target.FacilityId = source.FacilityId,
 target.FacilityKey = source.FacilityKey,
 target.PaymentLock = source.PaymentLock,
 target.LockedBy = source.LockedBy,
 target.LockedByUserKey = source.LockedByUserKey,
 target.UnpostedAmount = source.UnpostedAmount,
 target.PaymentCode = TRIM(source.PaymentCode),
 target.EOMB1 = TRIM(source.EOMB1),
 target.CreditCardType = source.CreditCardType,
 target.ClosingId = source.ClosingId,
 target.UnpostedCPT = source.UnpostedCPT,
 target.CheckTaxAmt = source.CheckTaxAmt,
 target.CheckTotalAmt = source.CheckTotalAmt,
 target.EraDetailId = source.EraDetailId,
 target.BatchId = source.BatchId,
 target.ParentPaymentId = source.ParentPaymentId,
 target.VoidFlag = source.VoidFlag,
 target.RecreatedFlag = source.RecreatedFlag,
 target.PracticeId = source.PracticeId,
 target.PracticeKey = source.PracticeKey,
 target.DeleteFlag = source.DeleteFlag,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PatientKey = source.PatientKey
WHEN NOT MATCHED THEN
  INSERT (ECWEdiInsPaymentKey, RegionKey, PayorType, PayorID, PayorIplanKey, ProviderID, ProviderKey, PaymentAmount, CheckNo, PaymentType, Paymentdate, CheckDate, Memo, PostedBy, PostedByUserKey, PostedDt, PostedTime, ModifiedDate, FacilityId, FacilityKey, PaymentLock, LockedBy, LockedByUserKey, UnpostedAmount, PaymentCode, EOMB1, CreditCardType, ClosingId, UnpostedCPT, CheckTaxAmt, CheckTotalAmt, EraDetailId, BatchId, ParentPaymentId, VoidFlag, RecreatedFlag, PracticeId, PracticeKey, DeleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PatientKey)
  VALUES (source.ECWEdiInsPaymentKey, source.RegionKey, source.PayorType, source.PayorID, source.PayorIplanKey, source.ProviderID, source.ProviderKey, source.PaymentAmount, TRIM(source.CheckNo), TRIM(source.PaymentType), source.Paymentdate, source.CheckDate, TRIM(source.Memo), source.PostedBy, source.PostedByUserKey, source.PostedDt, source.PostedTime, source.ModifiedDate, source.FacilityId, source.FacilityKey, source.PaymentLock, source.LockedBy, source.LockedByUserKey, source.UnpostedAmount, TRIM(source.PaymentCode), TRIM(source.EOMB1), source.CreditCardType, source.ClosingId, source.UnpostedCPT, source.CheckTaxAmt, source.CheckTotalAmt, source.EraDetailId, source.BatchId, source.ParentPaymentId, source.VoidFlag, source.RecreatedFlag, source.PracticeId, source.PracticeKey, source.DeleteFlag, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PatientKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWEdiInsPaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiInsPayment
      GROUP BY ECWEdiInsPaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiInsPayment');
ELSE
  COMMIT TRANSACTION;
END IF;
