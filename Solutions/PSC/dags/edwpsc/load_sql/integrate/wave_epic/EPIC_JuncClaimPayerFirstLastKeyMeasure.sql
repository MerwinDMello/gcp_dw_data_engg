
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_JuncClaimPayerFirstLastKeyMeasure AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_JuncClaimPayerFirstLastKeyMeasure AS source
ON target.ClaimKey = source.ClaimKey AND target.ClaimPayerKey = source.ClaimPayerKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimPayerKey = source.ClaimPayerKey,
 target.ClaimKey = source.ClaimKey,
 target.Coid = TRIM(source.Coid),
 target.PayerFirstBillKey = source.PayerFirstBillKey,
 target.PayerFirstBillDateKey = source.PayerFirstBillDateKey,
 target.PayerLastBillKey = source.PayerLastBillKey,
 target.PayerLastBillDateKey = source.PayerLastBillDateKey,
 target.PayerNumberOfBills = source.PayerNumberOfBills,
 target.PayerFirstClaimPaymentKey = source.PayerFirstClaimPaymentKey,
 target.PayerFirstClaimPaymentDateKey = source.PayerFirstClaimPaymentDateKey,
 target.PayerLastClaimPaymentKey = source.PayerLastClaimPaymentKey,
 target.PayerLastClaimPaymentDateKey = source.PayerLastClaimPaymentDateKey,
 target.PayerTotalPayments = source.PayerTotalPayments,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimPayerKey, ClaimKey, Coid, PayerFirstBillKey, PayerFirstBillDateKey, PayerLastBillKey, PayerLastBillDateKey, PayerNumberOfBills, PayerFirstClaimPaymentKey, PayerFirstClaimPaymentDateKey, PayerLastClaimPaymentKey, PayerLastClaimPaymentDateKey, PayerTotalPayments, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimPayerKey, source.ClaimKey, TRIM(source.Coid), source.PayerFirstBillKey, source.PayerFirstBillDateKey, source.PayerLastBillKey, source.PayerLastBillDateKey, source.PayerNumberOfBills, source.PayerFirstClaimPaymentKey, source.PayerFirstClaimPaymentDateKey, source.PayerLastClaimPaymentKey, source.PayerLastClaimPaymentDateKey, source.PayerTotalPayments, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimKey, ClaimPayerKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_JuncClaimPayerFirstLastKeyMeasure
      GROUP BY ClaimKey, ClaimPayerKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_JuncClaimPayerFirstLastKeyMeasure');
ELSE
  COMMIT TRANSACTION;
END IF;
