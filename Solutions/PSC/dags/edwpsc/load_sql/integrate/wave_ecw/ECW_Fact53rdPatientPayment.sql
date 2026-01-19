
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_Fact53rdPatientPayment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_Fact53rdPatientPayment AS source
ON target.PatientPaymentKey = source.PatientPaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.PatientPaymentKey = source.PatientPaymentKey,
 target.AccountNumber = TRIM(source.AccountNumber),
 target.PatientName = TRIM(source.PatientName),
 target.OCRScanLine = TRIM(source.OCRScanLine),
 target.StatementDate = TRIM(source.StatementDate),
 target.COID = TRIM(source.COID),
 target.PracticeID = TRIM(source.PracticeID),
 target.PaymentDate = TRIM(source.PaymentDate),
 target.PaymentTypeFlag = TRIM(source.PaymentTypeFlag),
 target.PaymentType = TRIM(source.PaymentType),
 target.CreditCardTypeFlag = TRIM(source.CreditCardTypeFlag),
 target.CreditCardType = TRIM(source.CreditCardType),
 target.CheckNumber = TRIM(source.CheckNumber),
 target.PaidAmount = source.PaidAmount,
 target.BalanceDue = source.BalanceDue,
 target.SourceFileName = TRIM(source.SourceFileName),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PatientPaymentKey, AccountNumber, PatientName, OCRScanLine, StatementDate, COID, PracticeID, PaymentDate, PaymentTypeFlag, PaymentType, CreditCardTypeFlag, CreditCardType, CheckNumber, PaidAmount, BalanceDue, SourceFileName, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PatientPaymentKey, TRIM(source.AccountNumber), TRIM(source.PatientName), TRIM(source.OCRScanLine), TRIM(source.StatementDate), TRIM(source.COID), TRIM(source.PracticeID), TRIM(source.PaymentDate), TRIM(source.PaymentTypeFlag), TRIM(source.PaymentType), TRIM(source.CreditCardTypeFlag), TRIM(source.CreditCardType), TRIM(source.CheckNumber), source.PaidAmount, source.BalanceDue, TRIM(source.SourceFileName), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatientPaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_Fact53rdPatientPayment
      GROUP BY PatientPaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_Fact53rdPatientPayment');
ELSE
  COMMIT TRANSACTION;
END IF;
