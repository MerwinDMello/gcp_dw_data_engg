
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_Fact53rdPatientTransaction AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_Fact53rdPatientTransaction AS source
ON target.PatientTransactionKey = source.PatientTransactionKey
WHEN MATCHED THEN
  UPDATE SET
  target.PatientTransactionKey = source.PatientTransactionKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.TransactionType = TRIM(source.TransactionType),
 target.METHOD = TRIM(source.METHOD),
 target.Channel = TRIM(source.Channel),
 target.Biller = TRIM(source.Biller),
 target.PatientName = TRIM(source.PatientName),
 target.PracticeName = TRIM(source.PracticeName),
 target.PatientKey = source.PatientKey,
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.ConfirmationNumber = TRIM(source.ConfirmationNumber),
 target.PaymentCreatedDate = source.PaymentCreatedDate,
 target.PaymentDate = source.PaymentDate,
 target.AccountType = TRIM(source.AccountType),
 target.TotalPaymentAmount = source.TotalPaymentAmount,
 target.PaymentStatus = TRIM(source.PaymentStatus),
 target.PaymentTime = TRIM(source.PaymentTime),
 target.SettledTime = TRIM(source.SettledTime),
 target.AddressLine1 = TRIM(source.AddressLine1),
 target.Zip = TRIM(source.Zip),
 target.PmtAccountType = TRIM(source.PmtAccountType),
 target.LastFourDigits = TRIM(source.LastFourDigits),
 target.AVSResponse = TRIM(source.AVSResponse),
 target.CVVResponse = TRIM(source.CVVResponse),
 target.StdEntryClass = TRIM(source.StdEntryClass),
 target.RevReasonOrCCAuth = TRIM(source.RevReasonOrCCAuth),
 target.UserID = TRIM(source.UserID),
 target.LoginID = TRIM(source.LoginID),
 target.CustomerID = TRIM(source.CustomerID),
 target.CSRRep = TRIM(source.CSRRep),
 target.COID = TRIM(source.COID),
 target.BillerRemittanceField1 = TRIM(source.BillerRemittanceField1),
 target.BillerRemittanceField2 = TRIM(source.BillerRemittanceField2),
 target.BillerRemittanceField3 = TRIM(source.BillerRemittanceField3),
 target.BillerRemittanceField5 = TRIM(source.BillerRemittanceField5),
 target.SourceFileName = TRIM(source.SourceFileName),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PatientTransactionKey, SourcePrimaryKeyValue, TransactionType, METHOD, Channel, Biller, PatientName, PracticeName, PatientKey, PatientAccountNumber, ConfirmationNumber, PaymentCreatedDate, PaymentDate, AccountType, TotalPaymentAmount, PaymentStatus, PaymentTime, SettledTime, AddressLine1, Zip, PmtAccountType, LastFourDigits, AVSResponse, CVVResponse, StdEntryClass, RevReasonOrCCAuth, UserID, LoginID, CustomerID, CSRRep, COID, BillerRemittanceField1, BillerRemittanceField2, BillerRemittanceField3, BillerRemittanceField5, SourceFileName, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PatientTransactionKey, source.SourcePrimaryKeyValue, TRIM(source.TransactionType), TRIM(source.METHOD), TRIM(source.Channel), TRIM(source.Biller), TRIM(source.PatientName), TRIM(source.PracticeName), source.PatientKey, TRIM(source.PatientAccountNumber), TRIM(source.ConfirmationNumber), source.PaymentCreatedDate, source.PaymentDate, TRIM(source.AccountType), source.TotalPaymentAmount, TRIM(source.PaymentStatus), TRIM(source.PaymentTime), TRIM(source.SettledTime), TRIM(source.AddressLine1), TRIM(source.Zip), TRIM(source.PmtAccountType), TRIM(source.LastFourDigits), TRIM(source.AVSResponse), TRIM(source.CVVResponse), TRIM(source.StdEntryClass), TRIM(source.RevReasonOrCCAuth), TRIM(source.UserID), TRIM(source.LoginID), TRIM(source.CustomerID), TRIM(source.CSRRep), TRIM(source.COID), TRIM(source.BillerRemittanceField1), TRIM(source.BillerRemittanceField2), TRIM(source.BillerRemittanceField3), TRIM(source.BillerRemittanceField5), TRIM(source.SourceFileName), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatientTransactionKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_Fact53rdPatientTransaction
      GROUP BY PatientTransactionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_Fact53rdPatientTransaction');
ELSE
  COMMIT TRANSACTION;
END IF;
