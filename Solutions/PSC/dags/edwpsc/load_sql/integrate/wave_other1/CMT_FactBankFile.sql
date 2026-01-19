
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CMT_FactBankFile AS target
USING {{ params.param_psc_stage_dataset_name }}.CMT_FactBankFile AS source
ON target.BankFileKey = source.BankFileKey
WHEN MATCHED THEN
  UPDATE SET
  target.BankFileKey = source.BankFileKey,
 target.BankFileId = source.BankFileId,
 target.DataImportId = source.DataImportId,
 target.BankName = TRIM(source.BankName),
 target.DepositDate = source.DepositDate,
 target.ACCOUNT = TRIM(source.ACCOUNT),
 target.DataType = TRIM(source.DataType),
 target.BaiCode = source.BaiCode,
 target.Amount = source.Amount,
 target.CustomerReference = source.CustomerReference,
 target.BankReference = TRIM(source.BankReference),
 target.Description = TRIM(source.Description),
 target.AppendDate = source.AppendDate,
 target.CreateBatchId = TRIM(source.CreateBatchId),
 target.BOADescId = source.BOADescId,
 target.PayerName = TRIM(source.PayerName),
 target.EntryDescription = TRIM(source.EntryDescription),
 target.ReceiverId = TRIM(source.ReceiverId),
 target.OriginatorId = TRIM(source.OriginatorId),
 target.StandardEntryClassCode = TRIM(source.StandardEntryClassCode),
 target.PaymentInformation = TRIM(source.PaymentInformation),
 target.TraceTypeCode = TRIM(source.TraceTypeCode),
 target.CheckOrEFTFlag = TRIM(source.CheckOrEFTFlag),
 target.CheckOrEFTNumber = TRIM(source.CheckOrEFTNumber),
 target.PayerIdNumber = TRIM(source.PayerIdNumber),
 target.OriginatingCompanySupplementalCode = TRIM(source.OriginatingCompanySupplementalCode),
 target.Notes = TRIM(source.Notes),
 target.BatchId = TRIM(source.BatchId),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (BankFileKey, BankFileId, DataImportId, BankName, DepositDate, ACCOUNT, DataType, BaiCode, Amount, CustomerReference, BankReference, Description, AppendDate, CreateBatchId, BOADescId, PayerName, EntryDescription, ReceiverId, OriginatorId, StandardEntryClassCode, PaymentInformation, TraceTypeCode, CheckOrEFTFlag, CheckOrEFTNumber, PayerIdNumber, OriginatingCompanySupplementalCode, Notes, BatchId, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.BankFileKey, source.BankFileId, source.DataImportId, TRIM(source.BankName), source.DepositDate, TRIM(source.ACCOUNT), TRIM(source.DataType), source.BaiCode, source.Amount, source.CustomerReference, TRIM(source.BankReference), TRIM(source.Description), source.AppendDate, TRIM(source.CreateBatchId), source.BOADescId, TRIM(source.PayerName), TRIM(source.EntryDescription), TRIM(source.ReceiverId), TRIM(source.OriginatorId), TRIM(source.StandardEntryClassCode), TRIM(source.PaymentInformation), TRIM(source.TraceTypeCode), TRIM(source.CheckOrEFTFlag), TRIM(source.CheckOrEFTNumber), TRIM(source.PayerIdNumber), TRIM(source.OriginatingCompanySupplementalCode), TRIM(source.Notes), TRIM(source.BatchId), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BankFileKey
      FROM {{ params.param_psc_core_dataset_name }}.CMT_FactBankFile
      GROUP BY BankFileKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CMT_FactBankFile');
ELSE
  COMMIT TRANSACTION;
END IF;
