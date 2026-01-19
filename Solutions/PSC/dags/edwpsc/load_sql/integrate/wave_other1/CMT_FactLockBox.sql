
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CMT_FactLockBox AS target
USING {{ params.param_psc_stage_dataset_name }}.CMT_FactLockBox AS source
ON target.LockboxKey = source.LockboxKey
WHEN MATCHED THEN
  UPDATE SET
  target.LockboxKey = source.LockboxKey,
 target.LockboxFileId = source.LockboxFileId,
 target.DataImportId = source.DataImportId,
 target.BankName = TRIM(source.BankName),
 target.DepositDate = source.DepositDate,
 target.AccountNumber = TRIM(source.AccountNumber),
 target.CheckAmount = source.CheckAmount,
 target.CheckNumber = TRIM(source.CheckNumber),
 target.LockboxNumber = source.LockboxNumber,
 target.BatchNumber = TRIM(source.BatchNumber),
 target.BatchTotal = source.BatchTotal,
 target.AppendDate = source.AppendDate,
 target.CreateBatchId = TRIM(source.CreateBatchId),
 target.BankAccountId = source.BankAccountId,
 target.BankAccountNumber = TRIM(source.BankAccountNumber),
 target.AccountName = TRIM(source.AccountName),
 target.RoutingNumber = source.RoutingNumber,
 target.GLAccountsId = source.GLAccountsId,
 target.LegalEntityId = source.LegalEntityId,
 target.DepositCOID = TRIM(source.DepositCOID),
 target.BankId = source.BankId,
 target.EndDate = source.EndDate,
 target.CreateDate = source.CreateDate,
 target.CreateUserId = TRIM(source.CreateUserId),
 target.IsBatch = source.IsBatch,
 target.ControlsAccountId = source.ControlsAccountId,
 target.IsZBT = source.IsZBT,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (LockboxKey, LockboxFileId, DataImportId, BankName, DepositDate, AccountNumber, CheckAmount, CheckNumber, LockboxNumber, BatchNumber, BatchTotal, AppendDate, CreateBatchId, BankAccountId, BankAccountNumber, AccountName, RoutingNumber, GLAccountsId, LegalEntityId, DepositCOID, BankId, EndDate, CreateDate, CreateUserId, IsBatch, ControlsAccountId, IsZBT, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.LockboxKey, source.LockboxFileId, source.DataImportId, TRIM(source.BankName), source.DepositDate, TRIM(source.AccountNumber), source.CheckAmount, TRIM(source.CheckNumber), source.LockboxNumber, TRIM(source.BatchNumber), source.BatchTotal, source.AppendDate, TRIM(source.CreateBatchId), source.BankAccountId, TRIM(source.BankAccountNumber), TRIM(source.AccountName), source.RoutingNumber, source.GLAccountsId, source.LegalEntityId, TRIM(source.DepositCOID), source.BankId, source.EndDate, source.CreateDate, TRIM(source.CreateUserId), source.IsBatch, source.ControlsAccountId, source.IsZBT, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LockboxKey
      FROM {{ params.param_psc_core_dataset_name }}.CMT_FactLockBox
      GROUP BY LockboxKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CMT_FactLockBox');
ELSE
  COMMIT TRANSACTION;
END IF;
