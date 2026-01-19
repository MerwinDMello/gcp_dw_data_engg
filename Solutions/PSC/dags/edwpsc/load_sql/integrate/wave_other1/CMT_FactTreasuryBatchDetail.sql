
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CMT_FactTreasuryBatchDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.CMT_FactTreasuryBatchDetail AS source
ON target.BatchDetailId = source.BatchDetailId AND target.BatchHistoryId = source.BatchHistoryId AND target.RegionId = source.RegionId
WHEN MATCHED THEN
  UPDATE SET
  target.BatchID = TRIM(source.BatchID),
 target.BatchDate = source.BatchDate,
 target.DepositDate = source.DepositDate,
 target.CheckReference = TRIM(source.CheckReference),
 target.Amount = source.Amount,
 target.BankAccount = TRIM(source.BankAccount),
 target.BankReference = TRIM(source.BankReference),
 target.Description = TRIM(source.Description),
 target.BankName = TRIM(source.BankName),
 target.PayerName = TRIM(source.PayerName),
 target.COID = TRIM(source.COID),
 target.TaxId = source.TaxId,
 target.LockboxNumber = source.LockboxNumber,
 target.LOCATION = TRIM(source.LOCATION),
 target.LocationID = source.LocationID,
 target.BatchState = source.BatchState,
 target.BatchStateDesc = TRIM(source.BatchStateDesc),
 target.Transactiontype = source.Transactiontype,
 target.TransactionTypeDesc = TRIM(source.TransactionTypeDesc),
 target.BatchHistoryId = source.BatchHistoryId,
 target.UserID = TRIM(source.UserID),
 target.HistoryReasonId = source.HistoryReasonId,
 target.HistoryReasonDescription = TRIM(source.HistoryReasonDescription),
 target.Notes = TRIM(source.Notes),
 target.UpdateDate = source.UpdateDate,
 target.ReceiverName = TRIM(source.ReceiverName),
 target.PaymentInformation = TRIM(source.PaymentInformation),
 target.RegionId = TRIM(source.RegionId),
 target.CheckOrEFTNumber = TRIM(source.CheckOrEFTNumber),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.BatchDetailId = source.BatchDetailId,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (BatchID, BatchDate, DepositDate, CheckReference, Amount, BankAccount, BankReference, Description, BankName, PayerName, COID, TaxId, LockboxNumber, LOCATION, LocationID, BatchState, BatchStateDesc, Transactiontype, TransactionTypeDesc, BatchHistoryId, UserID, HistoryReasonId, HistoryReasonDescription, Notes, UpdateDate, ReceiverName, PaymentInformation, RegionId, CheckOrEFTNumber, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, BatchDetailId, DWLastUpdateDateTime)
  VALUES (TRIM(source.BatchID), source.BatchDate, source.DepositDate, TRIM(source.CheckReference), source.Amount, TRIM(source.BankAccount), TRIM(source.BankReference), TRIM(source.Description), TRIM(source.BankName), TRIM(source.PayerName), TRIM(source.COID), source.TaxId, source.LockboxNumber, TRIM(source.LOCATION), source.LocationID, source.BatchState, TRIM(source.BatchStateDesc), source.Transactiontype, TRIM(source.TransactionTypeDesc), source.BatchHistoryId, TRIM(source.UserID), source.HistoryReasonId, TRIM(source.HistoryReasonDescription), TRIM(source.Notes), source.UpdateDate, TRIM(source.ReceiverName), TRIM(source.PaymentInformation), TRIM(source.RegionId), TRIM(source.CheckOrEFTNumber), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.BatchDetailId, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BatchDetailId, BatchHistoryId, RegionId
      FROM {{ params.param_psc_core_dataset_name }}.CMT_FactTreasuryBatchDetail
      GROUP BY BatchDetailId, BatchHistoryId, RegionId
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CMT_FactTreasuryBatchDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
