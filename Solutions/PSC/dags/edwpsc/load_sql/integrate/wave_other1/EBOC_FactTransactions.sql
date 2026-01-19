
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EBOC_FactTransactions AS target
USING {{ params.param_psc_stage_dataset_name }}.EBOC_FactTransactions AS source
ON target.KeyID = source.KeyID
WHEN MATCHED THEN
  UPDATE SET
  target.KeyID = TRIM(source.KeyID),
 target.UserName = TRIM(source.UserName),
 target.UserId = TRIM(source.UserId),
 target.GLDate = TRIM(source.GLDate),
 target.LineCount = TRIM(source.LineCount),
 target.Dollars = source.Dollars,
 target.TreasuryBatchNumber = TRIM(source.TreasuryBatchNumber),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (KeyID, UserName, UserId, GLDate, LineCount, Dollars, TreasuryBatchNumber, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (TRIM(source.KeyID), TRIM(source.UserName), TRIM(source.UserId), TRIM(source.GLDate), TRIM(source.LineCount), source.Dollars, TRIM(source.TreasuryBatchNumber), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT KeyID
      FROM {{ params.param_psc_core_dataset_name }}.EBOC_FactTransactions
      GROUP BY KeyID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EBOC_FactTransactions');
ELSE
  COMMIT TRANSACTION;
END IF;
