
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CMT_FactMonthlyTransactions AS target
USING {{ params.param_psc_stage_dataset_name }}.CMT_FactMonthlyTransactions AS source
ON target.TBNumber = source.TBNumber AND target.PostedDate = source.PostedDate AND target.AmountPosted = source.AmountPosted AND target.User34ID = source.User34ID AND target.PostType = source.PostType
WHEN MATCHED THEN
  UPDATE SET
  target.TBNumber = TRIM(source.TBNumber),
 target.TransactionType = TRIM(source.TransactionType),
 target.PostType = TRIM(source.PostType),
 target.PostedDate = source.PostedDate,
 target.AmountPosted = source.AmountPosted,
 target.User34ID = TRIM(source.User34ID),
 target.FirstName = TRIM(source.FirstName),
 target.LastName = TRIM(source.LastName),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (TBNumber, TransactionType, PostType, PostedDate, AmountPosted, User34ID, FirstName, LastName, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (TRIM(source.TBNumber), TRIM(source.TransactionType), TRIM(source.PostType), source.PostedDate, source.AmountPosted, TRIM(source.User34ID), TRIM(source.FirstName), TRIM(source.LastName), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TBNumber, PostedDate, AmountPosted, User34ID, PostType
      FROM {{ params.param_psc_core_dataset_name }}.CMT_FactMonthlyTransactions
      GROUP BY TBNumber, PostedDate, AmountPosted, User34ID, PostType
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CMT_FactMonthlyTransactions');
ELSE
  COMMIT TRANSACTION;
END IF;
