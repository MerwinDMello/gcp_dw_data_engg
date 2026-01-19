
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CMT_FactBatchProductivity AS target
USING {{ params.param_psc_stage_dataset_name }}.CMT_FactBatchProductivity AS source
ON target.BatchProductivityKey = source.BatchProductivityKey
WHEN MATCHED THEN
  UPDATE SET
  target.BatchProductivityKey = source.BatchProductivityKey,
 target.UserId = TRIM(source.UserId),
 target.FirstName = TRIM(source.FirstName),
 target.LastName = TRIM(source.LastName),
 target.BatchId = TRIM(source.BatchId),
 target.TaxId = TRIM(source.TaxId),
 target.BankPayerName = TRIM(source.BankPayerName),
 target.DateOfAction = source.DateOfAction,
 target.TimeOfAction = source.TimeOfAction,
 target.BatchStatusBeforeAction = TRIM(source.BatchStatusBeforeAction),
 target.BatchStatusAfterAction = TRIM(source.BatchStatusAfterAction),
 target.Action = TRIM(source.Action),
 target.Notes = TRIM(source.Notes),
 target.COID = TRIM(source.COID),
 target.Amount = source.Amount,
 target.CheckNumber = TRIM(source.CheckNumber),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (BatchProductivityKey, UserId, FirstName, LastName, BatchId, TaxId, BankPayerName, DateOfAction, TimeOfAction, BatchStatusBeforeAction, BatchStatusAfterAction, Action, Notes, COID, Amount, CheckNumber, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.BatchProductivityKey, TRIM(source.UserId), TRIM(source.FirstName), TRIM(source.LastName), TRIM(source.BatchId), TRIM(source.TaxId), TRIM(source.BankPayerName), source.DateOfAction, source.TimeOfAction, TRIM(source.BatchStatusBeforeAction), TRIM(source.BatchStatusAfterAction), TRIM(source.Action), TRIM(source.Notes), TRIM(source.COID), source.Amount, TRIM(source.CheckNumber), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BatchProductivityKey
      FROM {{ params.param_psc_core_dataset_name }}.CMT_FactBatchProductivity
      GROUP BY BatchProductivityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CMT_FactBatchProductivity');
ELSE
  COMMIT TRANSACTION;
END IF;
