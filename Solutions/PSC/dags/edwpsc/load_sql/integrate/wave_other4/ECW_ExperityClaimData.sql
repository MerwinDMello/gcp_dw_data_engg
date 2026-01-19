
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_ExperityClaimData AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_ExperityClaimData AS source
ON target.Id = source.Id
WHEN MATCHED THEN
  UPDATE SET
  target.Id = source.Id,
 target.FileName = TRIM(source.FileName),
 target.ClaimNumber = TRIM(source.ClaimNumber),
 target.ClaimAmount = source.ClaimAmount,
 target.BatchId = TRIM(source.BatchId),
 target.ClaimSource = TRIM(source.ClaimSource),
 target.AppendDate = source.AppendDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (Id, FileName, ClaimNumber, ClaimAmount, BatchId, ClaimSource, AppendDate, DWLastUpdateDateTime)
  VALUES (source.Id, TRIM(source.FileName), TRIM(source.ClaimNumber), source.ClaimAmount, TRIM(source.BatchId), TRIM(source.ClaimSource), source.AppendDate, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT Id
      FROM {{ params.param_psc_core_dataset_name }}.ECW_ExperityClaimData
      GROUP BY Id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_ExperityClaimData');
ELSE
  COMMIT TRANSACTION;
END IF;
