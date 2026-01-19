
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERAFilesProcessed AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERAFilesProcessed AS source
ON target.ERAFilesProcessedKey = source.ERAFilesProcessedKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERAFilesProcessedKey = source.ERAFilesProcessedKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.SourceFileName = TRIM(source.SourceFileName),
 target.FileName = TRIM(source.FileName),
 target.SourceFileCreatedDate = source.SourceFileCreatedDate,
 target.DateCreated = source.DateCreated,
 target.TransactionNumber = TRIM(source.TransactionNumber),
 target.TRN03 = TRIM(source.TRN03),
 target.IET_processed = source.IET_processed,
 target.PostedDate = source.PostedDate,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeletedFlag = source.DeletedFlag,
 target.FullClaimNumber = TRIM(source.FullClaimNumber),
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (ERAFilesProcessedKey, ClaimKey, ClaimNumber, SourceFileName, FileName, SourceFileCreatedDate, DateCreated, TransactionNumber, TRN03, IET_processed, PostedDate, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERAFilesProcessedKey, source.ClaimKey, source.ClaimNumber, TRIM(source.SourceFileName), TRIM(source.FileName), source.SourceFileCreatedDate, source.DateCreated, TRIM(source.TransactionNumber), TRIM(source.TRN03), source.IET_processed, source.PostedDate, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERAFilesProcessedKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERAFilesProcessed
      GROUP BY ERAFilesProcessedKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERAFilesProcessed');
ELSE
  COMMIT TRANSACTION;
END IF;
