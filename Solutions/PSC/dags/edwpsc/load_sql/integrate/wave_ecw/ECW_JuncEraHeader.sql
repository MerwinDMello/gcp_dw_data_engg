
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncEraHeader AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncEraHeader AS source
ON target.ECWEraHeaderKey = source.ECWEraHeaderKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWEraHeaderKey = source.ECWEraHeaderKey,
 target.RegionKey = source.RegionKey,
 target.ImportedBy = source.ImportedBy,
 target.ImportedByUserKey = source.ImportedByUserKey,
 target.ImportedDate = source.ImportedDate,
 target.RawFileName = TRIM(source.RawFileName),
 target.XML = TRIM(source.XML),
 target.ISA13 = TRIM(source.ISA13),
 target.ISA06 = TRIM(source.ISA06),
 target.DeletedBy = source.DeletedBy,
 target.DeletedByUserKey = source.DeletedByUserKey,
 target.DeletedDate = source.DeletedDate,
 target.DeletedReason = TRIM(source.DeletedReason),
 target.TRN02 = TRIM(source.TRN02),
 target.TRN03 = TRIM(source.TRN03),
 target.ANSI5010 = source.ANSI5010,
 target.deleteFlag = source.deleteFlag,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ECWEraHeaderKey, RegionKey, ImportedBy, ImportedByUserKey, ImportedDate, RawFileName, XML, ISA13, ISA06, DeletedBy, DeletedByUserKey, DeletedDate, DeletedReason, TRN02, TRN03, ANSI5010, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ECWEraHeaderKey, source.RegionKey, source.ImportedBy, source.ImportedByUserKey, source.ImportedDate, TRIM(source.RawFileName), TRIM(source.XML), TRIM(source.ISA13), TRIM(source.ISA06), source.DeletedBy, source.DeletedByUserKey, source.DeletedDate, TRIM(source.DeletedReason), TRIM(source.TRN02), TRIM(source.TRN03), source.ANSI5010, source.deleteFlag, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWEraHeaderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncEraHeader
      GROUP BY ECWEraHeaderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncEraHeader');
ELSE
  COMMIT TRANSACTION;
END IF;
