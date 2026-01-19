
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_JuncEraHeader AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_JuncEraHeader AS source
ON target.PVEraHeaderKey = source.PVEraHeaderKey
WHEN MATCHED THEN
  UPDATE SET
  target.PVEraHeaderKey = source.PVEraHeaderKey,
 target.RegionKey = source.RegionKey,
 target.ImportedBy = TRIM(source.ImportedBy),
 target.ImportedByUserKey = source.ImportedByUserKey,
 target.ImportedDate = source.ImportedDate,
 target.RawFileName = TRIM(source.RawFileName),
 target.XML = TRIM(source.XML),
 target.ISA13 = TRIM(source.ISA13),
 target.ISA06 = TRIM(source.ISA06),
 target.DeletedBy = TRIM(source.DeletedBy),
 target.DeletedByUserKey = source.DeletedByUserKey,
 target.DeletedDate = source.DeletedDate,
 target.DeletedReason = TRIM(source.DeletedReason),
 target.TRN02 = TRIM(source.TRN02),
 target.TRN03 = TRIM(source.TRN03),
 target.ANSI5010 = source.ANSI5010,
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (PVEraHeaderKey, RegionKey, ImportedBy, ImportedByUserKey, ImportedDate, RawFileName, XML, ISA13, ISA06, DeletedBy, DeletedByUserKey, DeletedDate, DeletedReason, TRN02, TRN03, ANSI5010, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PVEraHeaderKey, source.RegionKey, TRIM(source.ImportedBy), source.ImportedByUserKey, source.ImportedDate, TRIM(source.RawFileName), TRIM(source.XML), TRIM(source.ISA13), TRIM(source.ISA06), TRIM(source.DeletedBy), source.DeletedByUserKey, source.DeletedDate, TRIM(source.DeletedReason), TRIM(source.TRN02), TRIM(source.TRN03), source.ANSI5010, source.DeleteFlag, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PVEraHeaderKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_JuncEraHeader
      GROUP BY PVEraHeaderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_JuncEraHeader');
ELSE
  COMMIT TRANSACTION;
END IF;
