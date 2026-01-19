
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactCodingStatusMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactCodingStatusMeditechExpanse AS source
ON target.CCUCodingStatusKey = source.CCUCodingStatusKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUCodingStatusKey = source.CCUCodingStatusKey,
 target.RegionKey = source.RegionKey,
 target.WorkListUserKey = source.WorkListUserKey,
 target.WorkListDateTime = source.WorkListDateTime,
 target.WorkListStatus = TRIM(source.WorkListStatus),
 target.CodingStatus = TRIM(source.CodingStatus),
 target.CodingStatusDate = source.CodingStatusDate,
 target.CodingStatusUserKey = source.CodingStatusUserKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (CCUCodingStatusKey, RegionKey, WorkListUserKey, WorkListDateTime, WorkListStatus, CodingStatus, CodingStatusDate, CodingStatusUserKey, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.CCUCodingStatusKey, source.RegionKey, source.WorkListUserKey, source.WorkListDateTime, TRIM(source.WorkListStatus), TRIM(source.CodingStatus), source.CodingStatusDate, source.CodingStatusUserKey, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUCodingStatusKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactCodingStatusMeditechExpanse
      GROUP BY CCUCodingStatusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactCodingStatusMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
