
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefFacilityMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefFacilityMeditechExpanse AS source
ON target.FacilityKey = source.FacilityKey
WHEN MATCHED THEN
  UPDATE SET
  target.FacilityKey = source.FacilityKey,
 target.RegionKey = source.RegionKey,
 target.FacilityName = TRIM(source.FacilityName),
 target.FacilityAccountNumberPrefix = TRIM(source.FacilityAccountNumberPrefix),
 target.DeleteFlag = source.DeleteFlag,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (FacilityKey, RegionKey, FacilityName, FacilityAccountNumberPrefix, DeleteFlag, SourcePrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.FacilityKey, source.RegionKey, TRIM(source.FacilityName), TRIM(source.FacilityAccountNumberPrefix), source.DeleteFlag, TRIM(source.SourcePrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT FacilityKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefFacilityMeditechExpanse
      GROUP BY FacilityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefFacilityMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
