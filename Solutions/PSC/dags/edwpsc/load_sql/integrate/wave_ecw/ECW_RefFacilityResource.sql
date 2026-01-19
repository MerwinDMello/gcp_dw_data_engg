
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefFacilityResource AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefFacilityResource AS source
ON target.FacilityResourceKey = source.FacilityResourceKey
WHEN MATCHED THEN
  UPDATE SET
  target.FacilityResourceKey = source.FacilityResourceKey,
 target.FacilityResourceName = TRIM(source.FacilityResourceName),
 target.FacilityResourceUserType = source.FacilityResourceUserType,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (FacilityResourceKey, FacilityResourceName, FacilityResourceUserType, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
  VALUES (source.FacilityResourceKey, TRIM(source.FacilityResourceName), source.FacilityResourceUserType, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT FacilityResourceKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefFacilityResource
      GROUP BY FacilityResourceKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefFacilityResource');
ELSE
  COMMIT TRANSACTION;
END IF;
