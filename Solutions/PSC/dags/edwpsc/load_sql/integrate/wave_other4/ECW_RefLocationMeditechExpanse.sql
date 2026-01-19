
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefLocationMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefLocationMeditechExpanse AS source
ON target.LocationKey = source.LocationKey
WHEN MATCHED THEN
  UPDATE SET
  target.LocationKey = source.LocationKey,
 target.RegionKey = source.RegionKey,
 target.LocationName = TRIM(source.LocationName),
 target.LocationType = TRIM(source.LocationType),
 target.SiteID = TRIM(source.SiteID),
 target.COID = TRIM(source.COID),
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
  INSERT (LocationKey, RegionKey, LocationName, LocationType, SiteID, COID, DeleteFlag, SourcePrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.LocationKey, source.RegionKey, TRIM(source.LocationName), TRIM(source.LocationType), TRIM(source.SiteID), TRIM(source.COID), source.DeleteFlag, TRIM(source.SourcePrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LocationKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefLocationMeditechExpanse
      GROUP BY LocationKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefLocationMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
