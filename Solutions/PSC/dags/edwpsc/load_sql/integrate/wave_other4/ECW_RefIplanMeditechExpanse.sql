
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefIplanMeditechExpanse AS source
ON target.IplanKey = source.IplanKey
WHEN MATCHED THEN
  UPDATE SET
  target.IplanKey = source.IplanKey,
 target.RegionKey = source.RegionKey,
 target.IplanName = TRIM(source.IplanName),
 target.IplanPrimaryAddressLine1 = TRIM(source.IplanPrimaryAddressLine1),
 target.IplanPrimaryAddressLine2 = TRIM(source.IplanPrimaryAddressLine2),
 target.IplanPrimaryGeographyKey = source.IplanPrimaryGeographyKey,
 target.IplanGroupID = TRIM(source.IplanGroupID),
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
  INSERT (IplanKey, RegionKey, IplanName, IplanPrimaryAddressLine1, IplanPrimaryAddressLine2, IplanPrimaryGeographyKey, IplanGroupID, DeleteFlag, SourcePrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.IplanKey, source.RegionKey, TRIM(source.IplanName), TRIM(source.IplanPrimaryAddressLine1), TRIM(source.IplanPrimaryAddressLine2), source.IplanPrimaryGeographyKey, TRIM(source.IplanGroupID), source.DeleteFlag, TRIM(source.SourcePrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IplanKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechExpanse
      GROUP BY IplanKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
