
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefIplan AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefIplan AS source
ON target.IplanKey = source.IplanKey
WHEN MATCHED THEN
  UPDATE SET
  target.IplanKey = source.IplanKey,
 target.FinancialClassKey = source.FinancialClassKey,
 target.IplanGroupCactusKey = source.IplanGroupCactusKey,
 target.IplanGroupFinancialKey = source.IplanGroupFinancialKey,
 target.IplanName = TRIM(source.IplanName),
 target.IplanPhone = TRIM(source.IplanPhone),
 target.IplanPhone2 = TRIM(source.IplanPhone2),
 target.IplanFax = TRIM(source.IplanFax),
 target.IplanEmail = TRIM(source.IplanEmail),
 target.IplanPrimaryAddressLine1 = TRIM(source.IplanPrimaryAddressLine1),
 target.IplanPrimaryAddressLine2 = TRIM(source.IplanPrimaryAddressLine2),
 target.IplanPrimaryGeographyKey = source.IplanPrimaryGeographyKey,
 target.IplanPayorID = TRIM(source.IplanPayorID),
 target.IplanERAPayorID = TRIM(source.IplanERAPayorID),
 target.IplanFeeSchedId = source.IplanFeeSchedId,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.Inactive = source.Inactive,
 target.Notes = TRIM(source.Notes),
 target.EMCPayerId = TRIM(source.EMCPayerId),
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (IplanKey, FinancialClassKey, IplanGroupCactusKey, IplanGroupFinancialKey, IplanName, IplanPhone, IplanPhone2, IplanFax, IplanEmail, IplanPrimaryAddressLine1, IplanPrimaryAddressLine2, IplanPrimaryGeographyKey, IplanPayorID, IplanERAPayorID, IplanFeeSchedId, SourcePrimaryKeyValue, SourceARecordLastUpdated, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, Inactive, Notes, EMCPayerId, SysStartTime, SysEndTime)
  VALUES (source.IplanKey, source.FinancialClassKey, source.IplanGroupCactusKey, source.IplanGroupFinancialKey, TRIM(source.IplanName), TRIM(source.IplanPhone), TRIM(source.IplanPhone2), TRIM(source.IplanFax), TRIM(source.IplanEmail), TRIM(source.IplanPrimaryAddressLine1), TRIM(source.IplanPrimaryAddressLine2), source.IplanPrimaryGeographyKey, TRIM(source.IplanPayorID), TRIM(source.IplanERAPayorID), source.IplanFeeSchedId, source.SourcePrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.Inactive, TRIM(source.Notes), TRIM(source.EMCPayerId), source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IplanKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefIplan
      GROUP BY IplanKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefIplan');
ELSE
  COMMIT TRANSACTION;
END IF;
