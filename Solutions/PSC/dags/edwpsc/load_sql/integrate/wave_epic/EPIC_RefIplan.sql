
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefIplan AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefIplan AS source
ON target.IplanKey = source.IplanKey
WHEN MATCHED THEN
  UPDATE SET
  target.IplanKey = source.IplanKey,
 target.IplanName = TRIM(source.IplanName),
 target.IplanShortName = TRIM(source.IplanShortName),
 target.IplanAddress1 = TRIM(source.IplanAddress1),
 target.IplanAddress2 = TRIM(source.IplanAddress2),
 target.IplanCity = TRIM(source.IplanCity),
 target.IplanState = TRIM(source.IplanState),
 target.IplanZip = TRIM(source.IplanZip),
 target.IplanPhone = TRIM(source.IplanPhone),
 target.PayorId = TRIM(source.PayorId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.IplanPrimaryGeographyKey = source.IplanPrimaryGeographyKey,
 target.IplanGroupFinancialKey = source.IplanGroupFinancialKey,
 target.FinancialClassKey = source.FinancialClassKey,
 target.EpicFinancialClass = source.EpicFinancialClass,
 target.EpicFinancialClassDesc = TRIM(source.EpicFinancialClassDesc),
 target.EpicFinancialClassGrouped = TRIM(source.EpicFinancialClassGrouped)
WHEN NOT MATCHED THEN
  INSERT (IplanKey, IplanName, IplanShortName, IplanAddress1, IplanAddress2, IplanCity, IplanState, IplanZip, IplanPhone, PayorId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, IplanPrimaryGeographyKey, IplanGroupFinancialKey, FinancialClassKey, EpicFinancialClass, EpicFinancialClassDesc, EpicFinancialClassGrouped)
  VALUES (source.IplanKey, TRIM(source.IplanName), TRIM(source.IplanShortName), TRIM(source.IplanAddress1), TRIM(source.IplanAddress2), TRIM(source.IplanCity), TRIM(source.IplanState), TRIM(source.IplanZip), TRIM(source.IplanPhone), TRIM(source.PayorId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.IplanPrimaryGeographyKey, source.IplanGroupFinancialKey, source.FinancialClassKey, source.EpicFinancialClass, TRIM(source.EpicFinancialClassDesc), TRIM(source.EpicFinancialClassGrouped));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IplanKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefIplan
      GROUP BY IplanKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefIplan');
ELSE
  COMMIT TRANSACTION;
END IF;
