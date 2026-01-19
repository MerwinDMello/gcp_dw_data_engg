
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefAccountCrosswalk AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefAccountCrosswalk AS source
ON target.AccountId = source.AccountId AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.AccountKey = source.AccountKey,
 target.AccountId = source.AccountId,
 target.AccountInternalId = source.AccountInternalId,
 target.RegionKey = source.RegionKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.AccountName = TRIM(source.AccountName),
 target.AccountZcName = TRIM(source.AccountZcName),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (AccountKey, AccountId, AccountInternalId, RegionKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, AccountName, AccountZcName, DWLastUpdateDateTime)
  VALUES (source.AccountKey, source.AccountId, source.AccountInternalId, source.RegionKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.AccountName), TRIM(source.AccountZcName), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT AccountId, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefAccountCrosswalk
      GROUP BY AccountId, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefAccountCrosswalk');
ELSE
  COMMIT TRANSACTION;
END IF;
