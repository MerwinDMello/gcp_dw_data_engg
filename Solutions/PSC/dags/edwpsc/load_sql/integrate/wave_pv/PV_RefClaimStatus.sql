
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefClaimStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefClaimStatus AS source
ON target.ClaimStatusKey = source.ClaimStatusKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimStatusKey = source.ClaimStatusKey,
 target.ClaimStatusName = TRIM(source.ClaimStatusName),
 target.ClaimStatusShortName = TRIM(source.ClaimStatusShortName),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.ClaimStatusFullDesc = TRIM(source.ClaimStatusFullDesc)
WHEN NOT MATCHED THEN
  INSERT (ClaimStatusKey, ClaimStatusName, ClaimStatusShortName, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, ClaimStatusFullDesc)
  VALUES (source.ClaimStatusKey, TRIM(source.ClaimStatusName), TRIM(source.ClaimStatusShortName), source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, TRIM(source.ClaimStatusFullDesc));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimStatusKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefClaimStatus
      GROUP BY ClaimStatusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefClaimStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
