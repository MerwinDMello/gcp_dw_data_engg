
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimPayerCPID AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimPayerCPID AS source
ON target.ClaimPayerCPIDKey = source.ClaimPayerCPIDKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimPayerCPIDKey = source.ClaimPayerCPIDKey,
 target.ClaimKey = source.ClaimKey,
 target.IplanKey = source.IplanKey,
 target.CPID = TRIM(source.CPID),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimPayerCPIDKey, ClaimKey, IplanKey, CPID, RegionKey, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimPayerCPIDKey, source.ClaimKey, source.IplanKey, TRIM(source.CPID), source.RegionKey, TRIM(source.SourceAPrimaryKeyValue), TRIM(source.SourceBPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimPayerCPIDKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimPayerCPID
      GROUP BY ClaimPayerCPIDKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimPayerCPID');
ELSE
  COMMIT TRANSACTION;
END IF;
