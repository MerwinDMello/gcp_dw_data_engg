
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimPayerLiabilityChange AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimPayerLiabilityChange AS source
ON target.ClaimPayerLiabilityChangeKey = source.ClaimPayerLiabilityChangeKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimPayerLiabilityChangeKey = source.ClaimPayerLiabilityChangeKey,
 target.RegionID = source.RegionID,
 target.ClaimNumber = source.ClaimNumber,
 target.BilledToId = source.BilledToId,
 target.BilledToIdType = source.BilledToIdType,
 target.TransferHistoryDate = source.TransferHistoryDate,
 target.TransferHistoryTime = source.TransferHistoryTime,
 target.UserId = TRIM(source.UserId),
 target.TransferHistoryModifiedDate = source.TransferHistoryModifiedDate,
 target.UserKey = source.UserKey,
 target.ClaimKey = source.ClaimKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.ClaimPayerKey = source.ClaimPayerKey,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (ClaimPayerLiabilityChangeKey, RegionID, ClaimNumber, BilledToId, BilledToIdType, TransferHistoryDate, TransferHistoryTime, UserId, TransferHistoryModifiedDate, UserKey, ClaimKey, EncounterKey, EncounterID, ClaimPayerKey, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime, ArchivedRecord)
  VALUES (source.ClaimPayerLiabilityChangeKey, source.RegionID, source.ClaimNumber, source.BilledToId, source.BilledToIdType, source.TransferHistoryDate, source.TransferHistoryTime, TRIM(source.UserId), source.TransferHistoryModifiedDate, source.UserKey, source.ClaimKey, source.EncounterKey, source.EncounterID, source.ClaimPayerKey, source.SourceAPrimaryKeyValue, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimPayerLiabilityChangeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactClaimPayerLiabilityChange
      GROUP BY ClaimPayerLiabilityChangeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactClaimPayerLiabilityChange');
ELSE
  COMMIT TRANSACTION;
END IF;
