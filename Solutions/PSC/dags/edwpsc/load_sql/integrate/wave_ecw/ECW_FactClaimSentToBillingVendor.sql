
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimSentToBillingVendor AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimSentToBillingVendor AS source
ON target.SentToBillingVendorKey = source.SentToBillingVendorKey
WHEN MATCHED THEN
  UPDATE SET
  target.SentToBillingVendorKey = source.SentToBillingVendorKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.IplanKey = source.IplanKey,
 target.SentToBillingVendorMessage = TRIM(source.SentToBillingVendorMessage),
 target.SentToBillingVendorDateKey = source.SentToBillingVendorDateKey,
 target.SentToBillingVendorDateTime = source.SentToBillingVendorDateTime,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (SentToBillingVendorKey, ClaimKey, ClaimNumber, RegionKey, Coid, IplanKey, SentToBillingVendorMessage, SentToBillingVendorDateKey, SentToBillingVendorDateTime, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ArchivedRecord)
  VALUES (source.SentToBillingVendorKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.IplanKey, TRIM(source.SentToBillingVendorMessage), source.SentToBillingVendorDateKey, source.SentToBillingVendorDateTime, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SentToBillingVendorKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactClaimSentToBillingVendor
      GROUP BY SentToBillingVendorKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactClaimSentToBillingVendor');
ELSE
  COMMIT TRANSACTION;
END IF;
