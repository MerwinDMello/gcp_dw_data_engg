
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefPbBillingStat AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefPbBillingStat AS source
ON target.PbBillingStatKey = source.PbBillingStatKey
WHEN MATCHED THEN
  UPDATE SET
  target.PbBillingStatKey = source.PbBillingStatKey,
 target.PbBillingStatName = TRIM(source.PbBillingStatName),
 target.PbBillingStatTitle = TRIM(source.PbBillingStatTitle),
 target.PbBillingStatAbbr = TRIM(source.PbBillingStatAbbr),
 target.PbBillingStatInternalId = TRIM(source.PbBillingStatInternalId),
 target.PbBillingStatusC = TRIM(source.PbBillingStatusC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (PbBillingStatKey, PbBillingStatName, PbBillingStatTitle, PbBillingStatAbbr, PbBillingStatInternalId, PbBillingStatusC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PbBillingStatKey, TRIM(source.PbBillingStatName), TRIM(source.PbBillingStatTitle), TRIM(source.PbBillingStatAbbr), TRIM(source.PbBillingStatInternalId), TRIM(source.PbBillingStatusC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PbBillingStatKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefPbBillingStat
      GROUP BY PbBillingStatKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefPbBillingStat');
ELSE
  COMMIT TRANSACTION;
END IF;
