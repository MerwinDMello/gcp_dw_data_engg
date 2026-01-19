
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentSource AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefPaymentSource AS source
ON target.PaymentSourceKey = source.PaymentSourceKey
WHEN MATCHED THEN
  UPDATE SET
  target.PaymentSourceKey = source.PaymentSourceKey,
 target.PaymentSourceName = TRIM(source.PaymentSourceName),
 target.PaymentSourceTitle = TRIM(source.PaymentSourceTitle),
 target.PaymentSourceAbbr = TRIM(source.PaymentSourceAbbr),
 target.PaymentSourceInternalId = TRIM(source.PaymentSourceInternalId),
 target.PaymentSourceC = TRIM(source.PaymentSourceC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (PaymentSourceKey, PaymentSourceName, PaymentSourceTitle, PaymentSourceAbbr, PaymentSourceInternalId, PaymentSourceC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PaymentSourceKey, TRIM(source.PaymentSourceName), TRIM(source.PaymentSourceTitle), TRIM(source.PaymentSourceAbbr), TRIM(source.PaymentSourceInternalId), TRIM(source.PaymentSourceC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PaymentSourceKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentSource
      GROUP BY PaymentSourceKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentSource');
ELSE
  COMMIT TRANSACTION;
END IF;
