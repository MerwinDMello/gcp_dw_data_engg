
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentTypeEPIC AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefPaymentTypeEPIC AS source
ON target.EpicPaymentTypeKey = source.EpicPaymentTypeKey
WHEN MATCHED THEN
  UPDATE SET
  target.EpicPaymentTypeKey = source.EpicPaymentTypeKey,
 target.PaymentType = TRIM(source.PaymentType),
 target.PaymentTypeName = TRIM(source.PaymentTypeName),
 target.PaymentTypeDescription = TRIM(source.PaymentTypeDescription),
 target.PaymentTypeShortName = TRIM(source.PaymentTypeShortName),
 target.PaymentTypeActive = source.PaymentTypeActive,
 target.ProcId = TRIM(source.ProcId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EpicPaymentTypeKey, PaymentType, PaymentTypeName, PaymentTypeDescription, PaymentTypeShortName, PaymentTypeActive, ProcId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EpicPaymentTypeKey, TRIM(source.PaymentType), TRIM(source.PaymentTypeName), TRIM(source.PaymentTypeDescription), TRIM(source.PaymentTypeShortName), source.PaymentTypeActive, TRIM(source.ProcId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EpicPaymentTypeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentTypeEPIC
      GROUP BY EpicPaymentTypeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentTypeEPIC');
ELSE
  COMMIT TRANSACTION;
END IF;
