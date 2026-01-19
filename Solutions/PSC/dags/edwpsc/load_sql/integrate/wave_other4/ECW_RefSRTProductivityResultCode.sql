
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTProductivityResultCode AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTProductivityResultCode AS source
ON target.ProductivityResultCodeKey = source.ProductivityResultCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProductivityResultCodeKey = source.ProductivityResultCodeKey,
 target.COID = source.COID,
 target.ControlNumber = source.ControlNumber,
 target.InvoiceName = TRIM(source.InvoiceName),
 target.ResultCode = TRIM(source.ResultCode),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ProductivityResultCodeKey, COID, ControlNumber, InvoiceName, ResultCode, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ProductivityResultCodeKey, source.COID, source.ControlNumber, TRIM(source.InvoiceName), TRIM(source.ResultCode), source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProductivityResultCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTProductivityResultCode
      GROUP BY ProductivityResultCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTProductivityResultCode');
ELSE
  COMMIT TRANSACTION;
END IF;
