
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSuppressionDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_FactSuppressionDetail AS source
ON target.OpenConnectSuppressionDetailKey = source.OpenConnectSuppressionDetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.OpenConnectSuppressionDetailKey = source.OpenConnectSuppressionDetailKey,
 target.MessageCreatedDate = source.MessageCreatedDate,
 target.RouteStep = TRIM(source.RouteStep),
 target.StatusReason = TRIM(source.StatusReason),
 target.StatusMessage = TRIM(source.StatusMessage),
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdatedDate = source.DWLastUpdatedDate,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (OpenConnectSuppressionDetailKey, MessageCreatedDate, RouteStep, StatusReason, StatusMessage, DeleteFlag, DWLastUpdatedDate, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.OpenConnectSuppressionDetailKey, source.MessageCreatedDate, TRIM(source.RouteStep), TRIM(source.StatusReason), TRIM(source.StatusMessage), source.DeleteFlag, source.DWLastUpdatedDate, source.SourceAPrimaryKeyValue, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OpenConnectSuppressionDetailKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSuppressionDetail
      GROUP BY OpenConnectSuppressionDetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSuppressionDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
