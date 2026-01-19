
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_FactEDR AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_FactEDR AS source
ON target.OpenConnectEDRKey = source.OpenConnectEDRKey
WHEN MATCHED THEN
  UPDATE SET
  target.OpenConnectEDRKey = source.OpenConnectEDRKey,
 target.MessageId = source.MessageId,
 target.ActionId = TRIM(source.ActionId),
 target.ActionName = TRIM(source.ActionName),
 target.ErrorCategory = TRIM(source.ErrorCategory),
 target.CrosswalkError = TRIM(source.CrosswalkError),
 target.CrosswalkErrorRollup = TRIM(source.CrosswalkErrorRollup),
 target.DateErrorReceived = source.DateErrorReceived,
 target.ErrorId = TRIM(source.ErrorId),
 target.ErrorMessage = TRIM(source.ErrorMessage),
 target.ArtivaLoadDate = source.ArtivaLoadDate,
 target.Notes = TRIM(source.Notes),
 target.OriginalArtivaLoadDate = source.OriginalArtivaLoadDate,
 target.RouteStepStatusReasonId = TRIM(source.RouteStepStatusReasonId),
 target.RouteStepStatusReason = TRIM(source.RouteStepStatusReason),
 target.RouteStepId = TRIM(source.RouteStepId),
 target.RouteStepName = TRIM(source.RouteStepName),
 target.PSOCEOCID = TRIM(source.PSOCEOCID),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (OpenConnectEDRKey, MessageId, ActionId, ActionName, ErrorCategory, CrosswalkError, CrosswalkErrorRollup, DateErrorReceived, ErrorId, ErrorMessage, ArtivaLoadDate, Notes, OriginalArtivaLoadDate, RouteStepStatusReasonId, RouteStepStatusReason, RouteStepId, RouteStepName, PSOCEOCID, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
  VALUES (source.OpenConnectEDRKey, source.MessageId, TRIM(source.ActionId), TRIM(source.ActionName), TRIM(source.ErrorCategory), TRIM(source.CrosswalkError), TRIM(source.CrosswalkErrorRollup), source.DateErrorReceived, TRIM(source.ErrorId), TRIM(source.ErrorMessage), source.ArtivaLoadDate, TRIM(source.Notes), source.OriginalArtivaLoadDate, TRIM(source.RouteStepStatusReasonId), TRIM(source.RouteStepStatusReason), TRIM(source.RouteStepId), TRIM(source.RouteStepName), TRIM(source.PSOCEOCID), TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OpenConnectEDRKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_FactEDR
      GROUP BY OpenConnectEDRKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_FactEDR');
ELSE
  COMMIT TRANSACTION;
END IF;
