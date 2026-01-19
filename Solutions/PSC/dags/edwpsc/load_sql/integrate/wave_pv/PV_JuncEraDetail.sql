
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_JuncEraDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_JuncEraDetail AS source
ON target.PVEraDetailKey = source.PVEraDetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.PVEraDetailKey = source.PVEraDetailKey,
 target.RegionKey = source.RegionKey,
 target.FileId = source.FileId,
 target.PaymentId = source.PaymentId,
 target.PayorName = TRIM(source.PayorName),
 target.ProviderName = TRIM(source.ProviderName),
 target.PostedBy = TRIM(source.PostedBy),
 target.PostedByUserKey = source.PostedByUserKey,
 target.PostedDate = source.PostedDate,
 target.ST02 = TRIM(source.ST02),
 target.TransactionType = TRIM(source.TransactionType),
 target.TransactionAmount = source.TransactionAmount,
 target.TransactionMethod = TRIM(source.TransactionMethod),
 target.TransactionDate = TRIM(source.TransactionDate),
 target.TransactionNumber = TRIM(source.TransactionNumber),
 target.DeletedBy = TRIM(source.DeletedBy),
 target.DeletedByUserKey = source.DeletedByUserKey,
 target.DeletedDate = source.DeletedDate,
 target.DeletedReason = TRIM(source.DeletedReason),
 target.PayorId = source.PayorId,
 target.PayorIplanKey = source.PayorIplanKey,
 target.ProviderId = source.ProviderId,
 target.ProviderKey = source.ProviderKey,
 target.StOrderId = source.StOrderId,
 target.GsOrderId = source.GsOrderId,
 target.IsaOrderId = source.IsaOrderId,
 target.PostingMessage = TRIM(source.PostingMessage),
 target.deleteFlag = source.deleteFlag,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (PVEraDetailKey, RegionKey, FileId, PaymentId, PayorName, ProviderName, PostedBy, PostedByUserKey, PostedDate, ST02, TransactionType, TransactionAmount, TransactionMethod, TransactionDate, TransactionNumber, DeletedBy, DeletedByUserKey, DeletedDate, DeletedReason, PayorId, PayorIplanKey, ProviderId, ProviderKey, StOrderId, GsOrderId, IsaOrderId, PostingMessage, deleteFlag, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PVEraDetailKey, source.RegionKey, source.FileId, source.PaymentId, TRIM(source.PayorName), TRIM(source.ProviderName), TRIM(source.PostedBy), source.PostedByUserKey, source.PostedDate, TRIM(source.ST02), TRIM(source.TransactionType), source.TransactionAmount, TRIM(source.TransactionMethod), TRIM(source.TransactionDate), TRIM(source.TransactionNumber), TRIM(source.DeletedBy), source.DeletedByUserKey, source.DeletedDate, TRIM(source.DeletedReason), source.PayorId, source.PayorIplanKey, source.ProviderId, source.ProviderKey, source.StOrderId, source.GsOrderId, source.IsaOrderId, TRIM(source.PostingMessage), source.deleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PVEraDetailKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_JuncEraDetail
      GROUP BY PVEraDetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_JuncEraDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
