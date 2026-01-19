
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_JuncFirstLastKeyMeasure AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_JuncFirstLastKeyMeasure AS source
ON target.JuncOpenConnectFirstLastKeyMeasureKey = source.JuncOpenConnectFirstLastKeyMeasureKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncOpenConnectFirstLastKeyMeasureKey = source.JuncOpenConnectFirstLastKeyMeasureKey,
 target.OpenConnectMessageKey = source.OpenConnectMessageKey,
 target.MessageCreatedDate = source.MessageCreatedDate,
 target.FirstStatusReason = TRIM(source.FirstStatusReason),
 target.FirstStatusMessage = TRIM(source.FirstStatusMessage),
 target.FirstStatusDate = source.FirstStatusDate,
 target.FirstCategory = TRIM(source.FirstCategory),
 target.FirstCategoryDate = source.FirstCategoryDate,
 target.LastStatusReason = TRIM(source.LastStatusReason),
 target.LastStatusMessage = TRIM(source.LastStatusMessage),
 target.LastStatusDate = source.LastStatusDate,
 target.LastCategory = TRIM(source.LastCategory),
 target.LastCategoryDate = source.LastCategoryDate,
 target.FirstIsError = source.FirstIsError,
 target.FirstIsErrorDate = source.FirstIsErrorDate,
 target.FirstIsErrorEcw = source.FirstIsErrorEcw,
 target.FirstIsErrorEcwDate = source.FirstIsErrorEcwDate,
 target.FirstSuppressFlag = source.FirstSuppressFlag,
 target.FirstSuppressFlagDate = source.FirstSuppressFlagDate,
 target.FirstDFTDateReceived = source.FirstDFTDateReceived,
 target.LastDFTDateReceived = source.LastDFTDateReceived,
 target.FirstBatchDate = source.FirstBatchDate,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.FirstPendingFlag = source.FirstPendingFlag,
 target.FirsteCWSuppressFlag = source.FirsteCWSuppressFlag,
 target.FirsteCWSuppressFlagDate = source.FirsteCWSuppressFlagDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncOpenConnectFirstLastKeyMeasureKey, OpenConnectMessageKey, MessageCreatedDate, FirstStatusReason, FirstStatusMessage, FirstStatusDate, FirstCategory, FirstCategoryDate, LastStatusReason, LastStatusMessage, LastStatusDate, LastCategory, LastCategoryDate, FirstIsError, FirstIsErrorDate, FirstIsErrorEcw, FirstIsErrorEcwDate, FirstSuppressFlag, FirstSuppressFlagDate, FirstDFTDateReceived, LastDFTDateReceived, FirstBatchDate, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FirstPendingFlag, FirsteCWSuppressFlag, FirsteCWSuppressFlagDate, DWLastUpdateDateTime)
  VALUES (source.JuncOpenConnectFirstLastKeyMeasureKey, source.OpenConnectMessageKey, source.MessageCreatedDate, TRIM(source.FirstStatusReason), TRIM(source.FirstStatusMessage), source.FirstStatusDate, TRIM(source.FirstCategory), source.FirstCategoryDate, TRIM(source.LastStatusReason), TRIM(source.LastStatusMessage), source.LastStatusDate, TRIM(source.LastCategory), source.LastCategoryDate, source.FirstIsError, source.FirstIsErrorDate, source.FirstIsErrorEcw, source.FirstIsErrorEcwDate, source.FirstSuppressFlag, source.FirstSuppressFlagDate, source.FirstDFTDateReceived, source.LastDFTDateReceived, source.FirstBatchDate, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.FirstPendingFlag, source.FirsteCWSuppressFlag, source.FirsteCWSuppressFlagDate, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncOpenConnectFirstLastKeyMeasureKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_JuncFirstLastKeyMeasure
      GROUP BY JuncOpenConnectFirstLastKeyMeasureKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_JuncFirstLastKeyMeasure');
ELSE
  COMMIT TRANSACTION;
END IF;
