
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Onbase_FactWorkflowDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.Onbase_FactWorkflowDetail AS source
ON target.OnbaseWorkFlowDetailKey = source.OnbaseWorkFlowDetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.DocumentHandle = source.DocumentHandle,
 target.KeywordDocumentHandle = source.KeywordDocumentHandle,
 target.KeywordEntryDate = source.KeywordEntryDate,
 target.KeywordExitDate = source.KeywordExitDate,
 target.KeywordLifeCycleName = TRIM(source.KeywordLifeCycleName),
 target.KeywordDocumentType = TRIM(source.KeywordDocumentType),
 target.KeywordQueueName = TRIM(source.KeywordQueueName),
 target.KeywordUserID = TRIM(source.KeywordUserID),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.OnbaseWorkFlowDetailKey = source.OnbaseWorkFlowDetailKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (DocumentHandle, KeywordDocumentHandle, KeywordEntryDate, KeywordExitDate, KeywordLifeCycleName, KeywordDocumentType, KeywordQueueName, KeywordUserID, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, OnbaseWorkFlowDetailKey, DWLastUpdateDateTime)
  VALUES (source.DocumentHandle, source.KeywordDocumentHandle, source.KeywordEntryDate, source.KeywordExitDate, TRIM(source.KeywordLifeCycleName), TRIM(source.KeywordDocumentType), TRIM(source.KeywordQueueName), TRIM(source.KeywordUserID), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.OnbaseWorkFlowDetailKey, source.DWLastUpdateDateTime);

-- DELETE FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactWorkflowDetail WHERE DATE(InsertedDTM) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseWorkFlowDetailKey
      FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactWorkflowDetail
      GROUP BY OnbaseWorkFlowDetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Onbase_FactWorkflowDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
