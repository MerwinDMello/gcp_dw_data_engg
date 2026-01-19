
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Onbase_FactUserAction AS target
USING {{ params.param_psc_stage_dataset_name }}.Onbase_FactUserAction AS source
ON target.OnbaseUserActionId = source.OnbaseUserActionId
WHEN MATCHED THEN
  UPDATE SET
  target.OnbaseUserActionId = source.OnbaseUserActionId,
 target.TransactionNum = source.TransactionNum,
 target.DocHandleID = source.DocHandleID,
 target.ActionItem = TRIM(source.ActionItem),
 target.ActionDate = source.ActionDate,
 target.ItemName = TRIM(source.ItemName),
 target.ActionBy = TRIM(source.ActionBy),
 target.ActionCategory = TRIM(source.ActionCategory),
 target.ActionNum = source.ActionNum,
 target.SubactionNum = source.SubactionNum,
 target.DWLastupdateDateTime = source.DWLastupdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.Insertedby = TRIM(source.Insertedby),
 target.InsertedDTM = source.InsertedDTM
WHEN NOT MATCHED THEN
  INSERT (OnbaseUserActionId, TransactionNum, DocHandleID, ActionItem, ActionDate, ItemName, ActionBy, ActionCategory, ActionNum, SubactionNum, DWLastupdateDateTime, SourceSystemCode, Insertedby, InsertedDTM)
  VALUES (source.OnbaseUserActionId, source.TransactionNum, source.DocHandleID, TRIM(source.ActionItem), source.ActionDate, TRIM(source.ItemName), TRIM(source.ActionBy), TRIM(source.ActionCategory), source.ActionNum, source.SubactionNum, source.DWLastupdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.Insertedby), source.InsertedDTM);

-- DELETE FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactUserAction WHERE DATE(InsertedDTM) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseUserActionId
      FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactUserAction
      GROUP BY OnbaseUserActionId
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Onbase_FactUserAction');
ELSE
  COMMIT TRANSACTION;
END IF;
