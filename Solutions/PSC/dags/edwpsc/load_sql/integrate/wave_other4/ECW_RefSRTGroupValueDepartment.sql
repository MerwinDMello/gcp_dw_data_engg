
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGroupValueDepartment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTGroupValueDepartment AS source
ON target.GroupValueDeptKey = source.GroupValueDeptKey
WHEN MATCHED THEN
  UPDATE SET
  target.GroupValueDeptKey = source.GroupValueDeptKey,
 target.LOB = TRIM(source.LOB),
 target.GroupValue = TRIM(source.GroupValue),
 target.Department = source.Department,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (GroupValueDeptKey, LOB, GroupValue, Department, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.GroupValueDeptKey, TRIM(source.LOB), TRIM(source.GroupValue), source.Department, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GroupValueDeptKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGroupValueDepartment
      GROUP BY GroupValueDeptKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGroupValueDepartment');
ELSE
  COMMIT TRANSACTION;
END IF;
