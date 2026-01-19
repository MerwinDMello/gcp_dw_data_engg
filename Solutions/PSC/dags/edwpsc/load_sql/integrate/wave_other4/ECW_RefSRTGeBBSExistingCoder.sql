
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGeBBSExistingCoder AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTGeBBSExistingCoder AS source
ON target.GeBBSExistingCoderKey = source.GeBBSExistingCoderKey
WHEN MATCHED THEN
  UPDATE SET
  target.GeBBSExistingCoderKey = source.GeBBSExistingCoderKey,
 target.User34 = TRIM(source.User34),
 target.EmployeeName = TRIM(source.EmployeeName),
 target.UserRole = TRIM(source.UserRole),
 target.CoderTypeforContractRate = TRIM(source.CoderTypeforContractRate),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (GeBBSExistingCoderKey, User34, EmployeeName, UserRole, CoderTypeforContractRate, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.GeBBSExistingCoderKey, TRIM(source.User34), TRIM(source.EmployeeName), TRIM(source.UserRole), TRIM(source.CoderTypeforContractRate), source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GeBBSExistingCoderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGeBBSExistingCoder
      GROUP BY GeBBSExistingCoderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGeBBSExistingCoder');
ELSE
  COMMIT TRANSACTION;
END IF;
