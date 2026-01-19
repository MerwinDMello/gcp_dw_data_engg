
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTOnbaseContract AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTOnbaseContract AS source
ON target.OnbaseContractKey = source.OnbaseContractKey
WHEN MATCHED THEN
  UPDATE SET
  target.OnbaseContractKey = source.OnbaseContractKey,
 target.OnbaseContractLocation = TRIM(source.OnbaseContractLocation),
 target.OnbaseContractSourceSystem = TRIM(source.OnbaseContractSourceSystem),
 target.OnbaseContractLOBType = TRIM(source.OnbaseContractLOBType),
 target.OnbaseContractQueueName = TRIM(source.OnbaseContractQueueName),
 target.OnbaseContractControlNum = TRIM(source.OnbaseContractControlNum),
 target.OnbaseContractExecutiveName = TRIM(source.OnbaseContractExecutiveName),
 target.OnbaseContractDirectorName = TRIM(source.OnbaseContractDirectorName),
 target.OnbaseContractManagerName = TRIM(source.OnbaseContractManagerName),
 target.OnbaseContractResponsibleDepartment = TRIM(source.OnbaseContractResponsibleDepartment),
 target.OnbaseContractVendorFlag = TRIM(source.OnbaseContractVendorFlag),
 target.OnbaseContractVendorName = TRIM(source.OnbaseContractVendorName),
 target.OnbaseContractEmployeeType = TRIM(source.OnbaseContractEmployeeType),
 target.OnbaseContractLOB = TRIM(source.OnbaseContractLOB),
 target.OnbaseContractActiveFlag = TRIM(source.OnbaseContractActiveFlag),
 target.OnbaseContractEffectiveDateKey = source.OnbaseContractEffectiveDateKey,
 target.OnbaseContractTermedDateKey = source.OnbaseContractTermedDateKey,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (OnbaseContractKey, OnbaseContractLocation, OnbaseContractSourceSystem, OnbaseContractLOBType, OnbaseContractQueueName, OnbaseContractControlNum, OnbaseContractExecutiveName, OnbaseContractDirectorName, OnbaseContractManagerName, OnbaseContractResponsibleDepartment, OnbaseContractVendorFlag, OnbaseContractVendorName, OnbaseContractEmployeeType, OnbaseContractLOB, OnbaseContractActiveFlag, OnbaseContractEffectiveDateKey, OnbaseContractTermedDateKey, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.OnbaseContractKey, TRIM(source.OnbaseContractLocation), TRIM(source.OnbaseContractSourceSystem), TRIM(source.OnbaseContractLOBType), TRIM(source.OnbaseContractQueueName), TRIM(source.OnbaseContractControlNum), TRIM(source.OnbaseContractExecutiveName), TRIM(source.OnbaseContractDirectorName), TRIM(source.OnbaseContractManagerName), TRIM(source.OnbaseContractResponsibleDepartment), TRIM(source.OnbaseContractVendorFlag), TRIM(source.OnbaseContractVendorName), TRIM(source.OnbaseContractEmployeeType), TRIM(source.OnbaseContractLOB), TRIM(source.OnbaseContractActiveFlag), source.OnbaseContractEffectiveDateKey, source.OnbaseContractTermedDateKey, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseContractKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTOnbaseContract
      GROUP BY OnbaseContractKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTOnbaseContract');
ELSE
  COMMIT TRANSACTION;
END IF;
