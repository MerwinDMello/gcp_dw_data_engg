
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RefNonCustomerNPIConfig AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RefNonCustomerNPIConfig AS source
ON target.CCUNonCustomerNPIConfigKey = source.CCUNonCustomerNPIConfigKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUNonCustomerNPIConfigKey = source.CCUNonCustomerNPIConfigKey,
 target.CCUNonCustomerConfigKey = source.CCUNonCustomerConfigKey,
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ExecutionOrder = source.ExecutionOrder,
 target.EffectiveDateKey = source.EffectiveDateKey,
 target.TerminationDateKey = source.TerminationDateKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ApprovedFlag = TRIM(source.ApprovedFlag),
 target.ApprovedDate = source.ApprovedDate,
 target.ApprovedByID = TRIM(source.ApprovedByID),
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime,
 target.COID = TRIM(source.COID)
WHEN NOT MATCHED THEN
  INSERT (CCUNonCustomerNPIConfigKey, CCUNonCustomerConfigKey, ProviderNPI, ExecutionOrder, EffectiveDateKey, TerminationDateKey, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ApprovedFlag, ApprovedDate, ApprovedByID, SysStartTime, SysEndTime, COID)
  VALUES (source.CCUNonCustomerNPIConfigKey, source.CCUNonCustomerConfigKey, TRIM(source.ProviderNPI), source.ExecutionOrder, source.EffectiveDateKey, source.TerminationDateKey, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ApprovedFlag), source.ApprovedDate, TRIM(source.ApprovedByID), source.SysStartTime, source.SysEndTime, TRIM(source.COID));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUNonCustomerNPIConfigKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RefNonCustomerNPIConfig
      GROUP BY CCUNonCustomerNPIConfigKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RefNonCustomerNPIConfig');
ELSE
  COMMIT TRANSACTION;
END IF;
