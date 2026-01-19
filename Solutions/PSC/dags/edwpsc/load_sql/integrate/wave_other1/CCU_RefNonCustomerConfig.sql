
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RefNonCustomerConfig AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RefNonCustomerConfig AS source
ON target.CCUNonCustomerConfigKey = source.CCUNonCustomerConfigKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUNonCustomerConfigKey = source.CCUNonCustomerConfigKey,
 target.CCUNonCustomerConfigLabel = TRIM(source.CCUNonCustomerConfigLabel),
 target.COIDOperator = TRIM(source.COIDOperator),
 target.COID = TRIM(source.COID),
 target.SourceSystemOperator = TRIM(source.SourceSystemOperator),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.POSOperator = TRIM(source.POSOperator),
 target.POS = TRIM(source.POS),
 target.ProcedureCategory1Operator = TRIM(source.ProcedureCategory1Operator),
 target.ProcedureCategory1 = TRIM(source.ProcedureCategory1),
 target.VisitStatusOperator = TRIM(source.VisitStatusOperator),
 target.VisitStatus = TRIM(source.VisitStatus),
 target.VisitTypeOperator = TRIM(source.VisitTypeOperator),
 target.VisitType = TRIM(source.VisitType),
 target.LockFlagOperator = TRIM(source.LockFlagOperator),
 target.LockFlag = TRIM(source.LockFlag),
 target.CurrentInventoryOwner = TRIM(source.CurrentInventoryOwner),
 target.CurrentInventoryOwnerOperator = TRIM(source.CurrentInventoryOwnerOperator),
 target.CCUInventoryKeyQryOperator = TRIM(source.CCUInventoryKeyQryOperator),
 target.CCUInventoryKeyQry = TRIM(source.CCUInventoryKeyQry),
 target.InventoryOwner = TRIM(source.InventoryOwner),
 target.InventoryType = TRIM(source.InventoryType),
 target.GroupAssignment = TRIM(source.GroupAssignment),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ProcedureCategory2Operator = TRIM(source.ProcedureCategory2Operator),
 target.ProcedureCategory2 = TRIM(source.ProcedureCategory2),
 target.InventoryTypeOperator = TRIM(source.InventoryTypeOperator),
 target.ProviderSpecialty = TRIM(source.ProviderSpecialty),
 target.ProviderSpecialtyOperator = TRIM(source.ProviderSpecialtyOperator)
WHEN NOT MATCHED THEN
  INSERT (CCUNonCustomerConfigKey, CCUNonCustomerConfigLabel, COIDOperator, COID, SourceSystemOperator, SourceSystemCode, POSOperator, POS, ProcedureCategory1Operator, ProcedureCategory1, VisitStatusOperator, VisitStatus, VisitTypeOperator, VisitType, LockFlagOperator, LockFlag, CurrentInventoryOwner, CurrentInventoryOwnerOperator, CCUInventoryKeyQryOperator, CCUInventoryKeyQry, InventoryOwner, InventoryType, GroupAssignment, SourcePrimaryKeyValue, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ProcedureCategory2Operator, ProcedureCategory2, InventoryTypeOperator, ProviderSpecialty, ProviderSpecialtyOperator)
  VALUES (source.CCUNonCustomerConfigKey, TRIM(source.CCUNonCustomerConfigLabel), TRIM(source.COIDOperator), TRIM(source.COID), TRIM(source.SourceSystemOperator), TRIM(source.SourceSystemCode), TRIM(source.POSOperator), TRIM(source.POS), TRIM(source.ProcedureCategory1Operator), TRIM(source.ProcedureCategory1), TRIM(source.VisitStatusOperator), TRIM(source.VisitStatus), TRIM(source.VisitTypeOperator), TRIM(source.VisitType), TRIM(source.LockFlagOperator), TRIM(source.LockFlag), TRIM(source.CurrentInventoryOwner), TRIM(source.CurrentInventoryOwnerOperator), TRIM(source.CCUInventoryKeyQryOperator), TRIM(source.CCUInventoryKeyQry), TRIM(source.InventoryOwner), TRIM(source.InventoryType), TRIM(source.GroupAssignment), source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ProcedureCategory2Operator), TRIM(source.ProcedureCategory2), TRIM(source.InventoryTypeOperator), TRIM(source.ProviderSpecialty), TRIM(source.ProviderSpecialtyOperator));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUNonCustomerConfigKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RefNonCustomerConfig
      GROUP BY CCUNonCustomerConfigKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RefNonCustomerConfig');
ELSE
  COMMIT TRANSACTION;
END IF;
