
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactEPICInventory AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactEPICInventory AS source
ON target.CCUEpicInventoryKey = source.CCUEpicInventoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUEpicInventoryKey = source.CCUEpicInventoryKey,
 target.RegionKey = source.RegionKey,
 target.RegionName = TRIM(source.RegionName),
 target.Coid = TRIM(source.Coid),
 target.ClaimKey = source.ClaimKey,
 target.PatientMRN = TRIM(source.PatientMRN),
 target.AccountId = TRIM(source.AccountId),
 target.ServiceDate = source.ServiceDate,
 target.CreatedDate = source.CreatedDate,
 target.DaysInWorkQueue = source.DaysInWorkQueue,
 target.DaysSinceServiceDate = source.DaysSinceServiceDate,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.LiabilityIplanKey = source.LiabilityIplanKey,
 target.LiabilityFinancialClass = source.LiabilityFinancialClass,
 target.LiabilityFinancialClassName = TRIM(source.LiabilityFinancialClassName),
 target.LiabilityIplanName = TRIM(source.LiabilityIplanName),
 target.EpicFinancialClass = source.EpicFinancialClass,
 target.EpicFinancialClassDesc = TRIM(source.EpicFinancialClassDesc),
 target.ServicingProviderName = TRIM(source.ServicingProviderName),
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderNPI = TRIM(source.ServicingProviderNPI),
 target.FacilityName = TRIM(source.FacilityName),
 target.ClaimNumber = source.ClaimNumber,
 target.FacilityKey = source.FacilityKey,
 target.VisitTypeKey = source.VisitTypeKey,
 target.VisitStatusKey = source.VisitStatusKey,
 target.EncounterId = source.EncounterId,
 target.EncounterKey = source.EncounterKey,
 target.EncounterLockFlag = TRIM(source.EncounterLockFlag),
 target.CCUGoLiveStatus = TRIM(source.CCUGoLiveStatus),
 target.CCUGoLiveDate = source.CCUGoLiveDate,
 target.AssignedToVendor = TRIM(source.AssignedToVendor),
 target.VendorName = TRIM(source.VendorName),
 target.VendorAssignedDate = source.VendorAssignedDate,
 target.OWNER = TRIM(source.OWNER),
 target.GroupAssignment = TRIM(source.GroupAssignment),
 target.SourceSystem = TRIM(source.SourceSystem),
 target.LoadDate = source.LoadDate,
 target.Worked = source.Worked,
 target.WorkedDate = source.WorkedDate,
 target.ServiceDateStr = TRIM(source.ServiceDateStr),
 target.WorkQueueId = TRIM(source.WorkQueueId),
 target.WorkQueueType = TRIM(source.WorkQueueType),
 target.WorkQueueName = TRIM(source.WorkQueueName),
 target.EnteredWorkQueueDate = source.EnteredWorkQueueDate,
 target.WorkQueueTab = TRIM(source.WorkQueueTab),
 target.CCUNonCustomerNPIConfigKey = source.CCUNonCustomerNPIConfigKey,
 target.CCUNonCustomerConfigKey = source.CCUNonCustomerConfigKey,
 target.ProcedureCategory1 = TRIM(source.ProcedureCategory1),
 target.ProcedureCategory2 = TRIM(source.ProcedureCategory2),
 target.CPTCodes = TRIM(source.CPTCodes),
 target.WorkQueues = TRIM(source.WorkQueues),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.EncounterId2 = source.EncounterId2,
 target.BusincessDaysSinceServiceDate = source.BusincessDaysSinceServiceDate,
 target.BusincessDaysSinceLastStatusChange = source.BusincessDaysSinceLastStatusChange,
 target.BusincessDaysSinceLastModified = source.BusincessDaysSinceLastModified,
 target.daysinceModifiedDTM = source.daysinceModifiedDTM,
 target.CPTModifiers = TRIM(source.CPTModifiers),
 target.ICDCodes = TRIM(source.ICDCodes)
WHEN NOT MATCHED THEN
  INSERT (CCUEpicInventoryKey, RegionKey, RegionName, Coid, ClaimKey, PatientMRN, AccountId, ServiceDate, CreatedDate, DaysInWorkQueue, DaysSinceServiceDate, TotalBalanceAmt, LiabilityIplanKey, LiabilityFinancialClass, LiabilityFinancialClassName, LiabilityIplanName, EpicFinancialClass, EpicFinancialClassDesc, ServicingProviderName, ServicingProviderKey, ServicingProviderNPI, FacilityName, ClaimNumber, FacilityKey, VisitTypeKey, VisitStatusKey, EncounterId, EncounterKey, EncounterLockFlag, CCUGoLiveStatus, CCUGoLiveDate, AssignedToVendor, VendorName, VendorAssignedDate, OWNER, GroupAssignment, SourceSystem, LoadDate, Worked, WorkedDate, ServiceDateStr, WorkQueueId, WorkQueueType, WorkQueueName, EnteredWorkQueueDate, WorkQueueTab, CCUNonCustomerNPIConfigKey, CCUNonCustomerConfigKey, ProcedureCategory1, ProcedureCategory2, CPTCodes, WorkQueues, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, EncounterId2, BusincessDaysSinceServiceDate, BusincessDaysSinceLastStatusChange, BusincessDaysSinceLastModified, daysinceModifiedDTM, CPTModifiers, ICDCodes)
  VALUES (source.CCUEpicInventoryKey, source.RegionKey, TRIM(source.RegionName), TRIM(source.Coid), source.ClaimKey, TRIM(source.PatientMRN), TRIM(source.AccountId), source.ServiceDate, source.CreatedDate, source.DaysInWorkQueue, source.DaysSinceServiceDate, source.TotalBalanceAmt, source.LiabilityIplanKey, source.LiabilityFinancialClass, TRIM(source.LiabilityFinancialClassName), TRIM(source.LiabilityIplanName), source.EpicFinancialClass, TRIM(source.EpicFinancialClassDesc), TRIM(source.ServicingProviderName), source.ServicingProviderKey, TRIM(source.ServicingProviderNPI), TRIM(source.FacilityName), source.ClaimNumber, source.FacilityKey, source.VisitTypeKey, source.VisitStatusKey, source.EncounterId, source.EncounterKey, TRIM(source.EncounterLockFlag), TRIM(source.CCUGoLiveStatus), source.CCUGoLiveDate, TRIM(source.AssignedToVendor), TRIM(source.VendorName), source.VendorAssignedDate, TRIM(source.OWNER), TRIM(source.GroupAssignment), TRIM(source.SourceSystem), source.LoadDate, source.Worked, source.WorkedDate, TRIM(source.ServiceDateStr), TRIM(source.WorkQueueId), TRIM(source.WorkQueueType), TRIM(source.WorkQueueName), source.EnteredWorkQueueDate, TRIM(source.WorkQueueTab), source.CCUNonCustomerNPIConfigKey, source.CCUNonCustomerConfigKey, TRIM(source.ProcedureCategory1), TRIM(source.ProcedureCategory2), TRIM(source.CPTCodes), TRIM(source.WorkQueues), source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.EncounterId2, source.BusincessDaysSinceServiceDate, source.BusincessDaysSinceLastStatusChange, source.BusincessDaysSinceLastModified, source.daysinceModifiedDTM, TRIM(source.CPTModifiers), TRIM(source.ICDCodes));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUEpicInventoryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactEPICInventory
      GROUP BY CCUEpicInventoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactEPICInventory');
ELSE
  COMMIT TRANSACTION;
END IF;
