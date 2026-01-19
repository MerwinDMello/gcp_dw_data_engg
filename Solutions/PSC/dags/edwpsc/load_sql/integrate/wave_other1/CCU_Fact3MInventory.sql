
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_Fact3MInventory AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_Fact3MInventory AS source
ON target.CCU3MInventoryKey = source.CCU3MInventoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCU3MInventoryKey = source.CCU3MInventoryKey,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.Worklist = TRIM(source.Worklist),
 target.OWNER = TRIM(source.OWNER),
 target.RegionKey = source.RegionKey,
 target.COID = TRIM(source.COID),
 target.CCUGoLiveStatus = TRIM(source.CCUGoLiveStatus),
 target.CCUGoLiveDate = source.CCUGoLiveDate,
 target.AssignedToVendor = TRIM(source.AssignedToVendor),
 target.VendorName = TRIM(source.VendorName),
 target.VendorAssignedDate = source.VendorAssignedDate,
 target.LocationKey = source.LocationKey,
 target.PracticeKey = source.PracticeKey,
 target.FacilityKey = source.FacilityKey,
 target.EncounterID = TRIM(source.EncounterID),
 target.ServiceDate = source.ServiceDate,
 target.PatientKey = source.PatientKey,
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.VisitStatusDescription = TRIM(source.VisitStatusDescription),
 target.EncounterLockedDate = source.EncounterLockedDate,
 target.InventoryType = TRIM(source.InventoryType),
 target.ClaimVsEncounterFlag = TRIM(source.ClaimVsEncounterFlag),
 target.SourceLastUpdatedDateTime = source.SourceLastUpdatedDateTime,
 target.GroupAssignment = TRIM(source.GroupAssignment),
 target.ServiceDateConsolidationDateFlag = TRIM(source.ServiceDateConsolidationDateFlag),
 target.NonBillable = source.NonBillable,
 target.AccountID = TRIM(source.AccountID),
 target.VisitType = TRIM(source.VisitType),
 target.DischargeDate = source.DischargeDate,
 target.CareProviderFirstName = TRIM(source.CareProviderFirstName),
 target.CareProviderLastName = TRIM(source.CareProviderLastName),
 target.CareProviderMiddleName = TRIM(source.CareProviderMiddleName),
 target.RecordStatus = TRIM(source.RecordStatus),
 target.DocHoldReason = TRIM(source.DocHoldReason),
 target.Encounter3MKey = source.Encounter3MKey,
 target.LoadDate = source.LoadDate,
 target.Worked = source.Worked,
 target.WorkedDate = source.WorkedDate,
 target.EncounterLastModifiedDate = source.EncounterLastModifiedDate,
 target.DaysSinceModifiedDate = source.DaysSinceModifiedDate,
 target.DaysSinceServiceDate = source.DaysSinceServiceDate,
 target.BusincessDaysSinceServiceDate = source.BusincessDaysSinceServiceDate,
 target.BusincessDaysSinceLastModified = source.BusincessDaysSinceLastModified,
 target.ProcedureCategory1 = TRIM(source.ProcedureCategory1),
 target.ProcedureCategory2 = TRIM(source.ProcedureCategory2),
 target.CPTCodes = TRIM(source.CPTCodes),
 target.CCUNonCustomerConfigKey = source.CCUNonCustomerConfigKey,
 target.CCUNonCustomerNPIConfigKey = source.CCUNonCustomerNPIConfigKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.CPTModifiers = TRIM(source.CPTModifiers),
 target.ICDCodes = TRIM(source.ICDCodes),
 target.LoadKey = source.LoadKey,
 target.FacilityID = TRIM(source.FacilityID)
WHEN NOT MATCHED THEN
  INSERT (CCU3MInventoryKey, SourceSystem, Worklist, OWNER, RegionKey, COID, CCUGoLiveStatus, CCUGoLiveDate, AssignedToVendor, VendorName, VendorAssignedDate, LocationKey, PracticeKey, FacilityKey, EncounterID, ServiceDate, PatientKey, PatientFirstName, PatientLastName, PatientMiddleName, ServicingProviderKey, VisitStatusDescription, EncounterLockedDate, InventoryType, ClaimVsEncounterFlag, SourceLastUpdatedDateTime, GroupAssignment, ServiceDateConsolidationDateFlag, NonBillable, AccountID, VisitType, DischargeDate, CareProviderFirstName, CareProviderLastName, CareProviderMiddleName, RecordStatus, DocHoldReason, Encounter3MKey, LoadDate, Worked, WorkedDate, EncounterLastModifiedDate, DaysSinceModifiedDate, DaysSinceServiceDate, BusincessDaysSinceServiceDate, BusincessDaysSinceLastModified, ProcedureCategory1, ProcedureCategory2, CPTCodes, CCUNonCustomerConfigKey, CCUNonCustomerNPIConfigKey, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, CPTModifiers, ICDCodes, LoadKey, FacilityID)
  VALUES (source.CCU3MInventoryKey, TRIM(source.SourceSystem), TRIM(source.Worklist), TRIM(source.OWNER), source.RegionKey, TRIM(source.COID), TRIM(source.CCUGoLiveStatus), source.CCUGoLiveDate, TRIM(source.AssignedToVendor), TRIM(source.VendorName), source.VendorAssignedDate, source.LocationKey, source.PracticeKey, source.FacilityKey, TRIM(source.EncounterID), source.ServiceDate, source.PatientKey, TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PatientMiddleName), source.ServicingProviderKey, TRIM(source.VisitStatusDescription), source.EncounterLockedDate, TRIM(source.InventoryType), TRIM(source.ClaimVsEncounterFlag), source.SourceLastUpdatedDateTime, TRIM(source.GroupAssignment), TRIM(source.ServiceDateConsolidationDateFlag), source.NonBillable, TRIM(source.AccountID), TRIM(source.VisitType), source.DischargeDate, TRIM(source.CareProviderFirstName), TRIM(source.CareProviderLastName), TRIM(source.CareProviderMiddleName), TRIM(source.RecordStatus), TRIM(source.DocHoldReason), source.Encounter3MKey, source.LoadDate, source.Worked, source.WorkedDate, source.EncounterLastModifiedDate, source.DaysSinceModifiedDate, source.DaysSinceServiceDate, source.BusincessDaysSinceServiceDate, source.BusincessDaysSinceLastModified, TRIM(source.ProcedureCategory1), TRIM(source.ProcedureCategory2), TRIM(source.CPTCodes), source.CCUNonCustomerConfigKey, source.CCUNonCustomerNPIConfigKey, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.CPTModifiers), TRIM(source.ICDCodes), source.LoadKey, TRIM(source.FacilityID));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCU3MInventoryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_Fact3MInventory
      GROUP BY CCU3MInventoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_Fact3MInventory');
ELSE
  COMMIT TRANSACTION;
END IF;
