
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactMedaxionInventory AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactMedaxionInventory AS source
ON target.CCUMedaxionInventoryKey = source.CCUMedaxionInventoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUMedaxionInventoryKey = source.CCUMedaxionInventoryKey,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.OWNER = TRIM(source.OWNER),
 target.RegionKey = source.RegionKey,
 target.COID = TRIM(source.COID),
 target.CCUGoLiveStatus = TRIM(source.CCUGoLiveStatus),
 target.CCUGoLiveDate = source.CCUGoLiveDate,
 target.AssignedToVendor = TRIM(source.AssignedToVendor),
 target.VendorName = TRIM(source.VendorName),
 target.VendorAssignedDate = source.VendorAssignedDate,
 target.PracticeKey = source.PracticeKey,
 target.FacilityKey = source.FacilityKey,
 target.EncounterID = TRIM(source.EncounterID),
 target.ServiceDate = source.ServiceDate,
 target.PatientKey = source.PatientKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderSpecialty = TRIM(source.ServicingProviderSpecialty),
 target.ServicingProviderSpecialtyCategory = TRIM(source.ServicingProviderSpecialtyCategory),
 target.VisitStatusDescription = TRIM(source.VisitStatusDescription),
 target.EncounterLockedDate = source.EncounterLockedDate,
 target.InventoryType = TRIM(source.InventoryType),
 target.ClaimVsEncounterFlag = TRIM(source.ClaimVsEncounterFlag),
 target.SourceLastUpdatedDateTime = source.SourceLastUpdatedDateTime,
 target.GroupAssignment = TRIM(source.GroupAssignment),
 target.ServiceDateConsolidationDateFlag = TRIM(source.ServiceDateConsolidationDateFlag),
 target.POS = TRIM(source.POS),
 target.EncounterMedaxionKey = source.EncounterMedaxionKey,
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
 target.ICDCodes = TRIM(source.ICDCodes)
WHEN NOT MATCHED THEN
  INSERT (CCUMedaxionInventoryKey, SourceSystem, OWNER, RegionKey, COID, CCUGoLiveStatus, CCUGoLiveDate, AssignedToVendor, VendorName, VendorAssignedDate, PracticeKey, FacilityKey, EncounterID, ServiceDate, PatientKey, ServicingProviderKey, ServicingProviderSpecialty, ServicingProviderSpecialtyCategory, VisitStatusDescription, EncounterLockedDate, InventoryType, ClaimVsEncounterFlag, SourceLastUpdatedDateTime, GroupAssignment, ServiceDateConsolidationDateFlag, POS, EncounterMedaxionKey, LoadDate, Worked, WorkedDate, EncounterLastModifiedDate, DaysSinceModifiedDate, DaysSinceServiceDate, BusincessDaysSinceServiceDate, BusincessDaysSinceLastModified, ProcedureCategory1, ProcedureCategory2, CPTCodes, CCUNonCustomerConfigKey, CCUNonCustomerNPIConfigKey, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, CPTModifiers, ICDCodes)
  VALUES (source.CCUMedaxionInventoryKey, TRIM(source.SourceSystem), TRIM(source.OWNER), source.RegionKey, TRIM(source.COID), TRIM(source.CCUGoLiveStatus), source.CCUGoLiveDate, TRIM(source.AssignedToVendor), TRIM(source.VendorName), source.VendorAssignedDate, source.PracticeKey, source.FacilityKey, TRIM(source.EncounterID), source.ServiceDate, source.PatientKey, source.ServicingProviderKey, TRIM(source.ServicingProviderSpecialty), TRIM(source.ServicingProviderSpecialtyCategory), TRIM(source.VisitStatusDescription), source.EncounterLockedDate, TRIM(source.InventoryType), TRIM(source.ClaimVsEncounterFlag), source.SourceLastUpdatedDateTime, TRIM(source.GroupAssignment), TRIM(source.ServiceDateConsolidationDateFlag), TRIM(source.POS), source.EncounterMedaxionKey, source.LoadDate, source.Worked, source.WorkedDate, source.EncounterLastModifiedDate, source.DaysSinceModifiedDate, source.DaysSinceServiceDate, source.BusincessDaysSinceServiceDate, source.BusincessDaysSinceLastModified, TRIM(source.ProcedureCategory1), TRIM(source.ProcedureCategory2), TRIM(source.CPTCodes), source.CCUNonCustomerConfigKey, source.CCUNonCustomerNPIConfigKey, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.CPTModifiers), TRIM(source.ICDCodes));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUMedaxionInventoryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactMedaxionInventory
      GROUP BY CCUMedaxionInventoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactMedaxionInventory');
ELSE
  COMMIT TRANSACTION;
END IF;
