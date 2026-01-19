
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactECWInventory AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactECWInventory AS source
ON target.CCUeCWInventoryKey = source.CCUeCWInventoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUeCWInventoryKey = source.CCUeCWInventoryKey,
 target.RegionKey = source.RegionKey,
 target.FacilityKey = source.FacilityKey,
 target.PracticeKey = source.PracticeKey,
 target.DeptCode = TRIM(source.DeptCode),
 target.COID = TRIM(source.COID),
 target.CCUGoLiveStatus = TRIM(source.CCUGoLiveStatus),
 target.CCUGoLiveDate = source.CCUGoLiveDate,
 target.AssignedToVendor = TRIM(source.AssignedToVendor),
 target.VendorName = TRIM(source.VendorName),
 target.VendorAssignedDate = source.VendorAssignedDate,
 target.EncounterID = source.EncounterID,
 target.ServiceDate = source.ServiceDate,
 target.PatientKey = source.PatientKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderSpecialty = TRIM(source.ServicingProviderSpecialty),
 target.ServicingProviderSpecialtyCategory = TRIM(source.ServicingProviderSpecialtyCategory),
 target.VisitTypeKey = source.VisitTypeKey,
 target.VisitStatusKey = source.VisitStatusKey,
 target.EncounterLockedDate = source.EncounterLockedDate,
 target.DaysSinceLockedDate = source.DaysSinceLockedDate,
 target.ClaimCreatedByUserKey = source.ClaimCreatedByUserKey,
 target.ClaimStatusKey = source.ClaimStatusKey,
 target.LiabilityIplanKey = source.LiabilityIplanKey,
 target.ClaimNumber = TRIM(source.ClaimNumber),
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.OWNER = TRIM(source.OWNER),
 target.EncounterLockFlag = TRIM(source.EncounterLockFlag),
 target.InventoryType = TRIM(source.InventoryType),
 target.ClaimVsEncounterFlag = TRIM(source.ClaimVsEncounterFlag),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.GroupAssignment = TRIM(source.GroupAssignment),
 target.ClaimVoidFlag = source.ClaimVoidFlag,
 target.ClaimDeleteFlag = source.ClaimDeleteFlag,
 target.EncounterType = source.EncounterType,
 target.ClaimRequired = source.ClaimRequired,
 target.NonBillable = source.NonBillable,
 target.ServiceDateConsolidationDateFlag = TRIM(source.ServiceDateConsolidationDateFlag),
 target.EncounterKey = source.EncounterKey,
 target.ClaimKey = source.ClaimKey,
 target.LoadDate = source.LoadDate,
 target.DaysSinceServiceDate = source.DaysSinceServiceDate,
 target.LastClaimStatusUpdatedDateKey = source.LastClaimStatusUpdatedDateKey,
 target.DaysSinceLastStatusChange = source.DaysSinceLastStatusChange,
 target.EncounterLastModifiedDate = source.EncounterLastModifiedDate,
 target.DaysSinceEncounterLastModified = source.DaysSinceEncounterLastModified,
 target.Worked = source.Worked,
 target.WorkedDate = source.WorkedDate,
 target.CCUNonCustomerConfigKey = source.CCUNonCustomerConfigKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.SourceLastUpdatedDateTime = source.SourceLastUpdatedDateTime,
 target.CCUNonCustomerNPIConfigKey = source.CCUNonCustomerNPIConfigKey,
 target.ProcedureCategory1 = TRIM(source.ProcedureCategory1),
 target.ProcedureCategory2 = TRIM(source.ProcedureCategory2),
 target.CPTCodes = TRIM(source.CPTCodes),
 target.DateCorrectedFlag = source.DateCorrectedFlag,
 target.BusincessDaysSinceServiceDate = source.BusincessDaysSinceServiceDate,
 target.BusincessDaysSinceLastModified = source.BusincessDaysSinceLastModified,
 target.BusincessDaysSinceLastStatusChange = source.BusincessDaysSinceLastStatusChange,
 target.BusinessDaysSinceLockedDate = source.BusinessDaysSinceLockedDate,
 target.CPTModifiers = TRIM(source.CPTModifiers),
 target.ICDCodes = TRIM(source.ICDCodes)
WHEN NOT MATCHED THEN
  INSERT (CCUeCWInventoryKey, RegionKey, FacilityKey, PracticeKey, DeptCode, COID, CCUGoLiveStatus, CCUGoLiveDate, AssignedToVendor, VendorName, VendorAssignedDate, EncounterID, ServiceDate, PatientKey, ServicingProviderKey, ServicingProviderSpecialty, ServicingProviderSpecialtyCategory, VisitTypeKey, VisitStatusKey, EncounterLockedDate, DaysSinceLockedDate, ClaimCreatedByUserKey, ClaimStatusKey, LiabilityIplanKey, ClaimNumber, TotalBalanceAmt, SourceSystem, OWNER, EncounterLockFlag, InventoryType, ClaimVsEncounterFlag, DWLastUpdateDateTime, GroupAssignment, ClaimVoidFlag, ClaimDeleteFlag, EncounterType, ClaimRequired, NonBillable, ServiceDateConsolidationDateFlag, EncounterKey, ClaimKey, LoadDate, DaysSinceServiceDate, LastClaimStatusUpdatedDateKey, DaysSinceLastStatusChange, EncounterLastModifiedDate, DaysSinceEncounterLastModified, Worked, WorkedDate, CCUNonCustomerConfigKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SourceLastUpdatedDateTime, CCUNonCustomerNPIConfigKey, ProcedureCategory1, ProcedureCategory2, CPTCodes, DateCorrectedFlag, BusincessDaysSinceServiceDate, BusincessDaysSinceLastModified, BusincessDaysSinceLastStatusChange, BusinessDaysSinceLockedDate, CPTModifiers, ICDCodes)
  VALUES (source.CCUeCWInventoryKey, source.RegionKey, source.FacilityKey, source.PracticeKey, TRIM(source.DeptCode), TRIM(source.COID), TRIM(source.CCUGoLiveStatus), source.CCUGoLiveDate, TRIM(source.AssignedToVendor), TRIM(source.VendorName), source.VendorAssignedDate, source.EncounterID, source.ServiceDate, source.PatientKey, source.ServicingProviderKey, TRIM(source.ServicingProviderSpecialty), TRIM(source.ServicingProviderSpecialtyCategory), source.VisitTypeKey, source.VisitStatusKey, source.EncounterLockedDate, source.DaysSinceLockedDate, source.ClaimCreatedByUserKey, source.ClaimStatusKey, source.LiabilityIplanKey, TRIM(source.ClaimNumber), source.TotalBalanceAmt, TRIM(source.SourceSystem), TRIM(source.OWNER), TRIM(source.EncounterLockFlag), TRIM(source.InventoryType), TRIM(source.ClaimVsEncounterFlag), source.DWLastUpdateDateTime, TRIM(source.GroupAssignment), source.ClaimVoidFlag, source.ClaimDeleteFlag, source.EncounterType, source.ClaimRequired, source.NonBillable, TRIM(source.ServiceDateConsolidationDateFlag), source.EncounterKey, source.ClaimKey, source.LoadDate, source.DaysSinceServiceDate, source.LastClaimStatusUpdatedDateKey, source.DaysSinceLastStatusChange, source.EncounterLastModifiedDate, source.DaysSinceEncounterLastModified, source.Worked, source.WorkedDate, source.CCUNonCustomerConfigKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.SourceLastUpdatedDateTime, source.CCUNonCustomerNPIConfigKey, TRIM(source.ProcedureCategory1), TRIM(source.ProcedureCategory2), TRIM(source.CPTCodes), source.DateCorrectedFlag, source.BusincessDaysSinceServiceDate, source.BusincessDaysSinceLastModified, source.BusincessDaysSinceLastStatusChange, source.BusinessDaysSinceLockedDate, TRIM(source.CPTModifiers), TRIM(source.ICDCodes));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUeCWInventoryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactECWInventory
      GROUP BY CCUeCWInventoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactECWInventory');
ELSE
  COMMIT TRANSACTION;
END IF;
