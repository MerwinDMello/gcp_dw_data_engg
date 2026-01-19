
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactPKInventory AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactPKInventory AS source
ON target.CCUPKInventoryKey = source.CCUPKInventoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUPKInventoryKey = source.CCUPKInventoryKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.PKRegionName = TRIM(source.PKRegionName),
 target.AccountID = TRIM(source.AccountID),
 target.ServiceDate = source.ServiceDate,
 target.BillingProviderFirstName = TRIM(source.BillingProviderFirstName),
 target.BillingProviderLastName = TRIM(source.BillingProviderLastName),
 target.BillingProviderUserName = TRIM(source.BillingProviderUserName),
 target.BillingProviderUserNumber = TRIM(source.BillingProviderUserNumber),
 target.BillingProviderNPI = TRIM(source.BillingProviderNPI),
 target.BillingProviderDeleteFlag = TRIM(source.BillingProviderDeleteFlag),
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.BillingAreaNumber = TRIM(source.BillingAreaNumber),
 target.BillingDepartmentID = TRIM(source.BillingDepartmentID),
 target.COID = TRIM(source.COID),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.VisitID = TRIM(source.VisitID),
 target.VisitLocation = TRIM(source.VisitLocation),
 target.VisitType = TRIM(source.VisitType),
 target.ReasonForVisit = TRIM(source.ReasonForVisit),
 target.ModifiedDate = source.ModifiedDate,
 target.CCUGoLiveStatus = TRIM(source.CCUGoLiveStatus),
 target.CCUGoLiveDate = source.CCUGoLiveDate,
 target.AssignedToVendor = TRIM(source.AssignedToVendor),
 target.VendorName = TRIM(source.VendorName),
 target.VendorAssignedDate = source.VendorAssignedDate,
 target.OWNER = TRIM(source.OWNER),
 target.SourceSystem = TRIM(source.SourceSystem),
 target.InventoryType = TRIM(source.InventoryType),
 target.GroupAssignment = TRIM(source.GroupAssignment),
 target.LoadDate = source.LoadDate,
 target.DaysSinceModifiedDate = source.DaysSinceModifiedDate,
 target.DaysSinceServiceDate = source.DaysSinceServiceDate,
 target.Worked = source.Worked,
 target.WorkedDate = source.WorkedDate,
 target.RegionKey = source.RegionKey,
 target.CCUNonCustomerConfigKey = source.CCUNonCustomerConfigKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.EditFlag = TRIM(source.EditFlag),
 target.CCUNonCustomerNPIConfigKey = source.CCUNonCustomerNPIConfigKey,
 target.ProcedureCategory1 = TRIM(source.ProcedureCategory1),
 target.ProcedureCategory2 = TRIM(source.ProcedureCategory2),
 target.CPTCodes = TRIM(source.CPTCodes),
 target.EncounterId = source.EncounterId,
 target.VisitTypeKey = source.VisitTypeKey,
 target.BillingAreaName = TRIM(source.BillingAreaName),
 target.BusincessDaysSinceServiceDate = source.BusincessDaysSinceServiceDate,
 target.BusincessDaysSinceLastModified = source.BusincessDaysSinceLastModified,
 target.DischargeDate = source.DischargeDate,
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.HoldingBinCategory = TRIM(source.HoldingBinCategory),
 target.CPTModifiers = TRIM(source.CPTModifiers),
 target.ICDCodes = TRIM(source.ICDCodes),
 target.VisitStatus = TRIM(source.VisitStatus)
WHEN NOT MATCHED THEN
  INSERT (CCUPKInventoryKey, SourceAPrimaryKeyValue, PKRegionName, AccountID, ServiceDate, BillingProviderFirstName, BillingProviderLastName, BillingProviderUserName, BillingProviderUserNumber, BillingProviderNPI, BillingProviderDeleteFlag, PKFinancialNumber, BillingAreaNumber, BillingDepartmentID, COID, PatientMRN, VisitID, VisitLocation, VisitType, ReasonForVisit, ModifiedDate, CCUGoLiveStatus, CCUGoLiveDate, AssignedToVendor, VendorName, VendorAssignedDate, OWNER, SourceSystem, InventoryType, GroupAssignment, LoadDate, DaysSinceModifiedDate, DaysSinceServiceDate, Worked, WorkedDate, RegionKey, CCUNonCustomerConfigKey, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, EditFlag, CCUNonCustomerNPIConfigKey, ProcedureCategory1, ProcedureCategory2, CPTCodes, EncounterId, VisitTypeKey, BillingAreaName, BusincessDaysSinceServiceDate, BusincessDaysSinceLastModified, DischargeDate, PatientFirstName, PatientLastName, PatientMiddleName, HoldingBinCategory, CPTModifiers, ICDCodes, VisitStatus)
  VALUES (source.CCUPKInventoryKey, TRIM(source.SourceAPrimaryKeyValue), TRIM(source.PKRegionName), TRIM(source.AccountID), source.ServiceDate, TRIM(source.BillingProviderFirstName), TRIM(source.BillingProviderLastName), TRIM(source.BillingProviderUserName), TRIM(source.BillingProviderUserNumber), TRIM(source.BillingProviderNPI), TRIM(source.BillingProviderDeleteFlag), TRIM(source.PKFinancialNumber), TRIM(source.BillingAreaNumber), TRIM(source.BillingDepartmentID), TRIM(source.COID), TRIM(source.PatientMRN), TRIM(source.VisitID), TRIM(source.VisitLocation), TRIM(source.VisitType), TRIM(source.ReasonForVisit), source.ModifiedDate, TRIM(source.CCUGoLiveStatus), source.CCUGoLiveDate, TRIM(source.AssignedToVendor), TRIM(source.VendorName), source.VendorAssignedDate, TRIM(source.OWNER), TRIM(source.SourceSystem), TRIM(source.InventoryType), TRIM(source.GroupAssignment), source.LoadDate, source.DaysSinceModifiedDate, source.DaysSinceServiceDate, source.Worked, source.WorkedDate, source.RegionKey, source.CCUNonCustomerConfigKey, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.EditFlag), source.CCUNonCustomerNPIConfigKey, TRIM(source.ProcedureCategory1), TRIM(source.ProcedureCategory2), TRIM(source.CPTCodes), source.EncounterId, source.VisitTypeKey, TRIM(source.BillingAreaName), source.BusincessDaysSinceServiceDate, source.BusincessDaysSinceLastModified, source.DischargeDate, TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PatientMiddleName), TRIM(source.HoldingBinCategory), TRIM(source.CPTModifiers), TRIM(source.ICDCodes), TRIM(source.VisitStatus));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUPKInventoryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactPKInventory
      GROUP BY CCUPKInventoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactPKInventory');
ELSE
  COMMIT TRANSACTION;
END IF;
