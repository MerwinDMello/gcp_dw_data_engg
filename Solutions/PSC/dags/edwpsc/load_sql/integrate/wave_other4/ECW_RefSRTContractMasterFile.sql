
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTContractMasterFile AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTContractMasterFile AS source
ON target.ContractMasterFileKey = source.ContractMasterFileKey
WHEN MATCHED THEN
  UPDATE SET
  target.ContractMasterFileKey = source.ContractMasterFileKey,
 target.SOWStatus = TRIM(source.SOWStatus),
 target.Partner = TRIM(source.Partner),
 target.InvoiceControlNumber = source.InvoiceControlNumber,
 target.ItemNumber = source.ItemNumber,
 target.CLSource = TRIM(source.CLSource),
 target.ActiveEntry = source.ActiveEntry,
 target.ContractCOID = source.ContractCOID,
 target.LineOfBusiness = TRIM(source.LineOfBusiness),
 target.DeptartmentNumber = source.DeptartmentNumber,
 target.ItemNumberDescription = TRIM(source.ItemNumberDescription),
 target.OriginalItemNumber = source.OriginalItemNumber,
 target.OWNER = TRIM(source.OWNER),
 target.ValescoIndicator = source.ValescoIndicator,
 target.AccrualProductivity = source.AccrualProductivity,
 target.ContractCOIDDescription = TRIM(source.ContractCOIDDescription),
 target.SummaryOfService = TRIM(source.SummaryOfService),
 target.ContractLaborProductivityActual = source.ContractLaborProductivityActual,
 target.Workflow = TRIM(source.Workflow),
 target.SYSTEM = TRIM(source.SYSTEM),
 target.DeptDescription = TRIM(source.DeptDescription),
 target.SOWNumber = source.SOWNumber,
 target.ServiceModificationSchedule = source.ServiceModificationSchedule,
 target.ProjectedStartDate = source.ProjectedStartDate,
 target.SignedContractDate = source.SignedContractDate,
 target.ContractRenewal = source.ContractRenewal,
 target.TermedDate = source.TermedDate,
 target.InvoiceName = TRIM(source.InvoiceName),
 target.BaseFTERate = source.BaseFTERate,
 target.Rate = source.Rate,
 target.ContractType = TRIM(source.ContractType),
 target.ContractedProductivityPerHour = source.ContractedProductivityPerHour,
 target.Hours = TRIM(source.Hours),
 target.LOCATION = TRIM(source.LOCATION),
 target.QualityPenalty93to9499 = source.QualityPenalty93to9499,
 target.QualityPenalty90to9299 = source.QualityPenalty90to9299,
 target.QualityPenaltyLT90 = source.QualityPenaltyLT90,
 target.QualityPenalty85to8999 = source.QualityPenalty85to8999,
 target.QualityPenalty80to8499 = source.QualityPenalty80to8499,
 target.QualityPenaltyLT7999 = source.QualityPenaltyLT7999,
 target.ForecastSubmissions = TRIM(source.ForecastSubmissions),
 target.Notifications = TRIM(source.Notifications),
 target.SLAPenaltySummary = TRIM(source.SLAPenaltySummary),
 target.LegacyInvoiceControlNumber = source.LegacyInvoiceControlNumber,
 target.ModifiedByCustom = TRIM(source.ModifiedByCustom),
 target.ModifiedCustom = source.ModifiedCustom,
 target.ModifiedReference = source.ModifiedReference,
 target.VersionNumber = source.VersionNumber,
 target.SystemInteger = source.SystemInteger,
 target.duplicateRecord2 = TRIM(source.duplicateRecord2),
 target.isEditable = TRIM(source.isEditable),
 target.SPworkflowStatus = TRIM(source.SPworkflowStatus),
 target.AssignedTo = TRIM(source.AssignedTo),
 target.Duplicated = TRIM(source.Duplicated),
 target.ID = source.ID,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ContractMasterFileKey, SOWStatus, Partner, InvoiceControlNumber, ItemNumber, CLSource, ActiveEntry, ContractCOID, LineOfBusiness, DeptartmentNumber, ItemNumberDescription, OriginalItemNumber, OWNER, ValescoIndicator, AccrualProductivity, ContractCOIDDescription, SummaryOfService, ContractLaborProductivityActual, Workflow, SYSTEM, DeptDescription, SOWNumber, ServiceModificationSchedule, ProjectedStartDate, SignedContractDate, ContractRenewal, TermedDate, InvoiceName, BaseFTERate, Rate, ContractType, ContractedProductivityPerHour, Hours, LOCATION, QualityPenalty93to9499, QualityPenalty90to9299, QualityPenaltyLT90, QualityPenalty85to8999, QualityPenalty80to8499, QualityPenaltyLT7999, ForecastSubmissions, Notifications, SLAPenaltySummary, LegacyInvoiceControlNumber, ModifiedByCustom, ModifiedCustom, ModifiedReference, VersionNumber, SystemInteger, duplicateRecord2, isEditable, SPworkflowStatus, AssignedTo, Duplicated, ID, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ContractMasterFileKey, TRIM(source.SOWStatus), TRIM(source.Partner), source.InvoiceControlNumber, source.ItemNumber, TRIM(source.CLSource), source.ActiveEntry, source.ContractCOID, TRIM(source.LineOfBusiness), source.DeptartmentNumber, TRIM(source.ItemNumberDescription), source.OriginalItemNumber, TRIM(source.OWNER), source.ValescoIndicator, source.AccrualProductivity, TRIM(source.ContractCOIDDescription), TRIM(source.SummaryOfService), source.ContractLaborProductivityActual, TRIM(source.Workflow), TRIM(source.SYSTEM), TRIM(source.DeptDescription), source.SOWNumber, source.ServiceModificationSchedule, source.ProjectedStartDate, source.SignedContractDate, source.ContractRenewal, source.TermedDate, TRIM(source.InvoiceName), source.BaseFTERate, source.Rate, TRIM(source.ContractType), source.ContractedProductivityPerHour, TRIM(source.Hours), TRIM(source.LOCATION), source.QualityPenalty93to9499, source.QualityPenalty90to9299, source.QualityPenaltyLT90, source.QualityPenalty85to8999, source.QualityPenalty80to8499, source.QualityPenaltyLT7999, TRIM(source.ForecastSubmissions), TRIM(source.Notifications), TRIM(source.SLAPenaltySummary), source.LegacyInvoiceControlNumber, TRIM(source.ModifiedByCustom), source.ModifiedCustom, source.ModifiedReference, source.VersionNumber, source.SystemInteger, TRIM(source.duplicateRecord2), TRIM(source.isEditable), TRIM(source.SPworkflowStatus), TRIM(source.AssignedTo), TRIM(source.Duplicated), source.ID, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ContractMasterFileKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTContractMasterFile
      GROUP BY ContractMasterFileKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTContractMasterFile');
ELSE
  COMMIT TRANSACTION;
END IF;
