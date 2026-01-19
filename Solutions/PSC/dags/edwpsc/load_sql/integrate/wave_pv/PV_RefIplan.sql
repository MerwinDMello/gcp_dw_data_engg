
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefIplan AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefIplan AS source
ON target.IplanKey = source.IplanKey
WHEN MATCHED THEN
  UPDATE SET
  target.IplanKey = source.IplanKey,
 target.FinancialClassKey = source.FinancialClassKey,
 target.IplanGroupCactusKey = source.IplanGroupCactusKey,
 target.IplanGroupFinancialKey = source.IplanGroupFinancialKey,
 target.IplanName = TRIM(source.IplanName),
 target.IplanPhone = TRIM(source.IplanPhone),
 target.IplanPhone2 = TRIM(source.IplanPhone2),
 target.IplanFax = TRIM(source.IplanFax),
 target.IplanEmail = TRIM(source.IplanEmail),
 target.IplanPrimaryAddressLine1 = TRIM(source.IplanPrimaryAddressLine1),
 target.IplanPrimaryAddressLine2 = TRIM(source.IplanPrimaryAddressLine2),
 target.IplanPrimaryGeographyKey = source.IplanPrimaryGeographyKey,
 target.IplanPayorID = TRIM(source.IplanPayorID),
 target.IplanERAPayorID = TRIM(source.IplanERAPayorID),
 target.IplanFeeSchedId = source.IplanFeeSchedId,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.IplanDeleteFlag = source.IplanDeleteFlag,
 target.IplanInactive = source.IplanInactive,
 target.IplanNotes = TRIM(source.IplanNotes),
 target.IplanPractice = TRIM(source.IplanPractice),
 target.RegionKey = source.RegionKey,
 target.IplanType = TRIM(source.IplanType),
 target.IplanWCPricing = TRIM(source.IplanWCPricing),
 target.IplanOccMedPricing = TRIM(source.IplanOccMedPricing),
 target.PVFinancialClass = TRIM(source.PVFinancialClass),
 target.PVFinancialClassDesc = TRIM(source.PVFinancialClassDesc),
 target.PVFinancialClassGrouped = TRIM(source.PVFinancialClassGrouped),
 target.CompanySalesRepName = TRIM(source.CompanySalesRepName),
 target.EMCPayerID = TRIM(source.EMCPayerID),
 target.PaperPayerID = TRIM(source.PaperPayerID),
 target.CompanyNumber = source.CompanyNumber,
 target.CompanyName = TRIM(source.CompanyName),
 target.BillingNotes = TRIM(source.BillingNotes),
 target.EpsBillToName = TRIM(source.EpsBillToName),
 target.WCBillToType = TRIM(source.WCBillToType),
 target.WCBillToName = TRIM(source.WCBillToName),
 target.WCNotes = TRIM(source.WCNotes),
 target.WCInstructions = TRIM(source.WCInstructions),
 target.IplanBillType = TRIM(source.IplanBillType),
 target.NumberOfEmployeesRange = TRIM(source.NumberOfEmployeesRange),
 target.AccountRepUserKey = source.AccountRepUserKey,
 target.SubscriberFlag = source.SubscriberFlag
WHEN NOT MATCHED THEN
  INSERT (IplanKey, FinancialClassKey, IplanGroupCactusKey, IplanGroupFinancialKey, IplanName, IplanPhone, IplanPhone2, IplanFax, IplanEmail, IplanPrimaryAddressLine1, IplanPrimaryAddressLine2, IplanPrimaryGeographyKey, IplanPayorID, IplanERAPayorID, IplanFeeSchedId, SourcePrimaryKeyValue, SourceARecordLastUpdated, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, IplanDeleteFlag, IplanInactive, IplanNotes, IplanPractice, RegionKey, IplanType, IplanWCPricing, IplanOccMedPricing, PVFinancialClass, PVFinancialClassDesc, PVFinancialClassGrouped, CompanySalesRepName, EMCPayerID, PaperPayerID, CompanyNumber, CompanyName, BillingNotes, EpsBillToName, WCBillToType, WCBillToName, WCNotes, WCInstructions, IplanBillType, NumberOfEmployeesRange, AccountRepUserKey, SubscriberFlag)
  VALUES (source.IplanKey, source.FinancialClassKey, source.IplanGroupCactusKey, source.IplanGroupFinancialKey, TRIM(source.IplanName), TRIM(source.IplanPhone), TRIM(source.IplanPhone2), TRIM(source.IplanFax), TRIM(source.IplanEmail), TRIM(source.IplanPrimaryAddressLine1), TRIM(source.IplanPrimaryAddressLine2), source.IplanPrimaryGeographyKey, TRIM(source.IplanPayorID), TRIM(source.IplanERAPayorID), source.IplanFeeSchedId, TRIM(source.SourcePrimaryKeyValue), source.SourceARecordLastUpdated, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.IplanDeleteFlag, source.IplanInactive, TRIM(source.IplanNotes), TRIM(source.IplanPractice), source.RegionKey, TRIM(source.IplanType), TRIM(source.IplanWCPricing), TRIM(source.IplanOccMedPricing), TRIM(source.PVFinancialClass), TRIM(source.PVFinancialClassDesc), TRIM(source.PVFinancialClassGrouped), TRIM(source.CompanySalesRepName), TRIM(source.EMCPayerID), TRIM(source.PaperPayerID), source.CompanyNumber, TRIM(source.CompanyName), TRIM(source.BillingNotes), TRIM(source.EpsBillToName), TRIM(source.WCBillToType), TRIM(source.WCBillToName), TRIM(source.WCNotes), TRIM(source.WCInstructions), TRIM(source.IplanBillType), TRIM(source.NumberOfEmployeesRange), source.AccountRepUserKey, source.SubscriberFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IplanKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefIplan
      GROUP BY IplanKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefIplan');
ELSE
  COMMIT TRANSACTION;
END IF;
