
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefFacility_History AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefFacility_History AS source
ON target.FacilityKey = source.FacilityKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.FacilityKey = source.FacilityKey,
 target.FacilityPrimaryGeographyKey = source.FacilityPrimaryGeographyKey,
 target.PracticeKey = source.PracticeKey,
 target.FacilityBillingGeographyKey = source.FacilityBillingGeographyKey,
 target.FacilityName = TRIM(source.FacilityName),
 target.FacilityType = TRIM(source.FacilityType),
 target.FacilityPrimaryAddressLine1 = TRIM(source.FacilityPrimaryAddressLine1),
 target.FacilityPrimaryAddressLine2 = TRIM(source.FacilityPrimaryAddressLine2),
 target.FacilityTelephone = TRIM(source.FacilityTelephone),
 target.FacilityFax = TRIM(source.FacilityFax),
 target.FacilityEmail = TRIM(source.FacilityEmail),
 target.FacilityBillingAddressLine1 = TRIM(source.FacilityBillingAddressLine1),
 target.FacilityBillingAddressLine2 = TRIM(source.FacilityBillingAddressLine2),
 target.FacilityBillingTelephone = TRIM(source.FacilityBillingTelephone),
 target.FacilityBillingFax = TRIM(source.FacilityBillingFax),
 target.FacilityBillingEmail = TRIM(source.FacilityBillingEmail),
 target.FacilityPracticeType = TRIM(source.FacilityPracticeType),
 target.FacilityPracticeOption = TRIM(source.FacilityPracticeOption),
 target.FacilityPayableTo = TRIM(source.FacilityPayableTo),
 target.FacilityBankAccount = TRIM(source.FacilityBankAccount),
 target.FacilityFederalTaxID = TRIM(source.FacilityFederalTaxID),
 target.FacilityNotes = TRIM(source.FacilityNotes),
 target.FacilityPrimaryFacility = source.FacilityPrimaryFacility,
 target.FacilityCliaID = TRIM(source.FacilityCliaID),
 target.FacilityCode = TRIM(source.FacilityCode),
 target.POSKey = source.POSKey,
 target.FacilityMammoCertId = TRIM(source.FacilityMammoCertId),
 target.FacilityHPSAFlag = source.FacilityHPSAFlag,
 target.FacilityHPSAModifier = TRIM(source.FacilityHPSAModifier),
 target.Facilityvmid = TRIM(source.Facilityvmid),
 target.Facilityhl7id = TRIM(source.Facilityhl7id),
 target.FacilityColor = source.FacilityColor,
 target.FacilityFeeSchedule = source.FacilityFeeSchedule,
 target.FacilityIdType = source.FacilityIdType,
 target.FacilityPtStmtFacOption = source.FacilityPtStmtFacOption,
 target.FacilityPtStmtFacId = source.FacilityPtStmtFacId,
 target.FacilityRestrictPrintAppts = source.FacilityRestrictPrintAppts,
 target.FacilityNPI = TRIM(source.FacilityNPI),
 target.FacilityImmStateId = TRIM(source.FacilityImmStateId),
 target.FacilityTaxonomyCode = TRIM(source.FacilityTaxonomyCode),
 target.FacilityRestrictPrintStmts = source.FacilityRestrictPrintStmts,
 target.FacilityStartedOn = source.FacilityStartedOn,
 target.FacilityMerchantID = TRIM(source.FacilityMerchantID),
 target.FacilityRevenueCode = TRIM(source.FacilityRevenueCode),
 target.FacilityPercent = source.FacilityPercent,
 target.FacilityOverrideSalesTax = source.FacilityOverrideSalesTax,
 target.FacilityImmLocationId = TRIM(source.FacilityImmLocationId),
 target.FacilityTimeZoneCode = TRIM(source.FacilityTimeZoneCode),
 target.FacilityMessCallerId = TRIM(source.FacilityMessCallerId),
 target.FacilityMessOperNum = TRIM(source.FacilityMessOperNum),
 target.FacilityCreditCardOption = TRIM(source.FacilityCreditCardOption),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (FacilityKey, FacilityPrimaryGeographyKey, PracticeKey, FacilityBillingGeographyKey, FacilityName, FacilityType, FacilityPrimaryAddressLine1, FacilityPrimaryAddressLine2, FacilityTelephone, FacilityFax, FacilityEmail, FacilityBillingAddressLine1, FacilityBillingAddressLine2, FacilityBillingTelephone, FacilityBillingFax, FacilityBillingEmail, FacilityPracticeType, FacilityPracticeOption, FacilityPayableTo, FacilityBankAccount, FacilityFederalTaxID, FacilityNotes, FacilityPrimaryFacility, FacilityCliaID, FacilityCode, POSKey, FacilityMammoCertId, FacilityHPSAFlag, FacilityHPSAModifier, Facilityvmid, Facilityhl7id, FacilityColor, FacilityFeeSchedule, FacilityIdType, FacilityPtStmtFacOption, FacilityPtStmtFacId, FacilityRestrictPrintAppts, FacilityNPI, FacilityImmStateId, FacilityTaxonomyCode, FacilityRestrictPrintStmts, FacilityStartedOn, FacilityMerchantID, FacilityRevenueCode, FacilityPercent, FacilityOverrideSalesTax, FacilityImmLocationId, FacilityTimeZoneCode, FacilityMessCallerId, FacilityMessOperNum, FacilityCreditCardOption, SourcePrimaryKeyValue, SourceARecordLastUpdated, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey, SysStartTime, SysEndTime)
  VALUES (source.FacilityKey, source.FacilityPrimaryGeographyKey, source.PracticeKey, source.FacilityBillingGeographyKey, TRIM(source.FacilityName), TRIM(source.FacilityType), TRIM(source.FacilityPrimaryAddressLine1), TRIM(source.FacilityPrimaryAddressLine2), TRIM(source.FacilityTelephone), TRIM(source.FacilityFax), TRIM(source.FacilityEmail), TRIM(source.FacilityBillingAddressLine1), TRIM(source.FacilityBillingAddressLine2), TRIM(source.FacilityBillingTelephone), TRIM(source.FacilityBillingFax), TRIM(source.FacilityBillingEmail), TRIM(source.FacilityPracticeType), TRIM(source.FacilityPracticeOption), TRIM(source.FacilityPayableTo), TRIM(source.FacilityBankAccount), TRIM(source.FacilityFederalTaxID), TRIM(source.FacilityNotes), source.FacilityPrimaryFacility, TRIM(source.FacilityCliaID), TRIM(source.FacilityCode), source.POSKey, TRIM(source.FacilityMammoCertId), source.FacilityHPSAFlag, TRIM(source.FacilityHPSAModifier), TRIM(source.Facilityvmid), TRIM(source.Facilityhl7id), source.FacilityColor, source.FacilityFeeSchedule, source.FacilityIdType, source.FacilityPtStmtFacOption, source.FacilityPtStmtFacId, source.FacilityRestrictPrintAppts, TRIM(source.FacilityNPI), TRIM(source.FacilityImmStateId), TRIM(source.FacilityTaxonomyCode), source.FacilityRestrictPrintStmts, source.FacilityStartedOn, TRIM(source.FacilityMerchantID), TRIM(source.FacilityRevenueCode), source.FacilityPercent, source.FacilityOverrideSalesTax, TRIM(source.FacilityImmLocationId), TRIM(source.FacilityTimeZoneCode), TRIM(source.FacilityMessCallerId), TRIM(source.FacilityMessOperNum), TRIM(source.FacilityCreditCardOption), source.SourcePrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT FacilityKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefFacility_History
      GROUP BY FacilityKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefFacility_History');
ELSE
  COMMIT TRANSACTION;
END IF;
