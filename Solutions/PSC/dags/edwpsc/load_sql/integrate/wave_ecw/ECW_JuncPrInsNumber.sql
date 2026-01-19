
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncPrInsNumber AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncPrInsNumber AS source
ON target.ECWPrInsNumberKey = source.ECWPrInsNumberKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWPrInsNumberKey = source.ECWPrInsNumberKey,
 target.RegionKey = source.RegionKey,
 target.InsuranceId = source.InsuranceId,
 target.IplanKey = source.IplanKey,
 target.ProviderId = source.ProviderId,
 target.ProviderKey = source.ProviderKey,
 target.ProviderNumber = TRIM(source.ProviderNumber),
 target.GroupNumber = TRIM(source.GroupNumber),
 target.EffDate = source.EffDate,
 target.UseDefaults = source.UseDefaults,
 target.TaxId = TRIM(source.TaxId),
 target.TaxIdType = TRIM(source.TaxIdType),
 target.BillingFacilityId = source.BillingFacilityId,
 target.BillingFacilityKey = source.BillingFacilityKey,
 target.SiteId = TRIM(source.SiteId),
 target.RefProvIdType = TRIM(source.RefProvIdType),
 target.RefProvGrpIdType = TRIM(source.RefProvGrpIdType),
 target.UseHCFADefaults = source.UseHCFADefaults,
 target.HCFA24K = TRIM(source.HCFA24K),
 target.HCFA33Pin = TRIM(source.HCFA33Pin),
 target.HCFA33Grp = TRIM(source.HCFA33Grp),
 target.UseEMCDefaults = source.UseEMCDefaults,
 target.BA02 = TRIM(source.BA02),
 target.CA028 = TRIM(source.CA028),
 target.BA015 = TRIM(source.BA015),
 target.Hcfa24KIdType = TRIM(source.Hcfa24KIdType),
 target.Hcfa33GrpIdType = TRIM(source.Hcfa33GrpIdType),
 target.deleteFlag = source.deleteFlag,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ECWPrInsNumberKey, RegionKey, InsuranceId, IplanKey, ProviderId, ProviderKey, ProviderNumber, GroupNumber, EffDate, UseDefaults, TaxId, TaxIdType, BillingFacilityId, BillingFacilityKey, SiteId, RefProvIdType, RefProvGrpIdType, UseHCFADefaults, HCFA24K, HCFA33Pin, HCFA33Grp, UseEMCDefaults, BA02, CA028, BA015, Hcfa24KIdType, Hcfa33GrpIdType, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ECWPrInsNumberKey, source.RegionKey, source.InsuranceId, source.IplanKey, source.ProviderId, source.ProviderKey, TRIM(source.ProviderNumber), TRIM(source.GroupNumber), source.EffDate, source.UseDefaults, TRIM(source.TaxId), TRIM(source.TaxIdType), source.BillingFacilityId, source.BillingFacilityKey, TRIM(source.SiteId), TRIM(source.RefProvIdType), TRIM(source.RefProvGrpIdType), source.UseHCFADefaults, TRIM(source.HCFA24K), TRIM(source.HCFA33Pin), TRIM(source.HCFA33Grp), source.UseEMCDefaults, TRIM(source.BA02), TRIM(source.CA028), TRIM(source.BA015), TRIM(source.Hcfa24KIdType), TRIM(source.Hcfa33GrpIdType), source.deleteFlag, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWPrInsNumberKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncPrInsNumber
      GROUP BY ECWPrInsNumberKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncPrInsNumber');
ELSE
  COMMIT TRANSACTION;
END IF;
