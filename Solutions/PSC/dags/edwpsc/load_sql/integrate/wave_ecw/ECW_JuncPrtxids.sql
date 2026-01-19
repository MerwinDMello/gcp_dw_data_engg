
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncPrtxids AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncPrtxids AS source
ON target.ECWPrtxidsKey = source.ECWPrtxidsKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWPrtxidsKey = source.ECWPrtxidsKey,
 target.RegionKey = source.RegionKey,
 target.StartSvcDate = source.StartSvcDate,
 target.EndSvcDate = source.EndSvcDate,
 target.ProviderId = source.ProviderId,
 target.ProviderKey = source.ProviderKey,
 target.insuranceId = source.insuranceId,
 target.IplanKey = source.IplanKey,
 target.TaxId = TRIM(source.TaxId),
 target.TaxIdType = TRIM(source.TaxIdType),
 target.BillingFacilityId = source.BillingFacilityId,
 target.BillingFacilityKey = source.BillingFacilityKey,
 target.SiteId = TRIM(source.SiteId),
 target.ApptFacilityId = source.ApptFacilityId,
 target.ApptFacilityKey = source.ApptFacilityKey,
 target.AllServiceDates = source.AllServiceDates,
 target.AllAppointmentFacilities = source.AllAppointmentFacilities,
 target.deleteFlag = source.deleteFlag,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ECWPrtxidsKey, RegionKey, StartSvcDate, EndSvcDate, ProviderId, ProviderKey, insuranceId, IplanKey, TaxId, TaxIdType, BillingFacilityId, BillingFacilityKey, SiteId, ApptFacilityId, ApptFacilityKey, AllServiceDates, AllAppointmentFacilities, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ECWPrtxidsKey, source.RegionKey, source.StartSvcDate, source.EndSvcDate, source.ProviderId, source.ProviderKey, source.insuranceId, source.IplanKey, TRIM(source.TaxId), TRIM(source.TaxIdType), source.BillingFacilityId, source.BillingFacilityKey, TRIM(source.SiteId), source.ApptFacilityId, source.ApptFacilityKey, source.AllServiceDates, source.AllAppointmentFacilities, source.deleteFlag, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWPrtxidsKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncPrtxids
      GROUP BY ECWPrtxidsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncPrtxids');
ELSE
  COMMIT TRANSACTION;
END IF;
