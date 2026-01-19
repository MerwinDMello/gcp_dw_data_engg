
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiGroupNPI AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncEdiGroupNPI AS source
ON target.ECWEdiGroupNPIKey = source.ECWEdiGroupNPIKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWEdiGroupNPIKey = source.ECWEdiGroupNPIKey,
 target.RegionKey = source.RegionKey,
 target.NPIRuleName = TRIM(source.NPIRuleName),
 target.GroupNPI = TRIM(source.GroupNPI),
 target.deleteFlag = source.deleteFlag,
 target.AllApptFacilities = source.AllApptFacilities,
 target.AllPracticingProviders = source.AllPracticingProviders,
 target.SelectiveServiceDates = source.SelectiveServiceDates,
 target.StartServiceDate = source.StartServiceDate,
 target.EndServiceDate = source.EndServiceDate,
 target.AllFacilities = source.AllFacilities,
 target.AllPracticingFacilities = source.AllPracticingFacilities,
 target.AllNonPracticingFacilities = source.AllNonPracticingFacilities,
 target.SelectiveInsurances = source.SelectiveInsurances,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ECWEdiGroupNPIKey, RegionKey, NPIRuleName, GroupNPI, deleteFlag, AllApptFacilities, AllPracticingProviders, SelectiveServiceDates, StartServiceDate, EndServiceDate, AllFacilities, AllPracticingFacilities, AllNonPracticingFacilities, SelectiveInsurances, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ECWEdiGroupNPIKey, source.RegionKey, TRIM(source.NPIRuleName), TRIM(source.GroupNPI), source.deleteFlag, source.AllApptFacilities, source.AllPracticingProviders, source.SelectiveServiceDates, source.StartServiceDate, source.EndServiceDate, source.AllFacilities, source.AllPracticingFacilities, source.AllNonPracticingFacilities, source.SelectiveInsurances, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWEdiGroupNPIKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiGroupNPI
      GROUP BY ECWEdiGroupNPIKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiGroupNPI');
ELSE
  COMMIT TRANSACTION;
END IF;
