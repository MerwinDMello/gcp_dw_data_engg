
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimDiagnosis AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimDiagnosis AS source
ON target.ClaimDiagnosisKey = source.ClaimDiagnosisKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimDiagnosisKey = source.ClaimDiagnosisKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.PrimaryCode = source.PrimaryCode,
 target.ICDOrder = source.ICDOrder,
 target.ICDCode = TRIM(source.ICDCode),
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.SNOMEDValue = TRIM(source.SNOMEDValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.CreatedDate = source.CreatedDate,
 target.CreatedByUserKey = source.CreatedByUserKey
WHEN NOT MATCHED THEN
  INSERT (ClaimDiagnosisKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, DiagnosisCodeKey, PrimaryCode, ICDOrder, ICDCode, DeleteFlag, SourceAPrimaryKeyValue, SourceRecordLastUpdated, SNOMEDValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, CreatedDate, CreatedByUserKey)
  VALUES (source.ClaimDiagnosisKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.DiagnosisCodeKey, source.PrimaryCode, source.ICDOrder, TRIM(source.ICDCode), source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceRecordLastUpdated, TRIM(source.SNOMEDValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.CreatedDate, source.CreatedByUserKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimDiagnosisKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaimDiagnosis
      GROUP BY ClaimDiagnosisKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaimDiagnosis');
ELSE
  COMMIT TRANSACTION;
END IF;
