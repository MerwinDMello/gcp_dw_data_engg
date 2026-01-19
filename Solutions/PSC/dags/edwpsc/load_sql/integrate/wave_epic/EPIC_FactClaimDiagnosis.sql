
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimDiagnosis AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimDiagnosis AS source
ON target.ClaimDiagnosisKey = source.ClaimDiagnosisKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimDiagnosisKey = source.ClaimDiagnosisKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.PrimaryCode = source.PrimaryCode,
 target.ICDOrder = source.ICDOrder,
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimDiagnosisKey, ClaimKey, ClaimNumber, VisitNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, DiagnosisCodeKey, PrimaryCode, ICDOrder, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimDiagnosisKey, source.ClaimKey, source.ClaimNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.DiagnosisCodeKey, source.PrimaryCode, source.ICDOrder, source.DeleteFlag, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimDiagnosisKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimDiagnosis
      GROUP BY ClaimDiagnosisKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimDiagnosis');
ELSE
  COMMIT TRANSACTION;
END IF;
