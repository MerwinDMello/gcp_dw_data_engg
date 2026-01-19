
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtCodingVolumes AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtCodingVolumes AS source
ON target.SnapShotDate = source.SnapShotDate AND target.CodingVolumeKey = source.CodingVolumeKey
WHEN MATCHED THEN
  UPDATE SET
  target.CodingVolumeKey = source.CodingVolumeKey,
 target.Coid = TRIM(source.Coid),
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.PatientKey = source.PatientKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.TransactionByUserKey = source.TransactionByUserKey,
 target.TransactionDateKey = source.TransactionDateKey,
 target.SystemName = TRIM(source.SystemName),
 target.PraticeName = TRIM(source.PraticeName),
 target.VisitNumber = source.VisitNumber,
 target.RowNumber = source.RowNumber,
 target.RecordType = TRIM(source.RecordType),
 target.SnapShotDate = source.SnapShotDate,
 target.DeptCode = TRIM(source.DeptCode),
 target.UserType = TRIM(source.UserType),
 target.PlaceOfService = source.PlaceOfService,
 target.ClaimStatusName = TRIM(source.ClaimStatusName),
 target.VisitTypeName = TRIM(source.VisitTypeName)
WHEN NOT MATCHED THEN
  INSERT (CodingVolumeKey, Coid, ClaimKey, ClaimNumber, RegionKey, PatientKey, ServicingProviderKey, ClaimLineChargeKey, TransactionByUserKey, TransactionDateKey, SystemName, PraticeName, VisitNumber, RowNumber, RecordType, SnapShotDate, DeptCode, UserType, PlaceOfService, ClaimStatusName, VisitTypeName)
  VALUES (source.CodingVolumeKey, TRIM(source.Coid), source.ClaimKey, source.ClaimNumber, source.RegionKey, source.PatientKey, source.ServicingProviderKey, source.ClaimLineChargeKey, source.TransactionByUserKey, source.TransactionDateKey, TRIM(source.SystemName), TRIM(source.PraticeName), source.VisitNumber, source.RowNumber, TRIM(source.RecordType), source.SnapShotDate, TRIM(source.DeptCode), TRIM(source.UserType), source.PlaceOfService, TRIM(source.ClaimStatusName), TRIM(source.VisitTypeName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, CodingVolumeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtCodingVolumes
      GROUP BY SnapShotDate, CodingVolumeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtCodingVolumes');
ELSE
  COMMIT TRANSACTION;
END IF;
