
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactRHSubmissionDates AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactRHSubmissionDates AS source
ON target.RHSubmissionDateKey = source.RHSubmissionDateKey
WHEN MATCHED THEN
  UPDATE SET
  target.RHSubmissionDateKey = source.RHSubmissionDateKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.Practice = TRIM(source.Practice),
 target.ClaimSubmissionDateKey = source.ClaimSubmissionDateKey,
 target.TransmissionType = TRIM(source.TransmissionType),
 target.CPIDKey = TRIM(source.CPIDKey),
 target.PayerIndicator = TRIM(source.PayerIndicator),
 target.PayerName = TRIM(source.PayerName),
 target.ImportDateKey = source.ImportDateKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (RHSubmissionDateKey, ClaimKey, ClaimNumber, Coid, RegionKey, Practice, ClaimSubmissionDateKey, TransmissionType, CPIDKey, PayerIndicator, PayerName, ImportDateKey, SourceAPrimaryKeyValue, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.RHSubmissionDateKey, source.ClaimKey, source.ClaimNumber, TRIM(source.Coid), source.RegionKey, TRIM(source.Practice), source.ClaimSubmissionDateKey, TRIM(source.TransmissionType), TRIM(source.CPIDKey), TRIM(source.PayerIndicator), TRIM(source.PayerName), source.ImportDateKey, TRIM(source.SourceAPrimaryKeyValue), TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT RHSubmissionDateKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactRHSubmissionDates
      GROUP BY RHSubmissionDateKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactRHSubmissionDates');
ELSE
  COMMIT TRANSACTION;
END IF;
