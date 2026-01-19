
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactRHInitialError AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactRHInitialError AS source
ON target.RHInitialErrorKey = source.RHInitialErrorKey
WHEN MATCHED THEN
  UPDATE SET
  target.RHInitialErrorKey = source.RHInitialErrorKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.Coid = TRIM(source.Coid),
 target.ImportDateKey = source.ImportDateKey,
 target.RHInitialErrorClientId = TRIM(source.RHInitialErrorClientId),
 target.RHInitialErrorSubmissionDateKey = source.RHInitialErrorSubmissionDateKey,
 target.RHInitialErrorBridgeFileNumber = TRIM(source.RHInitialErrorBridgeFileNumber),
 target.RHInitialErrorClaimID = TRIM(source.RHInitialErrorClaimID),
 target.RHInitialErrorPatientControlNbr = TRIM(source.RHInitialErrorPatientControlNbr),
 target.RHInitialErrorReleasedDateKey = TRIM(source.RHInitialErrorReleasedDateKey),
 target.RHInitialErrorCategoryID = TRIM(source.RHInitialErrorCategoryID),
 target.RHInitialErrorCategoryName = TRIM(source.RHInitialErrorCategoryName),
 target.RHInitialErrorFieldDescription = TRIM(source.RHInitialErrorFieldDescription),
 target.RHInitialErrorIndex = TRIM(source.RHInitialErrorIndex),
 target.RHInitialErrorData = TRIM(source.RHInitialErrorData),
 target.RHInitialErrorCode = TRIM(source.RHInitialErrorCode),
 target.RHInitialErrorOriginalErrorInx = TRIM(source.RHInitialErrorOriginalErrorInx),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.FullClaimNumber = TRIM(source.FullClaimNumber),
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (RHInitialErrorKey, ClaimKey, ClaimNumber, Coid, ImportDateKey, RHInitialErrorClientId, RHInitialErrorSubmissionDateKey, RHInitialErrorBridgeFileNumber, RHInitialErrorClaimID, RHInitialErrorPatientControlNbr, RHInitialErrorReleasedDateKey, RHInitialErrorCategoryID, RHInitialErrorCategoryName, RHInitialErrorFieldDescription, RHInitialErrorIndex, RHInitialErrorData, RHInitialErrorCode, RHInitialErrorOriginalErrorInx, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FullClaimNumber, RegionKey)
  VALUES (source.RHInitialErrorKey, source.ClaimKey, source.ClaimNumber, TRIM(source.Coid), source.ImportDateKey, TRIM(source.RHInitialErrorClientId), source.RHInitialErrorSubmissionDateKey, TRIM(source.RHInitialErrorBridgeFileNumber), TRIM(source.RHInitialErrorClaimID), TRIM(source.RHInitialErrorPatientControlNbr), TRIM(source.RHInitialErrorReleasedDateKey), TRIM(source.RHInitialErrorCategoryID), TRIM(source.RHInitialErrorCategoryName), TRIM(source.RHInitialErrorFieldDescription), TRIM(source.RHInitialErrorIndex), TRIM(source.RHInitialErrorData), TRIM(source.RHInitialErrorCode), TRIM(source.RHInitialErrorOriginalErrorInx), source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT RHInitialErrorKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactRHInitialError
      GROUP BY RHInitialErrorKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactRHInitialError');
ELSE
  COMMIT TRANSACTION;
END IF;
