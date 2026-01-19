
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2QR AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactIETV2QR AS source
ON target.IETV2QRKey = source.IETV2QRKey
WHEN MATCHED THEN
  UPDATE SET
  target.IETV2QRKey = source.IETV2QRKey,
 target.RowNumber = source.RowNumber,
 target.CorrespondenceId = source.CorrespondenceId,
 target.Content = TRIM(source.Content),
 target.DateCreated = source.DateCreated,
 target.UserCreated = TRIM(source.UserCreated),
 target.CorrespondenceSubjectId = TRIM(source.CorrespondenceSubjectId),
 target.ClaimCaseId = TRIM(source.ClaimCaseId),
 target.ClaimNumber = TRIM(source.ClaimNumber),
 target.ClaimKey = source.ClaimKey,
 target.Inbound = source.Inbound,
 target.Resolution = TRIM(source.Resolution),
 target.ResolutionId = TRIM(source.ResolutionId),
 target.ResolutionDate = source.ResolutionDate,
 target.ResolutionUserId = TRIM(source.ResolutionUserId),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.FullClaimNumber = TRIM(source.FullClaimNumber),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (IETV2QRKey, RowNumber, CorrespondenceId, Content, DateCreated, UserCreated, CorrespondenceSubjectId, ClaimCaseId, ClaimNumber, ClaimKey, Inbound, Resolution, ResolutionId, ResolutionDate, ResolutionUserId, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FullClaimNumber, DWLastUpdateDateTime)
  VALUES (source.IETV2QRKey, source.RowNumber, source.CorrespondenceId, TRIM(source.Content), source.DateCreated, TRIM(source.UserCreated), TRIM(source.CorrespondenceSubjectId), TRIM(source.ClaimCaseId), TRIM(source.ClaimNumber), source.ClaimKey, source.Inbound, TRIM(source.Resolution), TRIM(source.ResolutionId), source.ResolutionDate, TRIM(source.ResolutionUserId), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.FullClaimNumber), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IETV2QRKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2QR
      GROUP BY IETV2QRKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2QR');
ELSE
  COMMIT TRANSACTION;
END IF;
