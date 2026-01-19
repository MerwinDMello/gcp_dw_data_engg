
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERARemarkCodes AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERARemarkCodes AS source
ON target.ERARemarkCodesKey = source.ERARemarkCodesKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERARemarkCodesKey = source.ERARemarkCodesKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.LQ01 = TRIM(source.LQ01),
 target.LQ02 = TRIM(source.LQ02),
 target.SEGMENT = TRIM(source.SEGMENT),
 target.SourceFileName = TRIM(source.SourceFileName),
 target.SourceFileCreatedDate = source.SourceFileCreatedDate,
 target.Seq = source.Seq,
 target.DateCreated = source.DateCreated,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeletedFlag = source.DeletedFlag,
 target.FullClaimNumber = TRIM(source.FullClaimNumber),
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (ERARemarkCodesKey, ClaimKey, ClaimNumber, LQ01, LQ02, SEGMENT, SourceFileName, SourceFileCreatedDate, Seq, DateCreated, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERARemarkCodesKey, source.ClaimKey, source.ClaimNumber, TRIM(source.LQ01), TRIM(source.LQ02), TRIM(source.SEGMENT), TRIM(source.SourceFileName), source.SourceFileCreatedDate, source.Seq, source.DateCreated, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERARemarkCodesKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERARemarkCodes
      GROUP BY ERARemarkCodesKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERARemarkCodes');
ELSE
  COMMIT TRANSACTION;
END IF;
