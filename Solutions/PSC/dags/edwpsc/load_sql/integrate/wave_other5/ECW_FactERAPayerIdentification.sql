
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERAPayerIdentification AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERAPayerIdentification AS source
ON target.ERAPayerIdentificationKey = source.ERAPayerIdentificationKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERAPayerIdentificationKey = source.ERAPayerIdentificationKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.N101 = TRIM(source.N101),
 target.N102 = TRIM(source.N102),
 target.N103 = TRIM(source.N103),
 target.N104 = TRIM(source.N104),
 target.N105 = TRIM(source.N105),
 target.N106 = TRIM(source.N106),
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
  INSERT (ERAPayerIdentificationKey, ClaimKey, ClaimNumber, N101, N102, N103, N104, N105, N106, SEGMENT, SourceFileName, SourceFileCreatedDate, Seq, DateCreated, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERAPayerIdentificationKey, source.ClaimKey, source.ClaimNumber, TRIM(source.N101), TRIM(source.N102), TRIM(source.N103), TRIM(source.N104), TRIM(source.N105), TRIM(source.N106), TRIM(source.SEGMENT), TRIM(source.SourceFileName), source.SourceFileCreatedDate, source.Seq, source.DateCreated, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERAPayerIdentificationKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERAPayerIdentification
      GROUP BY ERAPayerIdentificationKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERAPayerIdentification');
ELSE
  COMMIT TRANSACTION;
END IF;
