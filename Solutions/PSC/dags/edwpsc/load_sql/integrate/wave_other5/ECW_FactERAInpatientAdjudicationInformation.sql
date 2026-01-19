
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERAInpatientAdjudicationInformation AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERAInpatientAdjudicationInformation AS source
ON target.ERAInpatientAdjustmentInformationKey = source.ERAInpatientAdjustmentInformationKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERAInpatientAdjustmentInformationKey = source.ERAInpatientAdjustmentInformationKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.MIA01 = source.MIA01,
 target.MIA02 = source.MIA02,
 target.MIA03 = source.MIA03,
 target.MIA04 = source.MIA04,
 target.MIA05 = TRIM(source.MIA05),
 target.MIA06 = source.MIA06,
 target.MIA07 = source.MIA07,
 target.MIA08 = source.MIA08,
 target.MIA09 = source.MIA09,
 target.MIA10 = source.MIA10,
 target.MIA11 = source.MIA11,
 target.MIA12 = source.MIA12,
 target.MIA13 = source.MIA13,
 target.MIA14 = source.MIA14,
 target.MIA15 = source.MIA15,
 target.MIA16 = source.MIA16,
 target.MIA17 = source.MIA17,
 target.MIA18 = source.MIA18,
 target.MIA19 = source.MIA19,
 target.MIA20 = TRIM(source.MIA20),
 target.MIA21 = TRIM(source.MIA21),
 target.MIA22 = TRIM(source.MIA22),
 target.MIA23 = TRIM(source.MIA23),
 target.MIA24 = source.MIA24,
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
  INSERT (ERAInpatientAdjustmentInformationKey, ClaimKey, ClaimNumber, MIA01, MIA02, MIA03, MIA04, MIA05, MIA06, MIA07, MIA08, MIA09, MIA10, MIA11, MIA12, MIA13, MIA14, MIA15, MIA16, MIA17, MIA18, MIA19, MIA20, MIA21, MIA22, MIA23, MIA24, SEGMENT, SourceFileName, SourceFileCreatedDate, Seq, DateCreated, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERAInpatientAdjustmentInformationKey, source.ClaimKey, source.ClaimNumber, source.MIA01, source.MIA02, source.MIA03, source.MIA04, TRIM(source.MIA05), source.MIA06, source.MIA07, source.MIA08, source.MIA09, source.MIA10, source.MIA11, source.MIA12, source.MIA13, source.MIA14, source.MIA15, source.MIA16, source.MIA17, source.MIA18, source.MIA19, TRIM(source.MIA20), TRIM(source.MIA21), TRIM(source.MIA22), TRIM(source.MIA23), source.MIA24, TRIM(source.SEGMENT), TRIM(source.SourceFileName), source.SourceFileCreatedDate, source.Seq, source.DateCreated, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERAInpatientAdjustmentInformationKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERAInpatientAdjudicationInformation
      GROUP BY ERAInpatientAdjustmentInformationKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERAInpatientAdjudicationInformation');
ELSE
  COMMIT TRANSACTION;
END IF;
