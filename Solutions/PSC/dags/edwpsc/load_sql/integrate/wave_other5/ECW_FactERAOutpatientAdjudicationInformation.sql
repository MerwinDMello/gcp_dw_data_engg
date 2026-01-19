
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERAOutpatientAdjudicationInformation AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERAOutpatientAdjudicationInformation AS source
ON target.ERAOutpatientAdjustmentInformationKey = source.ERAOutpatientAdjustmentInformationKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERAOutpatientAdjustmentInformationKey = source.ERAOutpatientAdjustmentInformationKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.MOA01 = source.MOA01,
 target.MOA02 = source.MOA02,
 target.MOA03 = TRIM(source.MOA03),
 target.MOA04 = TRIM(source.MOA04),
 target.MOA05 = TRIM(source.MOA05),
 target.MOA06 = TRIM(source.MOA06),
 target.MOA07 = TRIM(source.MOA07),
 target.MOA08 = source.MOA08,
 target.MOA09 = source.MOA09,
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
  INSERT (ERAOutpatientAdjustmentInformationKey, ClaimKey, ClaimNumber, MOA01, MOA02, MOA03, MOA04, MOA05, MOA06, MOA07, MOA08, MOA09, SEGMENT, SourceFileName, SourceFileCreatedDate, Seq, DateCreated, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERAOutpatientAdjustmentInformationKey, source.ClaimKey, source.ClaimNumber, source.MOA01, source.MOA02, TRIM(source.MOA03), TRIM(source.MOA04), TRIM(source.MOA05), TRIM(source.MOA06), TRIM(source.MOA07), source.MOA08, source.MOA09, TRIM(source.SEGMENT), TRIM(source.SourceFileName), source.SourceFileCreatedDate, source.Seq, source.DateCreated, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERAOutpatientAdjustmentInformationKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERAOutpatientAdjudicationInformation
      GROUP BY ERAOutpatientAdjustmentInformationKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERAOutpatientAdjudicationInformation');
ELSE
  COMMIT TRANSACTION;
END IF;
