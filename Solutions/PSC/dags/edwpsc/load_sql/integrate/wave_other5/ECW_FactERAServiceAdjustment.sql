
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERAServiceAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERAServiceAdjustment AS source
ON target.ERAServiceAdjustmentKey = source.ERAServiceAdjustmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERAServiceAdjustmentKey = source.ERAServiceAdjustmentKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.CAS01 = TRIM(source.CAS01),
 target.CAS02 = TRIM(source.CAS02),
 target.CAS03 = source.CAS03,
 target.CAS04 = source.CAS04,
 target.CAS05 = TRIM(source.CAS05),
 target.CAS06 = source.CAS06,
 target.CAS07 = source.CAS07,
 target.CAS08 = TRIM(source.CAS08),
 target.CAS09 = source.CAS09,
 target.CAS10 = source.CAS10,
 target.CAS11 = TRIM(source.CAS11),
 target.CAS12 = source.CAS12,
 target.CAS13 = source.CAS13,
 target.CAS14 = TRIM(source.CAS14),
 target.CAS15 = source.CAS15,
 target.CAS16 = source.CAS16,
 target.CAS17 = TRIM(source.CAS17),
 target.CAS18 = source.CAS18,
 target.CAS19 = source.CAS19,
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
  INSERT (ERAServiceAdjustmentKey, ClaimKey, ClaimNumber, CAS01, CAS02, CAS03, CAS04, CAS05, CAS06, CAS07, CAS08, CAS09, CAS10, CAS11, CAS12, CAS13, CAS14, CAS15, CAS16, CAS17, CAS18, CAS19, SEGMENT, SourceFileName, SourceFileCreatedDate, Seq, DateCreated, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERAServiceAdjustmentKey, source.ClaimKey, source.ClaimNumber, TRIM(source.CAS01), TRIM(source.CAS02), source.CAS03, source.CAS04, TRIM(source.CAS05), source.CAS06, source.CAS07, TRIM(source.CAS08), source.CAS09, source.CAS10, TRIM(source.CAS11), source.CAS12, source.CAS13, TRIM(source.CAS14), source.CAS15, source.CAS16, TRIM(source.CAS17), source.CAS18, source.CAS19, TRIM(source.SEGMENT), TRIM(source.SourceFileName), source.SourceFileCreatedDate, source.Seq, source.DateCreated, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERAServiceAdjustmentKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERAServiceAdjustment
      GROUP BY ERAServiceAdjustmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERAServiceAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
