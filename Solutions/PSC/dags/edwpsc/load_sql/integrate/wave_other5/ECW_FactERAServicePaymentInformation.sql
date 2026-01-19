
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERAServicePaymentInformation AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERAServicePaymentInformation AS source
ON target.ERAServicePaymentInformationKey = source.ERAServicePaymentInformationKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERAServicePaymentInformationKey = source.ERAServicePaymentInformationKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.SVC01 = TRIM(source.SVC01),
 target.SVC02 = source.SVC02,
 target.SVC03 = source.SVC03,
 target.SVC04 = TRIM(source.SVC04),
 target.SVC05 = source.SVC05,
 target.SVC06 = TRIM(source.SVC06),
 target.SVC07 = source.SVC07,
 target.CPTCode = TRIM(source.CPTCode),
 target.Mod1 = TRIM(source.Mod1),
 target.Mod2 = TRIM(source.Mod2),
 target.Mod3 = TRIM(source.Mod3),
 target.Mod4 = TRIM(source.Mod4),
 target.SvcStartDate = source.SvcStartDate,
 target.SvcEndDate = source.SvcEndDate,
 target.SvcAllowedAmt = source.SvcAllowedAmt,
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
  INSERT (ERAServicePaymentInformationKey, ClaimKey, ClaimNumber, SVC01, SVC02, SVC03, SVC04, SVC05, SVC06, SVC07, CPTCode, Mod1, Mod2, Mod3, Mod4, SvcStartDate, SvcEndDate, SvcAllowedAmt, SEGMENT, SourceFileName, SourceFileCreatedDate, Seq, DateCreated, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERAServicePaymentInformationKey, source.ClaimKey, source.ClaimNumber, TRIM(source.SVC01), source.SVC02, source.SVC03, TRIM(source.SVC04), source.SVC05, TRIM(source.SVC06), source.SVC07, TRIM(source.CPTCode), TRIM(source.Mod1), TRIM(source.Mod2), TRIM(source.Mod3), TRIM(source.Mod4), source.SvcStartDate, source.SvcEndDate, source.SvcAllowedAmt, TRIM(source.SEGMENT), TRIM(source.SourceFileName), source.SourceFileCreatedDate, source.Seq, source.DateCreated, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERAServicePaymentInformationKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERAServicePaymentInformation
      GROUP BY ERAServicePaymentInformationKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERAServicePaymentInformation');
ELSE
  COMMIT TRANSACTION;
END IF;
