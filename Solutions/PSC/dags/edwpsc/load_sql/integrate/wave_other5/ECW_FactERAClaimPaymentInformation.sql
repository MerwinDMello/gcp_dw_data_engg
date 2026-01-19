
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactERAClaimPaymentInformation AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactERAClaimPaymentInformation AS source
ON target.ERAClaimPaymentInformationKey = source.ERAClaimPaymentInformationKey
WHEN MATCHED THEN
  UPDATE SET
  target.ERAClaimPaymentInformationKey = source.ERAClaimPaymentInformationKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.CLP01 = TRIM(source.CLP01),
 target.CLP02 = TRIM(source.CLP02),
 target.CLP03 = source.CLP03,
 target.CLP04 = source.CLP04,
 target.CLP05 = source.CLP05,
 target.CLP06 = TRIM(source.CLP06),
 target.CLP07 = TRIM(source.CLP07),
 target.CLP08 = TRIM(source.CLP08),
 target.CLP09 = TRIM(source.CLP09),
 target.CLP10 = TRIM(source.CLP10),
 target.CLP11 = TRIM(source.CLP11),
 target.CLP12 = source.CLP12,
 target.CLP13 = source.CLP13,
 target.CLP14 = TRIM(source.CLP14),
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
  INSERT (ERAClaimPaymentInformationKey, ClaimKey, ClaimNumber, CLP01, CLP02, CLP03, CLP04, CLP05, CLP06, CLP07, CLP08, CLP09, CLP10, CLP11, CLP12, CLP13, CLP14, SEGMENT, SourceFileName, SourceFileCreatedDate, Seq, DateCreated, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeletedFlag, FullClaimNumber, RegionKey)
  VALUES (source.ERAClaimPaymentInformationKey, source.ClaimKey, source.ClaimNumber, TRIM(source.CLP01), TRIM(source.CLP02), source.CLP03, source.CLP04, source.CLP05, TRIM(source.CLP06), TRIM(source.CLP07), TRIM(source.CLP08), TRIM(source.CLP09), TRIM(source.CLP10), TRIM(source.CLP11), source.CLP12, source.CLP13, TRIM(source.CLP14), TRIM(source.SEGMENT), TRIM(source.SourceFileName), source.SourceFileCreatedDate, source.Seq, source.DateCreated, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeletedFlag, TRIM(source.FullClaimNumber), source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ERAClaimPaymentInformationKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactERAClaimPaymentInformation
      GROUP BY ERAClaimPaymentInformationKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactERAClaimPaymentInformation');
ELSE
  COMMIT TRANSACTION;
END IF;
