
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EBOC_FactEdiDiscriminatorDeposit AS target
USING {{ params.param_psc_stage_dataset_name }}.EBOC_FactEdiDiscriminatorDeposit AS source
ON target.EBOCEdiDiscriminatorDepositKey = source.EBOCEdiDiscriminatorDepositKey
WHEN MATCHED THEN
  UPDATE SET
  target.EBOCEdiDiscriminatorDepositKey = source.EBOCEdiDiscriminatorDepositKey,
 target.DepositID = source.DepositID,
 target.TreasuryBatchNumber = TRIM(source.TreasuryBatchNumber),
 target.CreatorLoginName = TRIM(source.CreatorLoginName),
 target.CreatorName = TRIM(source.CreatorName),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.CreatedDateTime = source.CreatedDateTime
WHEN NOT MATCHED THEN
  INSERT (EBOCEdiDiscriminatorDepositKey, DepositID, TreasuryBatchNumber, CreatorLoginName, CreatorName, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, CreatedDateTime)
  VALUES (source.EBOCEdiDiscriminatorDepositKey, source.DepositID, TRIM(source.TreasuryBatchNumber), TRIM(source.CreatorLoginName), TRIM(source.CreatorName), source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.CreatedDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EBOCEdiDiscriminatorDepositKey
      FROM {{ params.param_psc_core_dataset_name }}.EBOC_FactEdiDiscriminatorDeposit
      GROUP BY EBOCEdiDiscriminatorDepositKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EBOC_FactEdiDiscriminatorDeposit');
ELSE
  COMMIT TRANSACTION;
END IF;
