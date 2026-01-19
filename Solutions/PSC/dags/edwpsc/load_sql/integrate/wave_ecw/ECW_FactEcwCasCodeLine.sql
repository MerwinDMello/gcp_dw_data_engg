
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEcwCasCodeLine AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEcwCasCodeLine AS source
ON target.EcwCasCodeLineKey = source.EcwCasCodeLineKey
WHEN MATCHED THEN
  UPDATE SET
  target.EcwCasCodeLineKey = source.EcwCasCodeLineKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimLineChargesKey = source.ClaimLineChargesKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.ClaimLinePaymentsKey = source.ClaimLinePaymentsKey,
 target.CasCodeKey = source.CasCodeKey,
 target.RegionKey = source.RegionKey,
 target.CasDetailId = source.CasDetailId,
 target.CasGroupCode = TRIM(source.CasGroupCode),
 target.CasCode = TRIM(source.CasCode),
 target.CasAmount = source.CasAmount,
 target.CasdeleteFlag = source.CasdeleteFlag,
 target.CasPostedAs = source.CasPostedAs,
 target.SourceAPrimaryKey = source.SourceAPrimaryKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (EcwCasCodeLineKey, ClaimKey, ClaimNumber, ClaimLineChargesKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, ClaimLinePaymentsKey, CasCodeKey, RegionKey, CasDetailId, CasGroupCode, CasCode, CasAmount, CasdeleteFlag, CasPostedAs, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ArchivedRecord)
  VALUES (source.EcwCasCodeLineKey, source.ClaimKey, source.ClaimNumber, source.ClaimLineChargesKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.ClaimLinePaymentsKey, source.CasCodeKey, source.RegionKey, source.CasDetailId, TRIM(source.CasGroupCode), TRIM(source.CasCode), source.CasAmount, source.CasdeleteFlag, source.CasPostedAs, source.SourceAPrimaryKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EcwCasCodeLineKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEcwCasCodeLine
      GROUP BY EcwCasCodeLineKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEcwCasCodeLine');
ELSE
  COMMIT TRANSACTION;
END IF;
