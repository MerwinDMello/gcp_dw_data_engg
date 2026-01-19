
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEcwRemarkCodeLine AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEcwRemarkCodeLine AS source
ON target.EcwRemarkCodeLineKey = source.EcwRemarkCodeLineKey
WHEN MATCHED THEN
  UPDATE SET
  target.EcwRemarkCodeLineKey = source.EcwRemarkCodeLineKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimLineChargesKey = source.ClaimLineChargesKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.ClaimLinePaymentsKey = source.ClaimLinePaymentsKey,
 target.RegionKey = source.RegionKey,
 target.RemarkDetailId = source.RemarkDetailId,
 target.RemarkCodeType = TRIM(source.RemarkCodeType),
 target.RemarkCode = TRIM(source.RemarkCode),
 target.RemarkdeleteFlag = source.RemarkdeleteFlag,
 target.SourceAPrimaryKey = source.SourceAPrimaryKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EcwRemarkCodeLineKey, ClaimKey, ClaimNumber, ClaimLineChargesKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, ClaimLinePaymentsKey, RegionKey, RemarkDetailId, RemarkCodeType, RemarkCode, RemarkdeleteFlag, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EcwRemarkCodeLineKey, source.ClaimKey, source.ClaimNumber, source.ClaimLineChargesKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.ClaimLinePaymentsKey, source.RegionKey, source.RemarkDetailId, TRIM(source.RemarkCodeType), TRIM(source.RemarkCode), source.RemarkdeleteFlag, source.SourceAPrimaryKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EcwRemarkCodeLineKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEcwRemarkCodeLine
      GROUP BY EcwRemarkCodeLineKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEcwRemarkCodeLine');
ELSE
  COMMIT TRANSACTION;
END IF;
