
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtIccChargeLag AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtIccChargeLag AS source
ON target.SnapShotDate = source.SnapShotDate AND target.IccChargeLagKey = source.IccChargeLagKey
WHEN MATCHED THEN
  UPDATE SET
  target.IccChargeLagKey = source.IccChargeLagKey,
 target.FirstTransactionMonth = source.FirstTransactionMonth,
 target.RegionKey = source.RegionKey,
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.LOBName = TRIM(source.LOBName),
 target.COID = TRIM(source.COID),
 target.COIDName = TRIM(source.COIDName),
 target.ServicingProviderName = TRIM(source.ServicingProviderName),
 target.RenderingProviderName = TRIM(source.RenderingProviderName),
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.LastClaimNumber = source.LastClaimNumber,
 target.VoidFlag = source.VoidFlag,
 target.VoidAndRecreateDate = source.VoidAndRecreateDate,
 target.ServiceDateKey = source.ServiceDateKey,
 target.LastVoidDateKey = source.LastVoidDateKey,
 target.FirstTransactionDateKey = source.FirstTransactionDateKey,
 target.Lag = source.Lag,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (IccChargeLagKey, FirstTransactionMonth, RegionKey, GroupName, DivisionName, MarketName, LOBName, COID, COIDName, ServicingProviderName, RenderingProviderName, ClaimKey, ClaimNumber, LastClaimNumber, VoidFlag, VoidAndRecreateDate, ServiceDateKey, LastVoidDateKey, FirstTransactionDateKey, Lag, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.IccChargeLagKey, source.FirstTransactionMonth, source.RegionKey, TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.LOBName), TRIM(source.COID), TRIM(source.COIDName), TRIM(source.ServicingProviderName), TRIM(source.RenderingProviderName), source.ClaimKey, source.ClaimNumber, source.LastClaimNumber, source.VoidFlag, source.VoidAndRecreateDate, source.ServiceDateKey, source.LastVoidDateKey, source.FirstTransactionDateKey, source.Lag, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, IccChargeLagKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtIccChargeLag
      GROUP BY SnapShotDate, IccChargeLagKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtIccChargeLag');
ELSE
  COMMIT TRANSACTION;
END IF;
