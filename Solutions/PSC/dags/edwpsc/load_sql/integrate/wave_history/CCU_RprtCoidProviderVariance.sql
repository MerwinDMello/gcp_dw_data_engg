
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidProviderVariance AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtCoidProviderVariance AS source
ON target.SnapShotDate = source.SnapShotDate AND target.CCUCoidProviderVarianceHistoryKey = source.CCUCoidProviderVarianceHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUCoidProviderVarianceHistoryKey = source.CCUCoidProviderVarianceHistoryKey,
 target.PriorSnapShotDateKey = source.PriorSnapShotDateKey,
 target.SnapShotDate = source.SnapShotDate,
 target.RecordType = TRIM(source.RecordType),
 target.Coid = TRIM(source.Coid),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderName = TRIM(source.ProviderName),
 target.PriorStatus = TRIM(source.PriorStatus),
 target.PriorCentralizedStatus = TRIM(source.PriorCentralizedStatus),
 target.CurrStatus = TRIM(source.CurrStatus),
 target.CurrCentralizedStatus = TRIM(source.CurrCentralizedStatus),
 target.RESULT = TRIM(source.RESULT),
 target.ResultCount = source.ResultCount,
 target.LoadDate = source.LoadDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (CCUCoidProviderVarianceHistoryKey, PriorSnapShotDateKey, SnapShotDate, RecordType, Coid, ProviderNPI, ProviderName, PriorStatus, PriorCentralizedStatus, CurrStatus, CurrCentralizedStatus, RESULT, ResultCount, LoadDate, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.CCUCoidProviderVarianceHistoryKey, source.PriorSnapShotDateKey, source.SnapShotDate, TRIM(source.RecordType), TRIM(source.Coid), TRIM(source.ProviderNPI), TRIM(source.ProviderName), TRIM(source.PriorStatus), TRIM(source.PriorCentralizedStatus), TRIM(source.CurrStatus), TRIM(source.CurrCentralizedStatus), TRIM(source.RESULT), source.ResultCount, source.LoadDate, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, CCUCoidProviderVarianceHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidProviderVariance
      GROUP BY SnapShotDate, CCUCoidProviderVarianceHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidProviderVariance');
ELSE
  COMMIT TRANSACTION;
END IF;
