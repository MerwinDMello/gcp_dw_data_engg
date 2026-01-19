
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtIetTrendingOpenIETs AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtIetTrendingOpenIETs AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = TRIM(source.SnapShotDate),
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.CoidName = TRIM(source.CoidName),
 target.CoidLOB = TRIM(source.CoidLOB),
 target.TotalErrorCount = source.TotalErrorCount,
 target.TotalAgeDays = source.TotalAgeDays,
 target.ErrorCountOver30Days = source.ErrorCountOver30Days,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, GroupName, DivisionName, MarketName, CoidName, CoidLOB, TotalErrorCount, TotalAgeDays, ErrorCountOver30Days, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
  VALUES (TRIM(source.SnapShotDate), TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.CoidName), TRIM(source.CoidLOB), source.TotalErrorCount, source.TotalAgeDays, source.ErrorCountOver30Days, TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtIetTrendingOpenIETs
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtIetTrendingOpenIETs');
ELSE
  COMMIT TRANSACTION;
END IF;
