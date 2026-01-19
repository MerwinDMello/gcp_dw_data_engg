
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotClaimsOnHold AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotClaimsOnHold AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.LoadDateKey = source.LoadDateKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.HoldCodeKey = source.HoldCodeKey,
 target.HoldCode = source.HoldCode,
 target.HoldCodeName = TRIM(source.HoldCodeName),
 target.HoldFromDateKey = source.HoldFromDateKey,
 target.HoldToDateKey = source.HoldToDateKey,
 target.DaysOnHold = source.DaysOnHold,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = source.InsertedBy,
 target.InsertedDTM = source.InsertedDTM
WHEN NOT MATCHED THEN
  INSERT (MonthID, SnapShotDate, LoadDateKey, ClaimKey, ClaimNumber, RegionKey, HoldCodeKey, HoldCode, HoldCodeName, HoldFromDateKey, HoldToDateKey, DaysOnHold, SourceSystemCode, InsertedBy, InsertedDTM)
  VALUES (source.MonthID, source.SnapShotDate, source.LoadDateKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, source.HoldCodeKey, source.HoldCode, TRIM(source.HoldCodeName), source.HoldFromDateKey, source.HoldToDateKey, source.DaysOnHold, TRIM(source.SourceSystemCode), source.InsertedBy, source.InsertedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotClaimsOnHold
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotClaimsOnHold');
ELSE
  COMMIT TRANSACTION;
END IF;
