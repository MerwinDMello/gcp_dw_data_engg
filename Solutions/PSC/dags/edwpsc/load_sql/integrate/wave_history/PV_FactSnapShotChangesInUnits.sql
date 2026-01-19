
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotChangesInUnits AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotChangesInUnits AS source
ON target.SnapShotDate = source.SnapShotDate AND target.UnitsKey = source.UnitsKey
WHEN MATCHED THEN
  UPDATE SET
  target.UnitsKey = source.UnitsKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.CPTID = TRIM(source.CPTID),
 target.DifferenceUnitsValue = source.DifferenceUnitsValue,
 target.DifferenceChargesValue = source.DifferenceChargesValue,
 target.FoundInPreviousMonth = source.FoundInPreviousMonth,
 target.DeletedLine = source.DeletedLine,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (UnitsKey, MonthID, SnapShotDate, Coid, RegionKey, PracticeID, CPTID, DifferenceUnitsValue, DifferenceChargesValue, FoundInPreviousMonth, DeletedLine, DWLastUpdateDateTime)
  VALUES (source.UnitsKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.RegionKey, TRIM(source.PracticeID), TRIM(source.CPTID), source.DifferenceUnitsValue, source.DifferenceChargesValue, source.FoundInPreviousMonth, source.DeletedLine, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, UnitsKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotChangesInUnits
      GROUP BY SnapShotDate, UnitsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotChangesInUnits');
ELSE
  COMMIT TRANSACTION;
END IF;
