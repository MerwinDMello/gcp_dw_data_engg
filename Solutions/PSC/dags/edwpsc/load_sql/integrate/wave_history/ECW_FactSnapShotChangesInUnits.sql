
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotChangesInUnits AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotChangesInUnits AS source
ON target.SnapShotDate = source.SnapShotDate AND target.UnitsKey = source.UnitsKey
WHEN MATCHED THEN
  UPDATE SET
  target.UnitsKey = source.UnitsKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.CPTID = source.CPTID,
 target.DifferenceUnitsValue = source.DifferenceUnitsValue,
 target.DifferenceChargesValue = source.DifferenceChargesValue,
 target.FoundInPreviousMonth = source.FoundInPreviousMonth,
 target.DeletedLine = source.DeletedLine,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.ClaimKey = source.ClaimKey,
 target.CPTChargesAmt = source.CPTChargesAmt,
 target.CPTUnits = source.CPTUnits
WHEN NOT MATCHED THEN
  INSERT (UnitsKey, MonthID, SnapShotDate, Coid, RegionKey, CPTID, DifferenceUnitsValue, DifferenceChargesValue, FoundInPreviousMonth, DeletedLine, DWLastUpdateDateTime, ClaimLineChargeKey, ClaimKey, CPTChargesAmt, CPTUnits)
  VALUES (source.UnitsKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.RegionKey, source.CPTID, source.DifferenceUnitsValue, source.DifferenceChargesValue, source.FoundInPreviousMonth, source.DeletedLine, source.DWLastUpdateDateTime, source.ClaimLineChargeKey, source.ClaimKey, source.CPTChargesAmt, source.CPTUnits);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, UnitsKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotChangesInUnits
      GROUP BY SnapShotDate, UnitsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotChangesInUnits');
ELSE
  COMMIT TRANSACTION;
END IF;
