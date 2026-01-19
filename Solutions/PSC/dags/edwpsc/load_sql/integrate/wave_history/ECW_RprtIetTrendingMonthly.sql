
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtIetTrendingMonthly AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtIetTrendingMonthly AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = TRIM(source.SnapShotDate),
 target.Group = TRIM(source.Group),
 target.Division = TRIM(source.Division),
 target.Market = TRIM(source.Market),
 target.COID = TRIM(source.COID),
 target.CoidName = TRIM(source.CoidName),
 target.PracticeIETs = source.PracticeIETs,
 target.OpenPracticeIETs = source.OpenPracticeIETs,
 target.OpenDaysOfPracticeIETs = source.OpenDaysOfPracticeIETs,
 target.ClosedPracticeIETs = source.ClosedPracticeIETs,
 target.TotalPracticeIETsDaysToResolution = source.TotalPracticeIETsDaysToResolution,
 target.PracticeIETsOpen30PlusDays = source.PracticeIETsOpen30PlusDays,
 target.NewPracticeIETs = source.NewPracticeIETs,
 target.NewOpenPracticeIETs = source.NewOpenPracticeIETs,
 target.NewOpenDaysOfPracticeIETs = source.NewOpenDaysOfPracticeIETs,
 target.NewClosedPracticeIETs = source.NewClosedPracticeIETs,
 target.NewClosedDaysToResolution = source.NewClosedDaysToResolution,
 target.OpenPracticeIETsLastMonth = source.OpenPracticeIETsLastMonth,
 target.OpenPracticeIETsLastMonthClosed30Days = source.OpenPracticeIETsLastMonthClosed30Days,
 target.PracticeIETsClosedMonth = source.PracticeIETsClosedMonth,
 target.PracticeIETsClosedMonthDaysToResolution = source.PracticeIETsClosedMonthDaysToResolution,
 target.NewClaims = source.NewClaims,
 target.NumberOfWeekdays = source.NumberOfWeekdays,
 target.RenderingProviderSpeciality = TRIM(source.RenderingProviderSpeciality),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.LOB = TRIM(source.LOB),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, `Group`, Division, Market, COID, CoidName, PracticeIETs, OpenPracticeIETs, OpenDaysOfPracticeIETs, ClosedPracticeIETs, TotalPracticeIETsDaysToResolution, PracticeIETsOpen30PlusDays, NewPracticeIETs, NewOpenPracticeIETs, NewOpenDaysOfPracticeIETs, NewClosedPracticeIETs, NewClosedDaysToResolution, OpenPracticeIETsLastMonth, OpenPracticeIETsLastMonthClosed30Days, PracticeIETsClosedMonth, PracticeIETsClosedMonthDaysToResolution, NewClaims, NumberOfWeekdays, RenderingProviderSpeciality, InsertedBy, InsertedDTM, LOB, DWLastUpdateDateTime)
  VALUES (TRIM(source.SnapShotDate), TRIM(source.Group), TRIM(source.Division), TRIM(source.Market), TRIM(source.COID), TRIM(source.CoidName), source.PracticeIETs, source.OpenPracticeIETs, source.OpenDaysOfPracticeIETs, source.ClosedPracticeIETs, source.TotalPracticeIETsDaysToResolution, source.PracticeIETsOpen30PlusDays, source.NewPracticeIETs, source.NewOpenPracticeIETs, source.NewOpenDaysOfPracticeIETs, source.NewClosedPracticeIETs, source.NewClosedDaysToResolution, source.OpenPracticeIETsLastMonth, source.OpenPracticeIETsLastMonthClosed30Days, source.PracticeIETsClosedMonth, source.PracticeIETsClosedMonthDaysToResolution, source.NewClaims, source.NumberOfWeekdays, TRIM(source.RenderingProviderSpeciality), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.LOB), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtIetTrendingMonthly
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtIetTrendingMonthly');
ELSE
  COMMIT TRANSACTION;
END IF;
