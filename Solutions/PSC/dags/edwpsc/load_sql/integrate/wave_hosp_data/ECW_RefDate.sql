
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefDate AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefDate AS source
ON target.DateKey = source.DateKey
WHEN MATCHED THEN
  UPDATE SET
  target.DateKey = source.DateKey,
 target.WeekId = source.WeekId,
 target.WeekNum = source.WeekNum,
 target.WeekDayFlag = source.WeekDayFlag,
 target.WeekOfMonth = source.WeekOfMonth,
 target.WeekEndDate = source.WeekEndDate,
 target.WeekEndFlag = source.WeekEndFlag,
 target.WeekDaysLeft = source.WeekDaysLeft,
 target.MonthId = source.MonthId,
 target.MonthIdDescS = TRIM(source.MonthIdDescS),
 target.MonthIdDescL = TRIM(source.MonthIdDescL),
 target.MonthNum = source.MonthNum,
 target.MonthNumDescS = TRIM(source.MonthNumDescS),
 target.MonthNumDescL = TRIM(source.MonthNumDescL),
 target.MonthDaysLeft = source.MonthDaysLeft,
 target.QtrId = source.QtrId,
 target.QtrDesc = TRIM(source.QtrDesc),
 target.QtrNum = source.QtrNum,
 target.QtrNumDescS = TRIM(source.QtrNumDescS),
 target.QtrNumDescL = TRIM(source.QtrNumDescL),
 target.SemiAnnualId = source.SemiAnnualId,
 target.SemiAnnualDesc = TRIM(source.SemiAnnualDesc),
 target.YearId = source.YearId,
 target.DayOfWeek = source.DayOfWeek,
 target.DowDescS = TRIM(source.DowDescS),
 target.DowDescL = TRIM(source.DowDescL),
 target.DayOfMonth = source.DayOfMonth,
 target.DayOfQtr = source.DayOfQtr,
 target.DayOfYear = source.DayOfYear,
 target.DaysInWeek = source.DaysInWeek,
 target.DaysInMonth = source.DaysInMonth,
 target.DaysInQtr = source.DaysInQtr,
 target.DaysInYear = source.DaysInYear,
 target.QtrDaysLeft = source.QtrDaysLeft,
 target.YearDaysLeft = source.YearDaysLeft,
 target.BankDayFlag = source.BankDayFlag,
 target.FederalBankHolidayFlag = source.FederalBankHolidayFlag,
 target.HcaHolidayFlag = source.HcaHolidayFlag,
 target.PeDate = source.PeDate,
 target.PeDatePriorMonth = source.PeDatePriorMonth,
 target.QtrDescDSS = TRIM(source.QtrDescDSS),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.WeekInMonth = source.WeekInMonth,
 target.WeekInYear = source.WeekInYear,
 target.BankDays = source.BankDays,
 target.EndOfMonth = source.EndOfMonth,
 target.CBOBusinessDays = source.CBOBusinessDays,
 target.PCWeekDesc = TRIM(source.PCWeekDesc),
 target.PCMonthID = source.PCMonthID,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (DateKey, WeekId, WeekNum, WeekDayFlag, WeekOfMonth, WeekEndDate, WeekEndFlag, WeekDaysLeft, MonthId, MonthIdDescS, MonthIdDescL, MonthNum, MonthNumDescS, MonthNumDescL, MonthDaysLeft, QtrId, QtrDesc, QtrNum, QtrNumDescS, QtrNumDescL, SemiAnnualId, SemiAnnualDesc, YearId, DayOfWeek, DowDescS, DowDescL, DayOfMonth, DayOfQtr, DayOfYear, DaysInWeek, DaysInMonth, DaysInQtr, DaysInYear, QtrDaysLeft, YearDaysLeft, BankDayFlag, FederalBankHolidayFlag, HcaHolidayFlag, PeDate, PeDatePriorMonth, QtrDescDSS, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, WeekInMonth, WeekInYear, BankDays, EndOfMonth, CBOBusinessDays, PCWeekDesc, PCMonthID, DWLastUpdateDateTime)
  VALUES (source.DateKey, source.WeekId, source.WeekNum, source.WeekDayFlag, source.WeekOfMonth, source.WeekEndDate, source.WeekEndFlag, source.WeekDaysLeft, source.MonthId, TRIM(source.MonthIdDescS), TRIM(source.MonthIdDescL), source.MonthNum, TRIM(source.MonthNumDescS), TRIM(source.MonthNumDescL), source.MonthDaysLeft, source.QtrId, TRIM(source.QtrDesc), source.QtrNum, TRIM(source.QtrNumDescS), TRIM(source.QtrNumDescL), source.SemiAnnualId, TRIM(source.SemiAnnualDesc), source.YearId, source.DayOfWeek, TRIM(source.DowDescS), TRIM(source.DowDescL), source.DayOfMonth, source.DayOfQtr, source.DayOfYear, source.DaysInWeek, source.DaysInMonth, source.DaysInQtr, source.DaysInYear, source.QtrDaysLeft, source.YearDaysLeft, source.BankDayFlag, source.FederalBankHolidayFlag, source.HcaHolidayFlag, source.PeDate, source.PeDatePriorMonth, TRIM(source.QtrDescDSS), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.WeekInMonth, source.WeekInYear, source.BankDays, source.EndOfMonth, source.CBOBusinessDays, TRIM(source.PCWeekDesc), source.PCMonthID, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DateKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefDate
      GROUP BY DateKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefDate');
ELSE
  COMMIT TRANSACTION;
END IF;
