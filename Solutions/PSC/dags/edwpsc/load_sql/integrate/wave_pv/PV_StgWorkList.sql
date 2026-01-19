
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgWorkList AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgWorkList AS source
ON target.WorkListPK = source.WorkListPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.WorkList = TRIM(source.WorkList),
 target.AssignedTo = TRIM(source.AssignedTo),
 target.WorkListType = TRIM(source.WorkListType),
 target.Priority = source.Priority,
 target.CreatedBy = TRIM(source.CreatedBy),
 target.CreatedOn = source.CreatedOn,
 target.RefreshedCount = source.RefreshedCount,
 target.WorkListPK = TRIM(source.WorkListPK),
 target.ProcessFrequency = source.ProcessFrequency,
 target.Clinic = TRIM(source.Clinic),
 target.VisitType = TRIM(source.VisitType),
 target.PayerType = TRIM(source.PayerType),
 target.PayerClass = TRIM(source.PayerClass),
 target.BalanceFrom = source.BalanceFrom,
 target.BalanceTo = source.BalanceTo,
 target.DaysFrom = source.DaysFrom,
 target.DaysTo = source.DaysTo,
 target.PatientFrom = TRIM(source.PatientFrom),
 target.PatientTo = TRIM(source.PatientTo),
 target.PayerFrom = TRIM(source.PayerFrom),
 target.PayerTo = TRIM(source.PayerTo),
 target.IsUnassignedWorklist = source.IsUnassignedWorklist,
 target.RefreshedAmount = source.RefreshedAmount,
 target.RefreshedOn = source.RefreshedOn,
 target.RefreshedBy = TRIM(source.RefreshedBy),
 target.LastUpdatedOn = source.LastUpdatedOn,
 target.LastUpdatedCount = source.LastUpdatedCount,
 target.LastUpdatedAmount = source.LastUpdatedAmount,
 target.TotalProcessingTimeMs = source.TotalProcessingTimeMs,
 target.NextRefreshDate = source.NextRefreshDate,
 target.RegionKey = source.RegionKey,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, WorkList, AssignedTo, WorkListType, Priority, CreatedBy, CreatedOn, RefreshedCount, WorkListPK, ProcessFrequency, Clinic, VisitType, PayerType, PayerClass, BalanceFrom, BalanceTo, DaysFrom, DaysTo, PatientFrom, PatientTo, PayerFrom, PayerTo, IsUnassignedWorklist, RefreshedAmount, RefreshedOn, RefreshedBy, LastUpdatedOn, LastUpdatedCount, LastUpdatedAmount, TotalProcessingTimeMs, NextRefreshDate, RegionKey, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), TRIM(source.WorkList), TRIM(source.AssignedTo), TRIM(source.WorkListType), source.Priority, TRIM(source.CreatedBy), source.CreatedOn, source.RefreshedCount, TRIM(source.WorkListPK), source.ProcessFrequency, TRIM(source.Clinic), TRIM(source.VisitType), TRIM(source.PayerType), TRIM(source.PayerClass), source.BalanceFrom, source.BalanceTo, source.DaysFrom, source.DaysTo, TRIM(source.PatientFrom), TRIM(source.PatientTo), TRIM(source.PayerFrom), TRIM(source.PayerTo), source.IsUnassignedWorklist, source.RefreshedAmount, source.RefreshedOn, TRIM(source.RefreshedBy), source.LastUpdatedOn, source.LastUpdatedCount, source.LastUpdatedAmount, source.TotalProcessingTimeMs, source.NextRefreshDate, source.RegionKey, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT WorkListPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgWorkList
      GROUP BY WorkListPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgWorkList');
ELSE
  COMMIT TRANSACTION;
END IF;
