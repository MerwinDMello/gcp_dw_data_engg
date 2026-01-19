
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedAxionWorkQueue AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterMedAxionWorkQueue AS source
ON target.DateOfService = source.DateOfService AND target.CaseNumberFin = source.CaseNumberFin AND target.PatientNumberMRN = source.PatientNumberMRN AND target.FileName = source.FileName AND target.FileDate = source.FileDate
WHEN MATCHED THEN
  UPDATE SET
  target.DateOfService = source.DateOfService,
 target.DaysSinceDateOfService = source.DaysSinceDateOfService,
 target.CurrentQueueEntryDate = source.CurrentQueueEntryDate,
 target.DaysInCurrentQueue = source.DaysInCurrentQueue,
 target.CaseStatus = TRIM(source.CaseStatus),
 target.CaseNumberFIN = TRIM(source.CaseNumberFIN),
 target.PatientNumberMRN = TRIM(source.PatientNumberMRN),
 target.PatientFirst3 = TRIM(source.PatientFirst3),
 target.PatientLast3 = TRIM(source.PatientLast3),
 target.PatientDOB = source.PatientDOB,
 target.ProviderName = TRIM(source.ProviderName),
 target.SupervisorName = TRIM(source.SupervisorName),
 target.Payer = TRIM(source.Payer),
 target.LOCATION = TRIM(source.LOCATION),
 target.WorkQueueCategory = TRIM(source.WorkQueueCategory),
 target.Filename = TRIM(source.Filename),
 target.FileDate = source.FileDate,
 target.Coid = TRIM(source.Coid),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (DateOfService, DaysSinceDateOfService, CurrentQueueEntryDate, DaysInCurrentQueue, CaseStatus, CaseNumberFIN, PatientNumberMRN, PatientFirst3, PatientLast3, PatientDOB, ProviderName, SupervisorName, Payer, LOCATION, WorkQueueCategory, Filename, FileDate, Coid, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.DateOfService, source.DaysSinceDateOfService, source.CurrentQueueEntryDate, source.DaysInCurrentQueue, TRIM(source.CaseStatus), TRIM(source.CaseNumberFIN), TRIM(source.PatientNumberMRN), TRIM(source.PatientFirst3), TRIM(source.PatientLast3), source.PatientDOB, TRIM(source.ProviderName), TRIM(source.SupervisorName), TRIM(source.Payer), TRIM(source.LOCATION), TRIM(source.WorkQueueCategory), TRIM(source.Filename), source.FileDate, TRIM(source.Coid), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DateOfService, CaseNumberFin, PatientNumberMRN, FileName, FileDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedAxionWorkQueue
      GROUP BY DateOfService, CaseNumberFin, PatientNumberMRN, FileName, FileDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedAxionWorkQueue');
ELSE
  COMMIT TRANSACTION;
END IF;
