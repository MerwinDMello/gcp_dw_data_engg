
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactNonProductivty AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactNonProductivty AS source
ON target.CCUNonProductivityKey = source.CCUNonProductivityKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUNonProductivityKey = source.CCUNonProductivityKey,
 target.NonProductivityDate = source.NonProductivityDate,
 target.User34 = TRIM(source.User34),
 target.CoderName = TRIM(source.CoderName),
 target.CoderStatus = TRIM(source.CoderStatus),
 target.ChargesEntered = source.ChargesEntered,
 target.TotalDenials = source.TotalDenials,
 target.TotalClaims = source.TotalClaims,
 target.TotalCPTCodes = source.TotalCPTCodes,
 target.EncounterStatusChange = source.EncounterStatusChange,
 target.ProductivityNotes = TRIM(source.ProductivityNotes),
 target.SystemOutages = source.SystemOutages,
 target.SystemOutageDescriptions = TRIM(source.SystemOutageDescriptions),
 target.ITRequestNumber = TRIM(source.ITRequestNumber),
 target.Meetings = source.Meetings,
 target.MeetingsDescriptions = TRIM(source.MeetingsDescriptions),
 target.Education = source.Education,
 target.EducationDescriptions = TRIM(source.EducationDescriptions),
 target.ChargeEntry = source.ChargeEntry,
 target.SpecialProjects = source.SpecialProjects,
 target.DFTInterfaceChargeEntry = source.DFTInterfaceChargeEntry,
 target.WaitingOnCharges = source.WaitingOnCharges,
 target.MeetingsNonConsolidated = source.MeetingsNonConsolidated,
 target.MeetingsNonConsolidatedDescriptions = TRIM(source.MeetingsNonConsolidatedDescriptions),
 target.AssistingPracticeFrontOfficeTask = source.AssistingPracticeFrontOfficeTask,
 target.AssistingPracticeDescriptions = TRIM(source.AssistingPracticeDescriptions),
 target.RebillPracticeDenials = source.RebillPracticeDenials,
 target.FormulaNonProductivity = source.FormulaNonProductivity,
 target.NonProductionTimeNotes = TRIM(source.NonProductionTimeNotes),
 target.CreatedDate = source.CreatedDate,
 target.CreatedBy = TRIM(source.CreatedBy),
 target.ModifiedDate = source.ModifiedDate,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (CCUNonProductivityKey, NonProductivityDate, User34, CoderName, CoderStatus, ChargesEntered, TotalDenials, TotalClaims, TotalCPTCodes, EncounterStatusChange, ProductivityNotes, SystemOutages, SystemOutageDescriptions, ITRequestNumber, Meetings, MeetingsDescriptions, Education, EducationDescriptions, ChargeEntry, SpecialProjects, DFTInterfaceChargeEntry, WaitingOnCharges, MeetingsNonConsolidated, MeetingsNonConsolidatedDescriptions, AssistingPracticeFrontOfficeTask, AssistingPracticeDescriptions, RebillPracticeDenials, FormulaNonProductivity, NonProductionTimeNotes, CreatedDate, CreatedBy, ModifiedDate, SourceSystem, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.CCUNonProductivityKey, source.NonProductivityDate, TRIM(source.User34), TRIM(source.CoderName), TRIM(source.CoderStatus), source.ChargesEntered, source.TotalDenials, source.TotalClaims, source.TotalCPTCodes, source.EncounterStatusChange, TRIM(source.ProductivityNotes), source.SystemOutages, TRIM(source.SystemOutageDescriptions), TRIM(source.ITRequestNumber), source.Meetings, TRIM(source.MeetingsDescriptions), source.Education, TRIM(source.EducationDescriptions), source.ChargeEntry, source.SpecialProjects, source.DFTInterfaceChargeEntry, source.WaitingOnCharges, source.MeetingsNonConsolidated, TRIM(source.MeetingsNonConsolidatedDescriptions), source.AssistingPracticeFrontOfficeTask, TRIM(source.AssistingPracticeDescriptions), source.RebillPracticeDenials, source.FormulaNonProductivity, TRIM(source.NonProductionTimeNotes), source.CreatedDate, TRIM(source.CreatedBy), source.ModifiedDate, TRIM(source.SourceSystem), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUNonProductivityKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactNonProductivty
      GROUP BY CCUNonProductivityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactNonProductivty');
ELSE
  COMMIT TRANSACTION;
END IF;
