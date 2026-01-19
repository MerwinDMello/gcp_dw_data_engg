
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgChart AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgChart AS source
ON target.ChartPK = source.ChartPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.ChartPK = TRIM(source.ChartPK),
 target.ChartPK_txt = TRIM(source.ChartPK_txt),
 target.ClinicPK = TRIM(source.ClinicPK),
 target.ClinicPK_txt = TRIM(source.ClinicPK_txt),
 target.PatInfoPk = TRIM(source.PatInfoPk),
 target.PatInfoPk_txt = TRIM(source.PatInfoPk_txt),
 target.LogDetailPK = TRIM(source.LogDetailPK),
 target.LogDetailPK_txt = TRIM(source.LogDetailPK_txt),
 target.ChartNumber = source.ChartNumber,
 target.SectionNone = source.SectionNone,
 target.SectionReviewed = source.SectionReviewed,
 target.Notes = TRIM(source.Notes),
 target.SignOffDate = source.SignOffDate,
 target.SignedOffBy = TRIM(source.SignedOffBy),
 target.EMCode = TRIM(source.EMCode),
 target.EMCodeTime = source.EMCodeTime,
 target.Risk = TRIM(source.Risk),
 target.PatientType = source.PatientType,
 target.CreatedBy = TRIM(source.CreatedBy),
 target.CreatedOn = source.CreatedOn,
 target.LastUpdatedBy = TRIM(source.LastUpdatedBy),
 target.LastUpdatedOn = source.LastUpdatedOn,
 target.SignOffSealedDate = source.SignOffSealedDate,
 target.SignedOffSealedBy = TRIM(source.SignedOffSealedBy),
 target.EMCodeSuggested = TRIM(source.EMCodeSuggested),
 target.EMCodeOverride = source.EMCodeOverride,
 target.Historian = TRIM(source.Historian),
 target.EMCodeModifiers = TRIM(source.EMCodeModifiers),
 target.LANGUAGE = source.LANGUAGE,
 target.SmokingStatus = source.SmokingStatus,
 target.SignOffSealedByPK = TRIM(source.SignOffSealedByPK),
 target.SignOffByPK = TRIM(source.SignOffByPK),
 target.ShowAllTabs = source.ShowAllTabs,
 target.HxNotCompletedReason = source.HxNotCompletedReason,
 target.HxNotCompletedUpdatedBy = TRIM(source.HxNotCompletedUpdatedBy),
 target.HxNotCompletedUpdatedOn = source.HxNotCompletedUpdatedOn,
 target.VisitSummary = TRIM(source.VisitSummary),
 target.SmokingStatusCode = source.SmokingStatusCode,
 target.TransitionOfCareTypeID = source.TransitionOfCareTypeID,
 target.TransitionOfCareOther = TRIM(source.TransitionOfCareOther),
 target.PatientProviderInteractionCode = source.PatientProviderInteractionCode,
 target.TransitionOfCareImportReconciled = source.TransitionOfCareImportReconciled,
 target.PortalDataLastSent = source.PortalDataLastSent,
 target.ImportSource = TRIM(source.ImportSource),
 target.ImportSourceCompletedBy = TRIM(source.ImportSourceCompletedBy),
 target.ImportSourceCompletedOn = source.ImportSourceCompletedOn,
 target.HistoryLevel = source.HistoryLevel,
 target.CC_HPI_Level = source.CC_HPI_Level,
 target.ROS_Level = source.ROS_Level,
 target.PFSH_Level = source.PFSH_Level,
 target.PhysicalLevel = source.PhysicalLevel,
 target.ComplexityLevel = source.ComplexityLevel,
 target.DataReviewLevel = source.DataReviewLevel,
 target.TimeLevel = source.TimeLevel,
 target.DiagnosisLevel = source.DiagnosisLevel,
 target.DisableComplexityThrottle = source.DisableComplexityThrottle,
 target.AssignedProviderPhysicianPK = TRIM(source.AssignedProviderPhysicianPK),
 target.AssignedProviderPhysicianPK_txt = TRIM(source.AssignedProviderPhysicianPK_txt),
 target.AssignedProviderUserProfilePK = TRIM(source.AssignedProviderUserProfilePK),
 target.AssignedProviderName = TRIM(source.AssignedProviderName),
 target.NonProviderDischarge = source.NonProviderDischarge,
 target.NonProviderSignOff = source.NonProviderSignOff,
 target.CoSignRequired = source.CoSignRequired,
 target.CoSignedByPK = TRIM(source.CoSignedByPK),
 target.CoSignDate = source.CoSignDate,
 target.CoSignedBy = TRIM(source.CoSignedBy),
 target.RegionKey = source.RegionKey,
 target.TS = source.TS,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.ModifiedBy = TRIM(source.ModifiedBy)
WHEN NOT MATCHED THEN
  INSERT (ChartPK, ChartPK_txt, ClinicPK, ClinicPK_txt, PatInfoPk, PatInfoPk_txt, LogDetailPK, LogDetailPK_txt, ChartNumber, SectionNone, SectionReviewed, Notes, SignOffDate, SignedOffBy, EMCode, EMCodeTime, Risk, PatientType, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn, SignOffSealedDate, SignedOffSealedBy, EMCodeSuggested, EMCodeOverride, Historian, EMCodeModifiers, LANGUAGE, SmokingStatus, SignOffSealedByPK, SignOffByPK, ShowAllTabs, HxNotCompletedReason, HxNotCompletedUpdatedBy, HxNotCompletedUpdatedOn, VisitSummary, SmokingStatusCode, TransitionOfCareTypeID, TransitionOfCareOther, PatientProviderInteractionCode, TransitionOfCareImportReconciled, PortalDataLastSent, ImportSource, ImportSourceCompletedBy, ImportSourceCompletedOn, HistoryLevel, CC_HPI_Level, ROS_Level, PFSH_Level, PhysicalLevel, ComplexityLevel, DataReviewLevel, TimeLevel, DiagnosisLevel, DisableComplexityThrottle, AssignedProviderPhysicianPK, AssignedProviderPhysicianPK_txt, AssignedProviderUserProfilePK, AssignedProviderName, NonProviderDischarge, NonProviderSignOff, CoSignRequired, CoSignedByPK, CoSignDate, CoSignedBy, RegionKey, TS, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.ChartPK), TRIM(source.ChartPK_txt), TRIM(source.ClinicPK), TRIM(source.ClinicPK_txt), TRIM(source.PatInfoPk), TRIM(source.PatInfoPk_txt), TRIM(source.LogDetailPK), TRIM(source.LogDetailPK_txt), source.ChartNumber, source.SectionNone, source.SectionReviewed, TRIM(source.Notes), source.SignOffDate, TRIM(source.SignedOffBy), TRIM(source.EMCode), source.EMCodeTime, TRIM(source.Risk), source.PatientType, TRIM(source.CreatedBy), source.CreatedOn, TRIM(source.LastUpdatedBy), source.LastUpdatedOn, source.SignOffSealedDate, TRIM(source.SignedOffSealedBy), TRIM(source.EMCodeSuggested), source.EMCodeOverride, TRIM(source.Historian), TRIM(source.EMCodeModifiers), source.LANGUAGE, source.SmokingStatus, TRIM(source.SignOffSealedByPK), TRIM(source.SignOffByPK), source.ShowAllTabs, source.HxNotCompletedReason, TRIM(source.HxNotCompletedUpdatedBy), source.HxNotCompletedUpdatedOn, TRIM(source.VisitSummary), source.SmokingStatusCode, source.TransitionOfCareTypeID, TRIM(source.TransitionOfCareOther), source.PatientProviderInteractionCode, source.TransitionOfCareImportReconciled, source.PortalDataLastSent, TRIM(source.ImportSource), TRIM(source.ImportSourceCompletedBy), source.ImportSourceCompletedOn, source.HistoryLevel, source.CC_HPI_Level, source.ROS_Level, source.PFSH_Level, source.PhysicalLevel, source.ComplexityLevel, source.DataReviewLevel, source.TimeLevel, source.DiagnosisLevel, source.DisableComplexityThrottle, TRIM(source.AssignedProviderPhysicianPK), TRIM(source.AssignedProviderPhysicianPK_txt), TRIM(source.AssignedProviderUserProfilePK), TRIM(source.AssignedProviderName), source.NonProviderDischarge, source.NonProviderSignOff, source.CoSignRequired, TRIM(source.CoSignedByPK), source.CoSignDate, TRIM(source.CoSignedBy), source.RegionKey, source.TS, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ChartPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgChart
      GROUP BY ChartPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgChart');
ELSE
  COMMIT TRANSACTION;
END IF;
