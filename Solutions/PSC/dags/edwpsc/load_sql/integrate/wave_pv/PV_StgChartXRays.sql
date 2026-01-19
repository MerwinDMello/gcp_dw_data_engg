
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgChartXRays AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgChartXRays AS source
ON target.ChartXrayPK = source.ChartXrayPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.ChartXrayPK = TRIM(source.ChartXrayPK),
 target.ChartXrayPK_txt = TRIM(source.ChartXrayPK_txt),
 target.ChartPK = TRIM(source.ChartPK),
 target.ChartPK_txt = TRIM(source.ChartPK_txt),
 target.Description = TRIM(source.Description),
 target.Status = source.Status,
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.Modifiers = TRIM(source.Modifiers),
 target.QuestionID = source.QuestionID,
 target.QuestionXML = TRIM(source.QuestionXML),
 target.TranscriptionText = TRIM(source.TranscriptionText),
 target.CompletedBy = TRIM(source.CompletedBy),
 target.CompletedOn = source.CompletedOn,
 target.CompletedByUserPK = TRIM(source.CompletedByUserPK),
 target.ReviewedBy = TRIM(source.ReviewedBy),
 target.ReviewedOn = source.ReviewedOn,
 target.ReviewedByUserPK = TRIM(source.ReviewedByUserPK),
 target.SuppressedOn = source.SuppressedOn,
 target.SuppressedBy = TRIM(source.SuppressedBy),
 target.CreatedBy = TRIM(source.CreatedBy),
 target.CreatedOn = source.CreatedOn,
 target.LastUpdatedBy = TRIM(source.LastUpdatedBy),
 target.LastUpdatedOn = source.LastUpdatedOn,
 target.LastUpdatedByUserPK = TRIM(source.LastUpdatedByUserPK),
 target.OrderedByPhysician = TRIM(source.OrderedByPhysician),
 target.OrderedByPhysicianOn = source.OrderedByPhysicianOn,
 target.OrderedByUserPK = TRIM(source.OrderedByUserPK),
 target.ReceivedByUserPK = TRIM(source.ReceivedByUserPK),
 target.RadiologyImagePath = TRIM(source.RadiologyImagePath),
 target.RadiologyShielding = source.RadiologyShielding,
 target.RadiologyTransport = source.RadiologyTransport,
 target.RadiologyTransportOther = TRIM(source.RadiologyTransportOther),
 target.DateTimeDeleted = source.DateTimeDeleted,
 target.DateSent = source.DateSent,
 target.FacilityType = source.FacilityType,
 target.PlacerOrderNumber = TRIM(source.PlacerOrderNumber),
 target.ProviderNote = TRIM(source.ProviderNote),
 target.RadiologyOverReadReviewedOn = source.RadiologyOverReadReviewedOn,
 target.RadiologyOverReadReviewedBy = TRIM(source.RadiologyOverReadReviewedBy),
 target.RadiologyOverReadExpectExternalResults = source.RadiologyOverReadExpectExternalResults,
 target.ProviderNoteAddedBy = TRIM(source.ProviderNoteAddedBy),
 target.ProviderNoteAddedOn = source.ProviderNoteAddedOn,
 target.SentBy = TRIM(source.SentBy),
 target.SeeAttachedDocument = source.SeeAttachedDocument,
 target.PhysicianPK = TRIM(source.PhysicianPK),
 target.NonProviderOrdered = source.NonProviderOrdered,
 target.PhysicianDescription = TRIM(source.PhysicianDescription),
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
  INSERT (ChartXrayPK, ChartXrayPK_txt, ChartPK, ChartPK_txt, Description, Status, ProcedureCode, Modifiers, QuestionID, QuestionXML, TranscriptionText, CompletedBy, CompletedOn, CompletedByUserPK, ReviewedBy, ReviewedOn, ReviewedByUserPK, SuppressedOn, SuppressedBy, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn, LastUpdatedByUserPK, OrderedByPhysician, OrderedByPhysicianOn, OrderedByUserPK, ReceivedByUserPK, RadiologyImagePath, RadiologyShielding, RadiologyTransport, RadiologyTransportOther, DateTimeDeleted, DateSent, FacilityType, PlacerOrderNumber, ProviderNote, RadiologyOverReadReviewedOn, RadiologyOverReadReviewedBy, RadiologyOverReadExpectExternalResults, ProviderNoteAddedBy, ProviderNoteAddedOn, SentBy, SeeAttachedDocument, PhysicianPK, NonProviderOrdered, PhysicianDescription, RegionKey, TS, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.ChartXrayPK), TRIM(source.ChartXrayPK_txt), TRIM(source.ChartPK), TRIM(source.ChartPK_txt), TRIM(source.Description), source.Status, TRIM(source.ProcedureCode), TRIM(source.Modifiers), source.QuestionID, TRIM(source.QuestionXML), TRIM(source.TranscriptionText), TRIM(source.CompletedBy), source.CompletedOn, TRIM(source.CompletedByUserPK), TRIM(source.ReviewedBy), source.ReviewedOn, TRIM(source.ReviewedByUserPK), source.SuppressedOn, TRIM(source.SuppressedBy), TRIM(source.CreatedBy), source.CreatedOn, TRIM(source.LastUpdatedBy), source.LastUpdatedOn, TRIM(source.LastUpdatedByUserPK), TRIM(source.OrderedByPhysician), source.OrderedByPhysicianOn, TRIM(source.OrderedByUserPK), TRIM(source.ReceivedByUserPK), TRIM(source.RadiologyImagePath), source.RadiologyShielding, source.RadiologyTransport, TRIM(source.RadiologyTransportOther), source.DateTimeDeleted, source.DateSent, source.FacilityType, TRIM(source.PlacerOrderNumber), TRIM(source.ProviderNote), source.RadiologyOverReadReviewedOn, TRIM(source.RadiologyOverReadReviewedBy), source.RadiologyOverReadExpectExternalResults, TRIM(source.ProviderNoteAddedBy), source.ProviderNoteAddedOn, TRIM(source.SentBy), source.SeeAttachedDocument, TRIM(source.PhysicianPK), source.NonProviderOrdered, TRIM(source.PhysicianDescription), source.RegionKey, source.TS, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ChartXrayPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgChartXRays
      GROUP BY ChartXrayPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgChartXRays');
ELSE
  COMMIT TRANSACTION;
END IF;
