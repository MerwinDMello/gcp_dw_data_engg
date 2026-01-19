
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgChartCodesHeader AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgChartCodesHeader AS source
ON target.ChartCodesHeaderPK = source.ChartCodesHeaderPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.ChartCodesHeaderPK = TRIM(source.ChartCodesHeaderPK),
 target.ChartPK = TRIM(source.ChartPK),
 target.CodeTypeID = source.CodeTypeID,
 target.Description = TRIM(source.Description),
 target.DefaultCodes = TRIM(source.DefaultCodes),
 target.QuestionID = source.QuestionID,
 target.QuestionXML = TRIM(source.QuestionXML),
 target.TranscriptionText = TRIM(source.TranscriptionText),
 target.ReviewRequired = source.ReviewRequired,
 target.CompletedBy = TRIM(source.CompletedBy),
 target.CompletedOn = source.CompletedOn,
 target.ReviewedBy = TRIM(source.ReviewedBy),
 target.ReviewedOn = source.ReviewedOn,
 target.SortOrder = source.SortOrder,
 target.CreatedBy = TRIM(source.CreatedBy),
 target.CreatedOn = source.CreatedOn,
 target.LastUpdatedBy = TRIM(source.LastUpdatedBy),
 target.LastUpdatedOn = source.LastUpdatedOn,
 target.OrderedByPhysician = TRIM(source.OrderedByPhysician),
 target.OrderedByPhysicianOn = source.OrderedByPhysicianOn,
 target.SuppressedBy = TRIM(source.SuppressedBy),
 target.SuppressedOn = source.SuppressedOn,
 target.AnswerXML = TRIM(source.AnswerXML),
 target.NDC = TRIM(source.NDC),
 target.OrderedByUserPK = TRIM(source.OrderedByUserPK),
 target.ReceivedByUserPK = TRIM(source.ReceivedByUserPK),
 target.CompletedByUserPK = TRIM(source.CompletedByUserPK),
 target.ReviewedByUserPK = TRIM(source.ReviewedByUserPK),
 target.LastUpdatedByUserPK = TRIM(source.LastUpdatedByUserPK),
 target.RadiologyOverReadReviewedOn = source.RadiologyOverReadReviewedOn,
 target.RadiologyOverReadReviewedBy = TRIM(source.RadiologyOverReadReviewedBy),
 target.PhysicianPK = TRIM(source.PhysicianPK),
 target.NonProviderOrdered = source.NonProviderOrdered,
 target.PhysicianDescription = TRIM(source.PhysicianDescription),
 target.ActionType = source.ActionType,
 target.RegionKey = source.RegionKey,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.ModifiedBy = TRIM(source.ModifiedBy)
WHEN NOT MATCHED THEN
  INSERT (ChartCodesHeaderPK, ChartPK, CodeTypeID, Description, DefaultCodes, QuestionID, QuestionXML, TranscriptionText, ReviewRequired, CompletedBy, CompletedOn, ReviewedBy, ReviewedOn, SortOrder, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn, OrderedByPhysician, OrderedByPhysicianOn, SuppressedBy, SuppressedOn, AnswerXML, NDC, OrderedByUserPK, ReceivedByUserPK, CompletedByUserPK, ReviewedByUserPK, LastUpdatedByUserPK, RadiologyOverReadReviewedOn, RadiologyOverReadReviewedBy, PhysicianPK, NonProviderOrdered, PhysicianDescription, ActionType, RegionKey, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.ChartCodesHeaderPK), TRIM(source.ChartPK), source.CodeTypeID, TRIM(source.Description), TRIM(source.DefaultCodes), source.QuestionID, TRIM(source.QuestionXML), TRIM(source.TranscriptionText), source.ReviewRequired, TRIM(source.CompletedBy), source.CompletedOn, TRIM(source.ReviewedBy), source.ReviewedOn, source.SortOrder, TRIM(source.CreatedBy), source.CreatedOn, TRIM(source.LastUpdatedBy), source.LastUpdatedOn, TRIM(source.OrderedByPhysician), source.OrderedByPhysicianOn, TRIM(source.SuppressedBy), source.SuppressedOn, TRIM(source.AnswerXML), TRIM(source.NDC), TRIM(source.OrderedByUserPK), TRIM(source.ReceivedByUserPK), TRIM(source.CompletedByUserPK), TRIM(source.ReviewedByUserPK), TRIM(source.LastUpdatedByUserPK), source.RadiologyOverReadReviewedOn, TRIM(source.RadiologyOverReadReviewedBy), TRIM(source.PhysicianPK), source.NonProviderOrdered, TRIM(source.PhysicianDescription), source.ActionType, source.RegionKey, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ChartCodesHeaderPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgChartCodesHeader
      GROUP BY ChartCodesHeaderPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgChartCodesHeader');
ELSE
  COMMIT TRANSACTION;
END IF;
