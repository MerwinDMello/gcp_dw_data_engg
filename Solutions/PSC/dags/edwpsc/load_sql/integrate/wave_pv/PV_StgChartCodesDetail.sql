
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgChartCodesDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgChartCodesDetail AS source
ON target.ChartCodesDetailPK = source.ChartCodesDetailPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.ChartCodesDetailPK = TRIM(source.ChartCodesDetailPK),
 target.ChartCodesDetailPK_txt = TRIM(source.ChartCodesDetailPK_txt),
 target.ChartCodesHeaderPK = TRIM(source.ChartCodesHeaderPK),
 target.ChartCodesHeaderPK_txt = TRIM(source.ChartCodesHeaderPK_txt),
 target.CodeTypeID = source.CodeTypeID,
 target.Code = TRIM(source.Code),
 target.CodeDescription = TRIM(source.CodeDescription),
 target.Modifiers = TRIM(source.Modifiers),
 target.Quantity = source.Quantity,
 target.CodeQuestionXML = TRIM(source.CodeQuestionXML),
 target.TransscriptionText = TRIM(source.TransscriptionText),
 target.Status = source.Status,
 target.ReviewRequired = source.ReviewRequired,
 target.CompletedOn = source.CompletedOn,
 target.ReviewedOn = source.ReviewedOn,
 target.CodeQuestionID = source.CodeQuestionID,
 target.CompletedBy = TRIM(source.CompletedBy),
 target.ReviewedBy = TRIM(source.ReviewedBy),
 target.CreatedBy = TRIM(source.CreatedBy),
 target.CreatedOn = source.CreatedOn,
 target.LastUpdatedBy = TRIM(source.LastUpdatedBy),
 target.LastUpdatedOn = source.LastUpdatedOn,
 target.QuestionId = source.QuestionId,
 target.AnswerXML = TRIM(source.AnswerXML),
 target.RadiologyImagePath = TRIM(source.RadiologyImagePath),
 target.RadiologyShielding = source.RadiologyShielding,
 target.RadiologyTransport = source.RadiologyTransport,
 target.RadiologyTransportOther = TRIM(source.RadiologyTransportOther),
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
  INSERT (ChartCodesDetailPK, ChartCodesDetailPK_txt, ChartCodesHeaderPK, ChartCodesHeaderPK_txt, CodeTypeID, Code, CodeDescription, Modifiers, Quantity, CodeQuestionXML, TransscriptionText, Status, ReviewRequired, CompletedOn, ReviewedOn, CodeQuestionID, CompletedBy, ReviewedBy, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn, QuestionId, AnswerXML, RadiologyImagePath, RadiologyShielding, RadiologyTransport, RadiologyTransportOther, RegionKey, TS, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.ChartCodesDetailPK), TRIM(source.ChartCodesDetailPK_txt), TRIM(source.ChartCodesHeaderPK), TRIM(source.ChartCodesHeaderPK_txt), source.CodeTypeID, TRIM(source.Code), TRIM(source.CodeDescription), TRIM(source.Modifiers), source.Quantity, TRIM(source.CodeQuestionXML), TRIM(source.TransscriptionText), source.Status, source.ReviewRequired, source.CompletedOn, source.ReviewedOn, source.CodeQuestionID, TRIM(source.CompletedBy), TRIM(source.ReviewedBy), TRIM(source.CreatedBy), source.CreatedOn, TRIM(source.LastUpdatedBy), source.LastUpdatedOn, source.QuestionId, TRIM(source.AnswerXML), TRIM(source.RadiologyImagePath), source.RadiologyShielding, source.RadiologyTransport, TRIM(source.RadiologyTransportOther), source.RegionKey, source.TS, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ChartCodesDetailPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgChartCodesDetail
      GROUP BY ChartCodesDetailPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgChartCodesDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
