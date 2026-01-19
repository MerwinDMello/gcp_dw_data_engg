
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgLabCodeHeader AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgLabCodeHeader AS source
ON target.LabCodeHeaderPK = source.LabCodeHeaderPK AND target.RegionKey = source.RegionKey AND target.LabRequestId = source.LabRequestId
WHEN MATCHED THEN
  UPDATE SET
  target.LabRequestId = source.LabRequestId,
 target.DefaultCodes = TRIM(source.DefaultCodes),
 target.QuestionXML = TRIM(source.QuestionXML),
 target.TranscriptionText = TRIM(source.TranscriptionText),
 target.CompletedBy = TRIM(source.CompletedBy),
 target.CompletedOn = source.CompletedOn,
 target.CompletedByUserPK = TRIM(source.CompletedByUserPK),
 target.ReviewedBy = TRIM(source.ReviewedBy),
 target.ReviewedOn = source.ReviewedOn,
 target.ReviewedByUserPK = TRIM(source.ReviewedByUserPK),
 target.CreatedBy = TRIM(source.CreatedBy),
 target.CreatedOn = source.CreatedOn,
 target.LastUpdatedBy = TRIM(source.LastUpdatedBy),
 target.LastUpdatedOn = source.LastUpdatedOn,
 target.LastUpdatedByUserPK = TRIM(source.LastUpdatedByUserPK),
 target.OrderedByPhysician = TRIM(source.OrderedByPhysician),
 target.OrderedByPhysicianOn = source.OrderedByPhysicianOn,
 target.OrderedByUserPK = TRIM(source.OrderedByUserPK),
 target.SuppressedBy = TRIM(source.SuppressedBy),
 target.SuppressedOn = source.SuppressedOn,
 target.LabCodeHeaderPK = TRIM(source.LabCodeHeaderPK),
 target.LabCodeHeaderPK_txt = TRIM(source.LabCodeHeaderPK_txt),
 target.PhysicianPK = TRIM(source.PhysicianPK),
 target.NonProviderOrdered = source.NonProviderOrdered,
 target.PhysicianDescription = TRIM(source.PhysicianDescription),
 target.RegionKey = source.RegionKey,
 target.TS = source.TS,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (LabRequestId, DefaultCodes, QuestionXML, TranscriptionText, CompletedBy, CompletedOn, CompletedByUserPK, ReviewedBy, ReviewedOn, ReviewedByUserPK, CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn, LastUpdatedByUserPK, OrderedByPhysician, OrderedByPhysicianOn, OrderedByUserPK, SuppressedBy, SuppressedOn, LabCodeHeaderPK, LabCodeHeaderPK_txt, PhysicianPK, NonProviderOrdered, PhysicianDescription, RegionKey, TS, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (source.LabRequestId, TRIM(source.DefaultCodes), TRIM(source.QuestionXML), TRIM(source.TranscriptionText), TRIM(source.CompletedBy), source.CompletedOn, TRIM(source.CompletedByUserPK), TRIM(source.ReviewedBy), source.ReviewedOn, TRIM(source.ReviewedByUserPK), TRIM(source.CreatedBy), source.CreatedOn, TRIM(source.LastUpdatedBy), source.LastUpdatedOn, TRIM(source.LastUpdatedByUserPK), TRIM(source.OrderedByPhysician), source.OrderedByPhysicianOn, TRIM(source.OrderedByUserPK), TRIM(source.SuppressedBy), source.SuppressedOn, TRIM(source.LabCodeHeaderPK), TRIM(source.LabCodeHeaderPK_txt), TRIM(source.PhysicianPK), source.NonProviderOrdered, TRIM(source.PhysicianDescription), source.RegionKey, source.TS, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LabCodeHeaderPK, RegionKey, LabRequestId
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgLabCodeHeader
      GROUP BY LabCodeHeaderPK, RegionKey, LabRequestId
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgLabCodeHeader');
ELSE
  COMMIT TRANSACTION;
END IF;
