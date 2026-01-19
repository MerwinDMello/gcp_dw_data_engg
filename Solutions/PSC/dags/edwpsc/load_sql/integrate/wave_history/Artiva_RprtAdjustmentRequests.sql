
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_RprtAdjustmentRequests AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_RprtAdjustmentRequests AS source
ON target.SnapShotDate = source.SnapShotDate AND target.AdjustmentRequestID = source.AdjustmentRequestID
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.AdjustmentRequestID = source.AdjustmentRequestID,
 target.AdjustmentReqStatus = TRIM(source.AdjustmentReqStatus),
 target.ClaimNumber = TRIM(source.ClaimNumber),
 target.EncounterID = source.EncounterID,
 target.AdjustmentType = TRIM(source.AdjustmentType),
 target.AdjustmentCode = TRIM(source.AdjustmentCode),
 target.AdjustmentDescription = TRIM(source.AdjustmentDescription),
 target.AdjustmentReqDate = source.AdjustmentReqDate,
 target.AdjustmentReqExportDate = source.AdjustmentReqExportDate,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTAdjustmentAmt = source.CPTAdjustmentAmt,
 target.ClaimAdjustmentAmt = source.ClaimAdjustmentAmt,
 target.RequestorID = TRIM(source.RequestorID),
 target.RequestorDept = TRIM(source.RequestorDept),
 target.AdjustmentReqUserName = TRIM(source.AdjustmentReqUserName),
 target.Level2ApprDate = source.Level2ApprDate,
 target.Level2ApprUserID = TRIM(source.Level2ApprUserID),
 target.Level2ApproverName = TRIM(source.Level2ApproverName),
 target.Level3ApprDate = source.Level3ApprDate,
 target.Level3ApprUserID = TRIM(source.Level3ApprUserID),
 target.Level3ApproverName = TRIM(source.Level3ApproverName),
 target.Level4ApprDate = source.Level4ApprDate,
 target.Level4ApprUserID = TRIM(source.Level4ApprUserID),
 target.Level4ApproverName = TRIM(source.Level4ApproverName),
 target.Level5ApprDate = source.Level5ApprDate,
 target.Level5ApprUserID = TRIM(source.Level5ApprUserID),
 target.Level5ApproverName = TRIM(source.Level5ApproverName),
 target.Level6ApprDate = source.Level6ApprDate,
 target.Level6ApprUserID = TRIM(source.Level6ApprUserID),
 target.Level6ApproverName = TRIM(source.Level6ApproverName),
 target.FinalApprovalLevel = source.FinalApprovalLevel,
 target.TotalDaystoExport = source.TotalDaystoExport,
 target.TotalDaysTo2ndLevel = source.TotalDaysTo2ndLevel,
 target.TotalDaysTo3rdLevel = source.TotalDaysTo3rdLevel,
 target.TotalDaysTo4thLevel = source.TotalDaysTo4thLevel,
 target.TotalDaysTo5thLevel = source.TotalDaysTo5thLevel,
 target.TotalDaysTo6thLevel = source.TotalDaysTo6thLevel,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, AdjustmentRequestID, AdjustmentReqStatus, ClaimNumber, EncounterID, AdjustmentType, AdjustmentCode, AdjustmentDescription, AdjustmentReqDate, AdjustmentReqExportDate, CPTCode, CPTAdjustmentAmt, ClaimAdjustmentAmt, RequestorID, RequestorDept, AdjustmentReqUserName, Level2ApprDate, Level2ApprUserID, Level2ApproverName, Level3ApprDate, Level3ApprUserID, Level3ApproverName, Level4ApprDate, Level4ApprUserID, Level4ApproverName, Level5ApprDate, Level5ApprUserID, Level5ApproverName, Level6ApprDate, Level6ApprUserID, Level6ApproverName, FinalApprovalLevel, TotalDaystoExport, TotalDaysTo2ndLevel, TotalDaysTo3rdLevel, TotalDaysTo4thLevel, TotalDaysTo5thLevel, TotalDaysTo6thLevel, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, source.AdjustmentRequestID, TRIM(source.AdjustmentReqStatus), TRIM(source.ClaimNumber), source.EncounterID, TRIM(source.AdjustmentType), TRIM(source.AdjustmentCode), TRIM(source.AdjustmentDescription), source.AdjustmentReqDate, source.AdjustmentReqExportDate, TRIM(source.CPTCode), source.CPTAdjustmentAmt, source.ClaimAdjustmentAmt, TRIM(source.RequestorID), TRIM(source.RequestorDept), TRIM(source.AdjustmentReqUserName), source.Level2ApprDate, TRIM(source.Level2ApprUserID), TRIM(source.Level2ApproverName), source.Level3ApprDate, TRIM(source.Level3ApprUserID), TRIM(source.Level3ApproverName), source.Level4ApprDate, TRIM(source.Level4ApprUserID), TRIM(source.Level4ApproverName), source.Level5ApprDate, TRIM(source.Level5ApprUserID), TRIM(source.Level5ApproverName), source.Level6ApprDate, TRIM(source.Level6ApprUserID), TRIM(source.Level6ApproverName), source.FinalApprovalLevel, source.TotalDaystoExport, source.TotalDaysTo2ndLevel, source.TotalDaysTo3rdLevel, source.TotalDaysTo4thLevel, source.TotalDaysTo5thLevel, source.TotalDaysTo6thLevel, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, AdjustmentRequestID
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_RprtAdjustmentRequests
      GROUP BY SnapShotDate, AdjustmentRequestID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_RprtAdjustmentRequests');
ELSE
  COMMIT TRANSACTION;
END IF;
