
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2QuestionsResponses AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactIETV2QuestionsResponses AS source
ON target.IETV2QuestionResponseKey = source.IETV2QuestionResponseKey
WHEN MATCHED THEN
  UPDATE SET
  target.IETV2QuestionResponseKey = source.IETV2QuestionResponseKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.QuestionNumber = source.QuestionNumber,
 target.Question = TRIM(source.Question),
 target.QuestionDateKey = source.QuestionDateKey,
 target.QuestionTime = source.QuestionTime,
 target.QuestionUserId = TRIM(source.QuestionUserId),
 target.QuestionCorrespondenceId = source.QuestionCorrespondenceId,
 target.Response = TRIM(source.Response),
 target.ResponseDateKey = source.ResponseDateKey,
 target.ResponseTime = source.ResponseTime,
 target.ResponseUserId = TRIM(source.ResponseUserId),
 target.ResponseCorrespondenceId = source.ResponseCorrespondenceId,
 target.DaysToRespond = source.DaysToRespond,
 target.Resolution = TRIM(source.Resolution),
 target.ResolutionId = TRIM(source.ResolutionId),
 target.ResolutionDateKey = source.ResolutionDateKey,
 target.ResolutionTime = source.ResolutionTime,
 target.ResolutionUserId = TRIM(source.ResolutionUserId),
 target.ClaimCaseId = TRIM(source.ClaimCaseId),
 target.CorrespondenceSubjectId = TRIM(source.CorrespondenceSubjectId),
 target.LastQuestionFlag = source.LastQuestionFlag,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (IETV2QuestionResponseKey, ClaimKey, ClaimNumber, QuestionNumber, Question, QuestionDateKey, QuestionTime, QuestionUserId, QuestionCorrespondenceId, Response, ResponseDateKey, ResponseTime, ResponseUserId, ResponseCorrespondenceId, DaysToRespond, Resolution, ResolutionId, ResolutionDateKey, ResolutionTime, ResolutionUserId, ClaimCaseId, CorrespondenceSubjectId, LastQuestionFlag, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.IETV2QuestionResponseKey, source.ClaimKey, source.ClaimNumber, source.QuestionNumber, TRIM(source.Question), source.QuestionDateKey, source.QuestionTime, TRIM(source.QuestionUserId), source.QuestionCorrespondenceId, TRIM(source.Response), source.ResponseDateKey, source.ResponseTime, TRIM(source.ResponseUserId), source.ResponseCorrespondenceId, source.DaysToRespond, TRIM(source.Resolution), TRIM(source.ResolutionId), source.ResolutionDateKey, source.ResolutionTime, TRIM(source.ResolutionUserId), TRIM(source.ClaimCaseId), TRIM(source.CorrespondenceSubjectId), source.LastQuestionFlag, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IETV2QuestionResponseKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2QuestionsResponses
      GROUP BY IETV2QuestionResponseKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2QuestionsResponses');
ELSE
  COMMIT TRANSACTION;
END IF;
