
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.3M_FactUserActivity AS target
USING {{ params.param_psc_stage_dataset_name }}.3M_FactUserActivity AS source
ON target.UserActivityKey = source.UserActivityKey
WHEN MATCHED THEN
  UPDATE SET
  target.UserActivityKey = source.UserActivityKey,
 target.CacAccountNumber = TRIM(source.CacAccountNumber),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.FacilityId = TRIM(source.FacilityId),
 target.ActivityDate = source.ActivityDate,
 target.HoldReason = TRIM(source.HoldReason),
 target.CodingStatus = TRIM(source.CodingStatus),
 target.UserName = TRIM(source.UserName),
 target.UserId = TRIM(source.UserId),
 target.COMMENT = TRIM(source.COMMENT),
 target.WorkListName = TRIM(source.WorkListName),
 target.FileName = TRIM(source.FileName),
 target.FileDate = TRIM(source.FileDate),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (UserActivityKey, CacAccountNumber, VisitNumber, FacilityId, ActivityDate, HoldReason, CodingStatus, UserName, UserId, COMMENT, WorkListName, FileName, FileDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.UserActivityKey, TRIM(source.CacAccountNumber), TRIM(source.VisitNumber), TRIM(source.FacilityId), source.ActivityDate, TRIM(source.HoldReason), TRIM(source.CodingStatus), TRIM(source.UserName), TRIM(source.UserId), TRIM(source.COMMENT), TRIM(source.WorkListName), TRIM(source.FileName), TRIM(source.FileDate), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UserActivityKey
      FROM {{ params.param_psc_core_dataset_name }}.3M_FactUserActivity
      GROUP BY UserActivityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.3M_FactUserActivity');
ELSE
  COMMIT TRANSACTION;
END IF;
