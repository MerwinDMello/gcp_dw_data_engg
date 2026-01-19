
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefJobHR AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefJobHR AS source
ON target.JobHRKey = source.JobHRKey
WHEN MATCHED THEN
  UPDATE SET
  target.JobHRKey = source.JobHRKey,
 target.JobCodeHR = TRIM(source.JobCodeHR),
 target.JobDescriptionHR = TRIM(source.JobDescriptionHR),
 target.JobClassCodeHR = TRIM(source.JobClassCodeHR),
 target.JobClassDescriptionHR = TRIM(source.JobClassDescriptionHR),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (JobHRKey, JobCodeHR, JobDescriptionHR, JobClassCodeHR, JobClassDescriptionHR, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.JobHRKey, TRIM(source.JobCodeHR), TRIM(source.JobDescriptionHR), TRIM(source.JobClassCodeHR), TRIM(source.JobClassDescriptionHR), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JobHRKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefJobHR
      GROUP BY JobHRKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefJobHR');
ELSE
  COMMIT TRANSACTION;
END IF;
