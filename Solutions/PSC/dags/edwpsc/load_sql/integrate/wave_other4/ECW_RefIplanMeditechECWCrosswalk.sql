
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechECWCrosswalk AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefIplanMeditechECWCrosswalk AS source
ON target.MeditechECWCrosswalkKey = source.MeditechECWCrosswalkKey
WHEN MATCHED THEN
  UPDATE SET
  target.MeditechECWCrosswalkKey = source.MeditechECWCrosswalkKey,
 target.MeditechIplanID = TRIM(source.MeditechIplanID),
 target.ECWIplanName = TRIM(source.ECWIplanName),
 target.ECWIplanID = source.ECWIplanID,
 target.IsGovernmentFlag = source.IsGovernmentFlag,
 target.IsAvailableInECWFlag = source.IsAvailableInECWFlag,
 target.IsBillableByMidLevelFlag = source.IsBillableByMidLevelFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (MeditechECWCrosswalkKey, MeditechIplanID, ECWIplanName, ECWIplanID, IsGovernmentFlag, IsAvailableInECWFlag, IsBillableByMidLevelFlag, DWLastUpdateDateTime)
  VALUES (source.MeditechECWCrosswalkKey, TRIM(source.MeditechIplanID), TRIM(source.ECWIplanName), source.ECWIplanID, source.IsGovernmentFlag, source.IsAvailableInECWFlag, source.IsBillableByMidLevelFlag, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MeditechECWCrosswalkKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechECWCrosswalk
      GROUP BY MeditechECWCrosswalkKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechECWCrosswalk');
ELSE
  COMMIT TRANSACTION;
END IF;
