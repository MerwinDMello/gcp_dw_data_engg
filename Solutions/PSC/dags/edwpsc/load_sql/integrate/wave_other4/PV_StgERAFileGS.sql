
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileGS AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileGS AS source
ON target.GS_id = source.GS_id
WHEN MATCHED THEN
  UPDATE SET
  target.GS_id = source.GS_id,
 target.fileid = source.fileid,
 target.GS01 = TRIM(source.GS01),
 target.GS02 = TRIM(source.GS02),
 target.GS03 = TRIM(source.GS03),
 target.GS04 = TRIM(source.GS04),
 target.GS05 = TRIM(source.GS05),
 target.GS06 = TRIM(source.GS06),
 target.GS07 = TRIM(source.GS07),
 target.GS08 = TRIM(source.GS08),
 target.GSSegment = TRIM(source.GSSegment),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (GS_id, fileid, GS01, GS02, GS03, GS04, GS05, GS06, GS07, GS08, GSSegment, InsertedDTM, DWLastUpdateDateTime)
  VALUES (source.GS_id, source.fileid, TRIM(source.GS01), TRIM(source.GS02), TRIM(source.GS03), TRIM(source.GS04), TRIM(source.GS05), TRIM(source.GS06), TRIM(source.GS07), TRIM(source.GS08), TRIM(source.GSSegment), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GS_id
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileGS
      GROUP BY GS_id
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileGS');
ELSE
  COMMIT TRANSACTION;
END IF;
