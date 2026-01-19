
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgERAFileName AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgERAFileName AS source
ON target.fileid = source.fileid
WHEN MATCHED THEN
  UPDATE SET
  target.fileid = source.fileid,
 target.filename = TRIM(source.filename),
 target.fullfilename = TRIM(source.fullfilename),
 target.filedate = source.filedate,
 target.ISA06 = TRIM(source.ISA06),
 target.ISA07 = TRIM(source.ISA07),
 target.ISA08 = TRIM(source.ISA08),
 target.ISA09 = TRIM(source.ISA09),
 target.ISA10 = TRIM(source.ISA10),
 target.ISA11 = TRIM(source.ISA11),
 target.ISA12 = TRIM(source.ISA12),
 target.ISA13 = TRIM(source.ISA13),
 target.ISA14 = TRIM(source.ISA14),
 target.ISA15 = TRIM(source.ISA15),
 target.ISA16 = TRIM(source.ISA16),
 target.ISASegment = TRIM(source.ISASegment),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (fileid, filename, fullfilename, filedate, ISA06, ISA07, ISA08, ISA09, ISA10, ISA11, ISA12, ISA13, ISA14, ISA15, ISA16, ISASegment, InsertedDTM, DWLastUpdateDateTime)
  VALUES (source.fileid, TRIM(source.filename), TRIM(source.fullfilename), source.filedate, TRIM(source.ISA06), TRIM(source.ISA07), TRIM(source.ISA08), TRIM(source.ISA09), TRIM(source.ISA10), TRIM(source.ISA11), TRIM(source.ISA12), TRIM(source.ISA13), TRIM(source.ISA14), TRIM(source.ISA15), TRIM(source.ISA16), TRIM(source.ISASegment), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT fileid
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgERAFileName
      GROUP BY fileid
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgERAFileName');
ELSE
  COMMIT TRANSACTION;
END IF;
