
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEACTIONHIST AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEACTIONHIST AS source
ON target.PSPEACTHID = source.PSPEACTHID
WHEN MATCHED THEN
  UPDATE SET
  target.PSPEACTHACTID = TRIM(source.PSPEACTHACTID),
 target.PSPEACTHAVIID = source.PSPEACTHAVIID,
 target.PSPEACTHDTE = source.PSPEACTHDTE,
 target.PSPEACTHID = source.PSPEACTHID,
 target.PSPEACTHNOTE = TRIM(source.PSPEACTHNOTE),
 target.PSPEACTHNOTELINE = TRIM(source.PSPEACTHNOTELINE),
 target.PSPEACTHPERFID = TRIM(source.PSPEACTHPERFID),
 target.PSPEACTHPOOLID = TRIM(source.PSPEACTHPOOLID),
 target.PSPEACTHPPIID = TRIM(source.PSPEACTHPPIID),
 target.PSPEACTHRESID = TRIM(source.PSPEACTHRESID),
 target.PSPEACTHSTAT = TRIM(source.PSPEACTHSTAT),
 target.PSPEACTHTIME = source.PSPEACTHTIME,
 target.PSPEACTHUSERID = TRIM(source.PSPEACTHUSERID),
 target.LastNote = TRIM(source.LastNote),
 target.LastNoteCnt = source.LastNoteCnt,
 target.LastNoteUserID = TRIM(source.LastNoteUserID),
 target.LastNoteDateTime = source.LastNoteDateTime,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PSPEACTHACTID, PSPEACTHAVIID, PSPEACTHDTE, PSPEACTHID, PSPEACTHNOTE, PSPEACTHNOTELINE, PSPEACTHPERFID, PSPEACTHPOOLID, PSPEACTHPPIID, PSPEACTHRESID, PSPEACTHSTAT, PSPEACTHTIME, PSPEACTHUSERID, LastNote, LastNoteCnt, LastNoteUserID, LastNoteDateTime, DWLastUpdateDateTime)
  VALUES (TRIM(source.PSPEACTHACTID), source.PSPEACTHAVIID, source.PSPEACTHDTE, source.PSPEACTHID, TRIM(source.PSPEACTHNOTE), TRIM(source.PSPEACTHNOTELINE), TRIM(source.PSPEACTHPERFID), TRIM(source.PSPEACTHPOOLID), TRIM(source.PSPEACTHPPIID), TRIM(source.PSPEACTHRESID), TRIM(source.PSPEACTHSTAT), source.PSPEACTHTIME, TRIM(source.PSPEACTHUSERID), TRIM(source.LastNote), source.LastNoteCnt, TRIM(source.LastNoteUserID), source.LastNoteDateTime, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PSPEACTHID
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEACTIONHIST
      GROUP BY PSPEACTHID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEACTIONHIST');
ELSE
  COMMIT TRANSACTION;
END IF;
