
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUTILITY_PSPESPANOTES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUTILITY_PSPESPANOTES
(note_cnt, note_date, note_time, notedatetime, note_type, note_user, pspespakey, pspespanotes, insertedby, inserteddtm, modifiedby, modifieddtm, dwlastupdatedatetime)
SELECT source.note_cnt, source.note_date, source.note_time, source.notedatetime, TRIM(source.note_type), TRIM(source.note_user), TRIM(source.pspespakey), TRIM(source.pspespanotes), TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.dwlastupdatedatetime
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPESPAUTILITY_PSPESPANOTES as source;

-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUTILITY_PSPESPANOTES AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPESPAUTILITY_PSPESPANOTES AS source
-- ON target.PSPESPAKEY = source.PSPESPAKEY AND target.NOTE_CNT = source.NOTE_CNT
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.note_cnt = source.note_cnt,
--  target.note_date = source.note_date,
--  target.note_time = source.note_time,
--  target.notedatetime = source.notedatetime,
--  target.note_type = TRIM(source.note_type),
--  target.note_user = TRIM(source.note_user),
--  target.pspespakey = TRIM(source.pspespakey),
--  target.pspespanotes = TRIM(source.pspespanotes),
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm,
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime
-- WHEN NOT MATCHED THEN
--   INSERT (note_cnt, note_date, note_time, notedatetime, note_type, note_user, pspespakey, pspespanotes, insertedby, inserteddtm, modifiedby, modifieddtm, dwlastupdatedatetime)
--   VALUES (source.note_cnt, source.note_date, source.note_time, source.notedatetime, TRIM(source.note_type), TRIM(source.note_user), TRIM(source.pspespakey), TRIM(source.pspespanotes), TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.dwlastupdatedatetime);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT PSPESPAKEY, NOTE_CNT
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUTILITY_PSPESPANOTES
--       GROUP BY PSPESPAKEY, NOTE_CNT
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUTILITY_PSPESPANOTES');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
