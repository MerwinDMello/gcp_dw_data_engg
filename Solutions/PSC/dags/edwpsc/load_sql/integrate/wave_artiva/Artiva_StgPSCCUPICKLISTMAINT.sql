
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPICKLISTMAINT ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPICKLISTMAINT
(psccupklstkey, psccupklstactive, psccupklstfieldnm, psccupklstfldvalue, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm, psccupklstactdte, psccupklstinactdate, psccupklstlastupdusr, psccupklstupddte)
SELECT source.psccupklstkey, TRIM(source.psccupklstactive), TRIM(source.psccupklstfieldnm), TRIM(source.psccupklstfldvalue), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.psccupklstactdte, source.psccupklstinactdate, TRIM(source.psccupklstlastupdusr), source.psccupklstupddte
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUPICKLISTMAINT as source;

-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPICKLISTMAINT AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUPICKLISTMAINT AS source
-- ON target.PSCCUPKLSTFIELDNM = source.PSCCUPKLSTFIELDNM AND target.PSCCUPKLSTFLDVALUE = source.PSCCUPKLSTFLDVALUE
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.psccupklstkey = source.psccupklstkey,
--  target.psccupklstactive = TRIM(source.psccupklstactive),
--  target.psccupklstfieldnm = TRIM(source.psccupklstfieldnm),
--  target.psccupklstfldvalue = TRIM(source.psccupklstfldvalue),
--  target.sourcesystemcode = TRIM(source.sourcesystemcode),
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime,
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm,
--  target.psccupklstactdte = source.psccupklstactdte,
--  target.psccupklstinactdate = source.psccupklstinactdate,
--  target.psccupklstlastupdusr = TRIM(source.psccupklstlastupdusr),
--  target.psccupklstupddte = source.psccupklstupddte
-- WHEN NOT MATCHED THEN
--   INSERT (psccupklstkey, psccupklstactive, psccupklstfieldnm, psccupklstfldvalue, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm, psccupklstactdte, psccupklstinactdate, psccupklstlastupdusr, psccupklstupddte)
--   VALUES (source.psccupklstkey, TRIM(source.psccupklstactive), TRIM(source.psccupklstfieldnm), TRIM(source.psccupklstfldvalue), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.psccupklstactdte, source.psccupklstinactdate, TRIM(source.psccupklstlastupdusr), source.psccupklstupddte);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT PSCCUPKLSTFIELDNM, PSCCUPKLSTFLDVALUE
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPICKLISTMAINT
--       GROUP BY PSCCUPKLSTFIELDNM, PSCCUPKLSTFLDVALUE
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPICKLISTMAINT');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
