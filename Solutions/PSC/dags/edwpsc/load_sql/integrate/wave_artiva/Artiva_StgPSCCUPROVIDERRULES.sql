
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVIDERRULES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVIDERRULES
(psccuprrulekey, psccuprrulconfeffdte, psccuprrulconfigkey, psccuprrulconfiglbl, psccuprrulconflkflg, psccuprrulcontrmdte, psccuprruleid, psccuprrulinvowner, psccuprrulrequestor, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm)
SELECT source.psccuprrulekey, source.psccuprrulconfeffdte, TRIM(source.psccuprrulconfigkey), TRIM(source.psccuprrulconfiglbl), TRIM(source.psccuprrulconflkflg), source.psccuprrulcontrmdte, source.psccuprruleid, TRIM(source.psccuprrulinvowner), TRIM(source.psccuprrulrequestor), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUPROVIDERRULES as source;

-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVIDERRULES AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUPROVIDERRULES AS source
-- ON target.PSCCUPRRULEKEY = source.PSCCUPRRULEKEY
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.psccuprrulekey = source.psccuprrulekey,
--  target.psccuprrulconfeffdte = source.psccuprrulconfeffdte,
--  target.psccuprrulconfigkey = TRIM(source.psccuprrulconfigkey),
--  target.psccuprrulconfiglbl = TRIM(source.psccuprrulconfiglbl),
--  target.psccuprrulconflkflg = TRIM(source.psccuprrulconflkflg),
--  target.psccuprrulcontrmdte = source.psccuprrulcontrmdte,
--  target.psccuprruleid = source.psccuprruleid,
--  target.psccuprrulinvowner = TRIM(source.psccuprrulinvowner),
--  target.psccuprrulrequestor = TRIM(source.psccuprrulrequestor),
--  target.sourcesystemcode = TRIM(source.sourcesystemcode),
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime,
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm
-- WHEN NOT MATCHED THEN
--   INSERT (psccuprrulekey, psccuprrulconfeffdte, psccuprrulconfigkey, psccuprrulconfiglbl, psccuprrulconflkflg, psccuprrulcontrmdte, psccuprruleid, psccuprrulinvowner, psccuprrulrequestor, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm)
--   VALUES (source.psccuprrulekey, source.psccuprrulconfeffdte, TRIM(source.psccuprrulconfigkey), TRIM(source.psccuprrulconfiglbl), TRIM(source.psccuprrulconflkflg), source.psccuprrulcontrmdte, source.psccuprruleid, TRIM(source.psccuprrulinvowner), TRIM(source.psccuprrulrequestor), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT PSCCUPRRULEKEY
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVIDERRULES
--       GROUP BY PSCCUPRRULEKEY
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVIDERRULES');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
