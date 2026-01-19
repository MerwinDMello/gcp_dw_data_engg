
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSFINCLASSCROSS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSFINCLASSCROSS
(psfckey, psfciplangroup, psfcrossfincls, psfcsource, psfcsourcefincls, psfcsourcetype, insertedby, inserteddtm, modifiedby, modifieddtm, dwlastupdatedatetime)
SELECT source.psfckey, TRIM(source.psfciplangroup), TRIM(source.psfcrossfincls), TRIM(source.psfcsource), TRIM(source.psfcsourcefincls), TRIM(source.psfcsourcetype), TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.dwlastupdatedatetime
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSFINCLASSCROSS as source;


-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSFINCLASSCROSS AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSFINCLASSCROSS AS source
-- ON target.PSFCKEY = source.PSFCKEY
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.psfckey = source.psfckey,
--  target.psfciplangroup = TRIM(source.psfciplangroup),
--  target.psfcrossfincls = TRIM(source.psfcrossfincls),
--  target.psfcsource = TRIM(source.psfcsource),
--  target.psfcsourcefincls = TRIM(source.psfcsourcefincls),
--  target.psfcsourcetype = TRIM(source.psfcsourcetype),
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm,
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime
-- WHEN NOT MATCHED THEN
--   INSERT (psfckey, psfciplangroup, psfcrossfincls, psfcsource, psfcsourcefincls, psfcsourcetype, insertedby, inserteddtm, modifiedby, modifieddtm, dwlastupdatedatetime)
--   VALUES (source.psfckey, TRIM(source.psfciplangroup), TRIM(source.psfcrossfincls), TRIM(source.psfcsource), TRIM(source.psfcsourcefincls), TRIM(source.psfcsourcetype), TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.dwlastupdatedatetime);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT PSFCKEY
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSFINCLASSCROSS
--       GROUP BY PSFCKEY
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSFINCLASSCROSS');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
