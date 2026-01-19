TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDRULES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDRULES
(psccucoidrulekey, psccucoidrulconeffdt, psccucoidrulconfgkey, psccucoidrulconfglbl, psccucoidrulconlkflg, psccucoidrulcontrmdt, psccucoidruleid, psccucoidrulinvowner, psccucoidrulrequestr, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm)
SELECT source.psccucoidrulekey, source.psccucoidrulconeffdt, TRIM(source.psccucoidrulconfgkey), TRIM(source.psccucoidrulconfglbl), TRIM(source.psccucoidrulconlkflg), source.psccucoidrulcontrmdt, source.psccucoidruleid, TRIM(source.psccucoidrulinvowner), TRIM(source.psccucoidrulrequestr), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUCOIDRULES as source;


-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDRULES AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUCOIDRULES AS source
-- ON target.PSCCUCOIDRULEKEY = source.PSCCUCOIDRULEKEY
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.psccucoidrulekey = source.psccucoidrulekey,
--  target.psccucoidrulconeffdt = source.psccucoidrulconeffdt,
--  target.psccucoidrulconfgkey = TRIM(source.psccucoidrulconfgkey),
--  target.psccucoidrulconfglbl = TRIM(source.psccucoidrulconfglbl),
--  target.psccucoidrulconlkflg = TRIM(source.psccucoidrulconlkflg),
--  target.psccucoidrulcontrmdt = source.psccucoidrulcontrmdt,
--  target.psccucoidruleid = source.psccucoidruleid,
--  target.psccucoidrulinvowner = TRIM(source.psccucoidrulinvowner),
--  target.psccucoidrulrequestr = TRIM(source.psccucoidrulrequestr),
--  target.sourcesystemcode = TRIM(source.sourcesystemcode),
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime,
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm
-- WHEN NOT MATCHED THEN
--   INSERT (psccucoidrulekey, psccucoidrulconeffdt, psccucoidrulconfgkey, psccucoidrulconfglbl, psccucoidrulconlkflg, psccucoidrulcontrmdt, psccucoidruleid, psccucoidrulinvowner, psccucoidrulrequestr, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm)
--   VALUES (source.psccucoidrulekey, source.psccucoidrulconeffdt, TRIM(source.psccucoidrulconfgkey), TRIM(source.psccucoidrulconfglbl), TRIM(source.psccucoidrulconlkflg), source.psccucoidrulcontrmdt, source.psccucoidruleid, TRIM(source.psccucoidrulinvowner), TRIM(source.psccucoidrulrequestr), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT PSCCUCOIDRULEKEY
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDRULES
--       GROUP BY PSCCUCOIDRULEKEY
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDRULES');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
