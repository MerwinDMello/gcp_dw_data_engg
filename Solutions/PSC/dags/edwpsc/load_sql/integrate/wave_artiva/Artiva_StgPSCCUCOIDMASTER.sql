
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDMASTER ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDMASTER
(psccucoidkey, psccucoidcentstat, psccucoidclsdvstdte, psccucoidconsoldte, psccucoiddiscontdte, psccucoiddivision, psccucoidenddtecom, psccucoidgmecoid, psccucoidgroup, psccucoidgrpassign, psccucoidlmtdsvcs, psccucoidlob, psccucoidmarket, psccucoidname, psccucoidnotes, psccucoidnum, psccucoidpkenviron, psccucoidspecialty, psccucoidstatus, psccucoidsvcscdbyven, psccucoidvendenddte, psccucoidvendname, psccucoidvendplcd, psccucoidvendpupdte, psccucoidvendsysplcd, psccucoidvndassgndte, psccucoidworkflow, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm)
SELECT source.psccucoidkey, TRIM(source.psccucoidcentstat), source.psccucoidclsdvstdte, source.psccucoidconsoldte, source.psccucoiddiscontdte, TRIM(source.psccucoiddivision), TRIM(source.psccucoidenddtecom), TRIM(source.psccucoidgmecoid), TRIM(source.psccucoidgroup), TRIM(source.psccucoidgrpassign), TRIM(source.psccucoidlmtdsvcs), TRIM(source.psccucoidlob), TRIM(source.psccucoidmarket), TRIM(source.psccucoidname), TRIM(source.psccucoidnotes), TRIM(source.psccucoidnum), TRIM(source.psccucoidpkenviron), TRIM(source.psccucoidspecialty), TRIM(source.psccucoidstatus), TRIM(source.psccucoidsvcscdbyven), source.psccucoidvendenddte, TRIM(source.psccucoidvendname), TRIM(source.psccucoidvendplcd), source.psccucoidvendpupdte, TRIM(source.psccucoidvendsysplcd), source.psccucoidvndassgndte, TRIM(source.psccucoidworkflow), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUCOIDMASTER as source;

-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDMASTER AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUCOIDMASTER AS source
-- ON target.PSCCUCOIDNUM = source.PSCCUCOIDNUM
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.psccucoidkey = source.psccucoidkey,
--  target.psccucoidcentstat = TRIM(source.psccucoidcentstat),
--  target.psccucoidclsdvstdte = source.psccucoidclsdvstdte,
--  target.psccucoidconsoldte = source.psccucoidconsoldte,
--  target.psccucoiddiscontdte = source.psccucoiddiscontdte,
--  target.psccucoiddivision = TRIM(source.psccucoiddivision),
--  target.psccucoidenddtecom = TRIM(source.psccucoidenddtecom),
--  target.psccucoidgmecoid = TRIM(source.psccucoidgmecoid),
--  target.psccucoidgroup = TRIM(source.psccucoidgroup),
--  target.psccucoidgrpassign = TRIM(source.psccucoidgrpassign),
--  target.psccucoidlmtdsvcs = TRIM(source.psccucoidlmtdsvcs),
--  target.psccucoidlob = TRIM(source.psccucoidlob),
--  target.psccucoidmarket = TRIM(source.psccucoidmarket),
--  target.psccucoidname = TRIM(source.psccucoidname),
--  target.psccucoidnotes = TRIM(source.psccucoidnotes),
--  target.psccucoidnum = TRIM(source.psccucoidnum),
--  target.psccucoidpkenviron = TRIM(source.psccucoidpkenviron),
--  target.psccucoidspecialty = TRIM(source.psccucoidspecialty),
--  target.psccucoidstatus = TRIM(source.psccucoidstatus),
--  target.psccucoidsvcscdbyven = TRIM(source.psccucoidsvcscdbyven),
--  target.psccucoidvendenddte = source.psccucoidvendenddte,
--  target.psccucoidvendname = TRIM(source.psccucoidvendname),
--  target.psccucoidvendplcd = TRIM(source.psccucoidvendplcd),
--  target.psccucoidvendpupdte = source.psccucoidvendpupdte,
--  target.psccucoidvendsysplcd = TRIM(source.psccucoidvendsysplcd),
--  target.psccucoidvndassgndte = source.psccucoidvndassgndte,
--  target.psccucoidworkflow = TRIM(source.psccucoidworkflow),
--  target.sourcesystemcode = TRIM(source.sourcesystemcode),
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime,
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm
-- WHEN NOT MATCHED THEN
--   INSERT (psccucoidkey, psccucoidcentstat, psccucoidclsdvstdte, psccucoidconsoldte, psccucoiddiscontdte, psccucoiddivision, psccucoidenddtecom, psccucoidgmecoid, psccucoidgroup, psccucoidgrpassign, psccucoidlmtdsvcs, psccucoidlob, psccucoidmarket, psccucoidname, psccucoidnotes, psccucoidnum, psccucoidpkenviron, psccucoidspecialty, psccucoidstatus, psccucoidsvcscdbyven, psccucoidvendenddte, psccucoidvendname, psccucoidvendplcd, psccucoidvendpupdte, psccucoidvendsysplcd, psccucoidvndassgndte, psccucoidworkflow, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm)
--   VALUES (source.psccucoidkey, TRIM(source.psccucoidcentstat), source.psccucoidclsdvstdte, source.psccucoidconsoldte, source.psccucoiddiscontdte, TRIM(source.psccucoiddivision), TRIM(source.psccucoidenddtecom), TRIM(source.psccucoidgmecoid), TRIM(source.psccucoidgroup), TRIM(source.psccucoidgrpassign), TRIM(source.psccucoidlmtdsvcs), TRIM(source.psccucoidlob), TRIM(source.psccucoidmarket), TRIM(source.psccucoidname), TRIM(source.psccucoidnotes), TRIM(source.psccucoidnum), TRIM(source.psccucoidpkenviron), TRIM(source.psccucoidspecialty), TRIM(source.psccucoidstatus), TRIM(source.psccucoidsvcscdbyven), source.psccucoidvendenddte, TRIM(source.psccucoidvendname), TRIM(source.psccucoidvendplcd), source.psccucoidvendpupdte, TRIM(source.psccucoidvendsysplcd), source.psccucoidvndassgndte, TRIM(source.psccucoidworkflow), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT PSCCUCOIDNUM
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDMASTER
--       GROUP BY PSCCUCOIDNUM
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUCOIDMASTER');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
