
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVMASTER ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVMASTER
(psccuprovkey, psccupr100audit, psccuprassigntvu, psccuprcdchgvardet, psccuprchrgovrride, psccuprcodechgvar, psccuprcoidmid, psccuprcoidnpiid, psccuprcompchrgent, psccuprdedcoder, psccuprembdcoder, psccuprmisdocprocvar, psccuprmissdocvar, psccuprmulticoidassc, psccuprnonhcafac, psccuprnouseecwsch, psccuprovcoidnum, psccuprovnotes, psccuprpaperproc, psccuprpapprocdetail, psccuprposvariation, psccuprprovcentstat, psccuprprovecwemr, psccuprprovenddtecom, psccuprprovgrpassign, psccuprprovlmtdsrvc, psccuprprovname, psccuprprovnonstdemr, psccuprprovnonstdsys, psccuprprovnpi, psccuprprovpatkeepcc, psccuprprovpkenv, psccuprprovspec, psccuprprovstatus, psccuprprovsvcsvend, psccuprprovtermdte, psccuprprovuseecw, psccuprprovuseepic, psccuprprovvendnm, psccuprprovvendsyspl, psccuprprovvenplcd, psccuprprovwrkflw, psccuprprvvndasgndte, psccuprprvvndenddte, psccuprprvvndpupdte, psccuprselfcoding, psccuprtosvariation, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm, psccupr100audit_wfcount, psccuprassigntvu_wfcount, psccuprchrgovrride_wfcount, psccuprcodechgvar_wfcount, psccuprcompchrgent_wfcount, psccuprdedcoder_wfcount, psccuprembdcoder_wfcount, psccuprmissdocvar_wfcount, psccuprnouseecwsch_wfcount, psccuprpaperproc_wfcount, psccuprprovecwemr_wfcount, psccuprprovpatkeepcc_wfcount, psccuprprovuseecw_wfcount, psccuprprovuseepic_wfcount, psccuprnonhcafac_wfcount, psccuprprovnonstdemr_wfcount, psccuprprovnonstdsys_wfcount, psccuprnonhcafacexc, psccuprprovstartdte)
SELECT source.psccuprovkey, TRIM(source.psccupr100audit), TRIM(source.psccuprassigntvu), TRIM(source.psccuprcdchgvardet), TRIM(source.psccuprchrgovrride), TRIM(source.psccuprcodechgvar), source.psccuprcoidmid, source.psccuprcoidnpiid, TRIM(source.psccuprcompchrgent), TRIM(source.psccuprdedcoder), TRIM(source.psccuprembdcoder), TRIM(source.psccuprmisdocprocvar), TRIM(source.psccuprmissdocvar), TRIM(source.psccuprmulticoidassc), TRIM(source.psccuprnonhcafac), TRIM(source.psccuprnouseecwsch), TRIM(source.psccuprovcoidnum), TRIM(source.psccuprovnotes), TRIM(source.psccuprpaperproc), TRIM(source.psccuprpapprocdetail), TRIM(source.psccuprposvariation), TRIM(source.psccuprprovcentstat), TRIM(source.psccuprprovecwemr), TRIM(source.psccuprprovenddtecom), TRIM(source.psccuprprovgrpassign), TRIM(source.psccuprprovlmtdsrvc), TRIM(source.psccuprprovname), TRIM(source.psccuprprovnonstdemr), TRIM(source.psccuprprovnonstdsys), source.psccuprprovnpi, TRIM(source.psccuprprovpatkeepcc), TRIM(source.psccuprprovpkenv), TRIM(source.psccuprprovspec), TRIM(source.psccuprprovstatus), TRIM(source.psccuprprovsvcsvend), source.psccuprprovtermdte, TRIM(source.psccuprprovuseecw), TRIM(source.psccuprprovuseepic), TRIM(source.psccuprprovvendnm), TRIM(source.psccuprprovvendsyspl), TRIM(source.psccuprprovvenplcd), TRIM(source.psccuprprovwrkflw), source.psccuprprvvndasgndte, source.psccuprprvvndenddte, source.psccuprprvvndpupdte, TRIM(source.psccuprselfcoding), TRIM(source.psccuprtosvariation), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.psccupr100audit_wfcount, source.psccuprassigntvu_wfcount, source.psccuprchrgovrride_wfcount, source.psccuprcodechgvar_wfcount, source.psccuprcompchrgent_wfcount, source.psccuprdedcoder_wfcount, source.psccuprembdcoder_wfcount, source.psccuprmissdocvar_wfcount, source.psccuprnouseecwsch_wfcount, source.psccuprpaperproc_wfcount, source.psccuprprovecwemr_wfcount, source.psccuprprovpatkeepcc_wfcount, source.psccuprprovuseecw_wfcount, source.psccuprprovuseepic_wfcount, source.psccuprnonhcafac_wfcount, source.psccuprprovnonstdemr_wfcount, source.psccuprprovnonstdsys_wfcount, TRIM(source.psccuprnonhcafacexc), source.psccuprprovstartdte
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUPROVMASTER as source;


-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVMASTER AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSCCUPROVMASTER AS source
-- ON target.PSCCUPRPROVNPI = source.PSCCUPRPROVNPI AND target.PSCCUPROVCOIDNUM = source.PSCCUPROVCOIDNUM AND target.PSCCUPRCOIDMID = source.PSCCUPRCOIDMID
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.psccuprovkey = source.psccuprovkey,
--  target.psccupr100audit = TRIM(source.psccupr100audit),
--  target.psccuprassigntvu = TRIM(source.psccuprassigntvu),
--  target.psccuprcdchgvardet = TRIM(source.psccuprcdchgvardet),
--  target.psccuprchrgovrride = TRIM(source.psccuprchrgovrride),
--  target.psccuprcodechgvar = TRIM(source.psccuprcodechgvar),
--  target.psccuprcoidmid = source.psccuprcoidmid,
--  target.psccuprcoidnpiid = source.psccuprcoidnpiid,
--  target.psccuprcompchrgent = TRIM(source.psccuprcompchrgent),
--  target.psccuprdedcoder = TRIM(source.psccuprdedcoder),
--  target.psccuprembdcoder = TRIM(source.psccuprembdcoder),
--  target.psccuprmisdocprocvar = TRIM(source.psccuprmisdocprocvar),
--  target.psccuprmissdocvar = TRIM(source.psccuprmissdocvar),
--  target.psccuprmulticoidassc = TRIM(source.psccuprmulticoidassc),
--  target.psccuprnonhcafac = TRIM(source.psccuprnonhcafac),
--  target.psccuprnouseecwsch = TRIM(source.psccuprnouseecwsch),
--  target.psccuprovcoidnum = TRIM(source.psccuprovcoidnum),
--  target.psccuprovnotes = TRIM(source.psccuprovnotes),
--  target.psccuprpaperproc = TRIM(source.psccuprpaperproc),
--  target.psccuprpapprocdetail = TRIM(source.psccuprpapprocdetail),
--  target.psccuprposvariation = TRIM(source.psccuprposvariation),
--  target.psccuprprovcentstat = TRIM(source.psccuprprovcentstat),
--  target.psccuprprovecwemr = TRIM(source.psccuprprovecwemr),
--  target.psccuprprovenddtecom = TRIM(source.psccuprprovenddtecom),
--  target.psccuprprovgrpassign = TRIM(source.psccuprprovgrpassign),
--  target.psccuprprovlmtdsrvc = TRIM(source.psccuprprovlmtdsrvc),
--  target.psccuprprovname = TRIM(source.psccuprprovname),
--  target.psccuprprovnonstdemr = TRIM(source.psccuprprovnonstdemr),
--  target.psccuprprovnonstdsys = TRIM(source.psccuprprovnonstdsys),
--  target.psccuprprovnpi = source.psccuprprovnpi,
--  target.psccuprprovpatkeepcc = TRIM(source.psccuprprovpatkeepcc),
--  target.psccuprprovpkenv = TRIM(source.psccuprprovpkenv),
--  target.psccuprprovspec = TRIM(source.psccuprprovspec),
--  target.psccuprprovstatus = TRIM(source.psccuprprovstatus),
--  target.psccuprprovsvcsvend = TRIM(source.psccuprprovsvcsvend),
--  target.psccuprprovtermdte = source.psccuprprovtermdte,
--  target.psccuprprovuseecw = TRIM(source.psccuprprovuseecw),
--  target.psccuprprovuseepic = TRIM(source.psccuprprovuseepic),
--  target.psccuprprovvendnm = TRIM(source.psccuprprovvendnm),
--  target.psccuprprovvendsyspl = TRIM(source.psccuprprovvendsyspl),
--  target.psccuprprovvenplcd = TRIM(source.psccuprprovvenplcd),
--  target.psccuprprovwrkflw = TRIM(source.psccuprprovwrkflw),
--  target.psccuprprvvndasgndte = source.psccuprprvvndasgndte,
--  target.psccuprprvvndenddte = source.psccuprprvvndenddte,
--  target.psccuprprvvndpupdte = source.psccuprprvvndpupdte,
--  target.psccuprselfcoding = TRIM(source.psccuprselfcoding),
--  target.psccuprtosvariation = TRIM(source.psccuprtosvariation),
--  target.sourcesystemcode = TRIM(source.sourcesystemcode),
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime,
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm,
--  target.psccupr100audit_wfcount = source.psccupr100audit_wfcount,
--  target.psccuprassigntvu_wfcount = source.psccuprassigntvu_wfcount,
--  target.psccuprchrgovrride_wfcount = source.psccuprchrgovrride_wfcount,
--  target.psccuprcodechgvar_wfcount = source.psccuprcodechgvar_wfcount,
--  target.psccuprcompchrgent_wfcount = source.psccuprcompchrgent_wfcount,
--  target.psccuprdedcoder_wfcount = source.psccuprdedcoder_wfcount,
--  target.psccuprembdcoder_wfcount = source.psccuprembdcoder_wfcount,
--  target.psccuprmissdocvar_wfcount = source.psccuprmissdocvar_wfcount,
--  target.psccuprnouseecwsch_wfcount = source.psccuprnouseecwsch_wfcount,
--  target.psccuprpaperproc_wfcount = source.psccuprpaperproc_wfcount,
--  target.psccuprprovecwemr_wfcount = source.psccuprprovecwemr_wfcount,
--  target.psccuprprovpatkeepcc_wfcount = source.psccuprprovpatkeepcc_wfcount,
--  target.psccuprprovuseecw_wfcount = source.psccuprprovuseecw_wfcount,
--  target.psccuprprovuseepic_wfcount = source.psccuprprovuseepic_wfcount,
--  target.psccuprnonhcafac_wfcount = source.psccuprnonhcafac_wfcount,
--  target.psccuprprovnonstdemr_wfcount = source.psccuprprovnonstdemr_wfcount,
--  target.psccuprprovnonstdsys_wfcount = source.psccuprprovnonstdsys_wfcount,
--  target.psccuprnonhcafacexc = TRIM(source.psccuprnonhcafacexc),
--  target.psccuprprovstartdte = source.psccuprprovstartdte
-- WHEN NOT MATCHED THEN
--   INSERT (psccuprovkey, psccupr100audit, psccuprassigntvu, psccuprcdchgvardet, psccuprchrgovrride, psccuprcodechgvar, psccuprcoidmid, psccuprcoidnpiid, psccuprcompchrgent, psccuprdedcoder, psccuprembdcoder, psccuprmisdocprocvar, psccuprmissdocvar, psccuprmulticoidassc, psccuprnonhcafac, psccuprnouseecwsch, psccuprovcoidnum, psccuprovnotes, psccuprpaperproc, psccuprpapprocdetail, psccuprposvariation, psccuprprovcentstat, psccuprprovecwemr, psccuprprovenddtecom, psccuprprovgrpassign, psccuprprovlmtdsrvc, psccuprprovname, psccuprprovnonstdemr, psccuprprovnonstdsys, psccuprprovnpi, psccuprprovpatkeepcc, psccuprprovpkenv, psccuprprovspec, psccuprprovstatus, psccuprprovsvcsvend, psccuprprovtermdte, psccuprprovuseecw, psccuprprovuseepic, psccuprprovvendnm, psccuprprovvendsyspl, psccuprprovvenplcd, psccuprprovwrkflw, psccuprprvvndasgndte, psccuprprvvndenddte, psccuprprvvndpupdte, psccuprselfcoding, psccuprtosvariation, sourcesystemcode, dwlastupdatedatetime, insertedby, inserteddtm, modifiedby, modifieddtm, psccupr100audit_wfcount, psccuprassigntvu_wfcount, psccuprchrgovrride_wfcount, psccuprcodechgvar_wfcount, psccuprcompchrgent_wfcount, psccuprdedcoder_wfcount, psccuprembdcoder_wfcount, psccuprmissdocvar_wfcount, psccuprnouseecwsch_wfcount, psccuprpaperproc_wfcount, psccuprprovecwemr_wfcount, psccuprprovpatkeepcc_wfcount, psccuprprovuseecw_wfcount, psccuprprovuseepic_wfcount, psccuprnonhcafac_wfcount, psccuprprovnonstdemr_wfcount, psccuprprovnonstdsys_wfcount, psccuprnonhcafacexc, psccuprprovstartdte)
--   VALUES (source.psccuprovkey, TRIM(source.psccupr100audit), TRIM(source.psccuprassigntvu), TRIM(source.psccuprcdchgvardet), TRIM(source.psccuprchrgovrride), TRIM(source.psccuprcodechgvar), source.psccuprcoidmid, source.psccuprcoidnpiid, TRIM(source.psccuprcompchrgent), TRIM(source.psccuprdedcoder), TRIM(source.psccuprembdcoder), TRIM(source.psccuprmisdocprocvar), TRIM(source.psccuprmissdocvar), TRIM(source.psccuprmulticoidassc), TRIM(source.psccuprnonhcafac), TRIM(source.psccuprnouseecwsch), TRIM(source.psccuprovcoidnum), TRIM(source.psccuprovnotes), TRIM(source.psccuprpaperproc), TRIM(source.psccuprpapprocdetail), TRIM(source.psccuprposvariation), TRIM(source.psccuprprovcentstat), TRIM(source.psccuprprovecwemr), TRIM(source.psccuprprovenddtecom), TRIM(source.psccuprprovgrpassign), TRIM(source.psccuprprovlmtdsrvc), TRIM(source.psccuprprovname), TRIM(source.psccuprprovnonstdemr), TRIM(source.psccuprprovnonstdsys), source.psccuprprovnpi, TRIM(source.psccuprprovpatkeepcc), TRIM(source.psccuprprovpkenv), TRIM(source.psccuprprovspec), TRIM(source.psccuprprovstatus), TRIM(source.psccuprprovsvcsvend), source.psccuprprovtermdte, TRIM(source.psccuprprovuseecw), TRIM(source.psccuprprovuseepic), TRIM(source.psccuprprovvendnm), TRIM(source.psccuprprovvendsyspl), TRIM(source.psccuprprovvenplcd), TRIM(source.psccuprprovwrkflw), source.psccuprprvvndasgndte, source.psccuprprvvndenddte, source.psccuprprvvndpupdte, TRIM(source.psccuprselfcoding), TRIM(source.psccuprtosvariation), TRIM(source.sourcesystemcode), source.dwlastupdatedatetime, TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, source.psccupr100audit_wfcount, source.psccuprassigntvu_wfcount, source.psccuprchrgovrride_wfcount, source.psccuprcodechgvar_wfcount, source.psccuprcompchrgent_wfcount, source.psccuprdedcoder_wfcount, source.psccuprembdcoder_wfcount, source.psccuprmissdocvar_wfcount, source.psccuprnouseecwsch_wfcount, source.psccuprpaperproc_wfcount, source.psccuprprovecwemr_wfcount, source.psccuprprovpatkeepcc_wfcount, source.psccuprprovuseecw_wfcount, source.psccuprprovuseepic_wfcount, source.psccuprnonhcafac_wfcount, source.psccuprprovnonstdemr_wfcount, source.psccuprprovnonstdsys_wfcount, TRIM(source.psccuprnonhcafacexc), source.psccuprprovstartdte);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT PSCCUPRPROVNPI, PSCCUPROVCOIDNUM, PSCCUPRCOIDMID
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVMASTER
--       GROUP BY PSCCUPRPROVNPI, PSCCUPROVCOIDNUM, PSCCUPRCOIDMID
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPSCCUPROVMASTER');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
