##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE difference, control2_difference, control3_difference, src_rec_count, tgt_rec_count int64 DEFAULT 0;

DECLARE control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount NUMERIC DEFAULT 0;

DECLARE srctableid, tolerance_percent INT64;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = NULL;

SET sourcesysnm = ''; -- This needs to be added

SET srcdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_stage_dataset_name }} , '.') AS arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_core_dataset_name }} , '.') AS arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET audit_type ='RECORD_COUNT';

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET
(src_rec_count) = 
(
SELECT count(*)
FROM
  (SELECT a.*
   FROM
     (SELECT DISTINCT NULL AS patient_case_sk,
                      ca_server.server_sk,
                      trim(format('%11d', stg.casenumber)) AS casenumber,
                      trim(format('%11d', stg.hospitalizationid)) AS hospitalizationid,
                      a_0.patient_sk AS patid,
                      trim(format('%11d', stg.hospitalid)) AS hospitalid,
                      trim(stg.stsvendorid) AS stsvendorid,
                      trim(stg.aparticid) AS aparticid,
                      trim(stg.cparticid) AS cparticid,
                      trim(stg.tparticid) AS tparticid,
                      trim(format('%11d', stg.optype)) AS optype,
                      trim(format('%11d', stg.databasetype)) AS databasetype,
                      stg.surgdt AS surgdt,
                      trim(stg.orentryt) AS orentryt,
                      trim(stg.sistartt) AS sistartt,
                      trim(stg.sistopt) AS sistopt,
                      trim(stg.orexitt) AS orexitt,
                      stg.eclsoffdt AS eclsoffdt,
                      trim(stg.heightcm) AS heightcm,
                      trim(stg.weightkg) AS weightkg,
                      trim(format('%11d', stg.multiday)) AS multiday,
                      trim(format('%11d', stg.imedaprot)) AS imedaprot,
                      trim(format('%11d', stg.imedeaca)) AS imedeaca,
                      trim(format('%11d', stg.imeddesmo)) AS imeddesmo,
                      trim(format('%11d', stg.imedtran)) AS imedtran,
                      trim(format('%11d', stg.imedaprotd)) AS imedaprotd,
                      trim(format('%11d', stg.imedeacad)) AS imedeacad,
                      trim(regexp_replace(format('%#20.2f', stg.imeddesmod), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS asimeddesmod,
                      trim(format('%11d', stg.imedtrand)) AS imedtrand,
                      trim(format('%11d', stg.procloc)) AS procloc,
                      trim(format('%11d', stg.reopinadm)) AS reopinadm,
                      trim(format('%11d', stg.prvctopn)) AS prvctopn,
                      trim(format('%11d', stg.prvoctopn)) AS prvoctopn,
                      trim(format('%11d', stg.xclamptmnc)) AS xclamptmnc,
                      trim(format('%11d', stg.surgeon)) AS surgeon,
                      trim(format('%11d', stg.asstsurgeon)) AS asstsurgeon,
                      trim(format('%11d', stg.resident)) AS resident,
                      trim(format('%11d', stg.primanesname)) AS primanesname,
                      trim(format('%11d', stg.secanes)) AS secanes,
                      trim(format('%11d', stg.felres)) AS felres,
                      trim(format('%11d', stg.crna)) AS crna,
                      trim(format('%11d', stg.noncvphys)) AS noncvphys,
                      trim(format('%11d', stg.secanesid2)) AS secanesid2,
                      trim(format('%11d', stg.crnaid)) AS crnaid,
                      trim(format('%11d', stg.nurse)) AS nurse,
                      trim(format('%11d', stg.perfusionist)) AS perfusionist,
                      trim(format('%11d', stg.physicianassistant)) AS physicianassistant,
                      trim(format('%11d', stg.ibdprod)) AS ibdprod,
                      trim(format('%11d', stg.ibdprodref)) AS ibdprodref,
                      trim(format('%11d', stg.ibdrbc)) AS ibdrbc,
                      trim(format('%11d', stg.ibdrbcde)) AS ibdrbcde,
                      trim(format('%11d', stg.ibdrbcu)) AS ibdrbcu,
                      trim(format('%11d', stg.ibdrbcm)) AS ibdrbcm,
                      trim(format('%11d', stg.ibdffp)) AS ibdffp,
                      trim(format('%11d', stg.ibdffpde)) AS ibdffpde,
                      trim(format('%11d', stg.ibdffpu)) AS ibdffpu,
                      trim(format('%11d', stg.ibdffpm)) AS ibdffpm,
                      trim(format('%11d', stg.ibdcryo)) AS ibdcryo,
                      trim(format('%11d', stg.ibdcryode)) AS ibdcryode,
                      trim(format('%11d', stg.ibdcryou)) AS ibdcryou,
                      trim(format('%11d', stg.ibdcryom)) AS ibdcryom,
                      trim(format('%11d', stg.ibdplat)) AS ibdplat,
                      trim(format('%11d', stg.ibdplatde)) AS ibdplatde,
                      trim(format('%11d', stg.ibdplatu)) AS ibdplatu,
                      trim(format('%11d', stg.ibdplatm)) AS ibdplatm,
                      trim(format('%11d', stg.ibdwb)) AS ibdwb,
                      trim(format('%11d', stg.ibdwbde)) AS ibdwbde,
                      trim(format('%11d', stg.ibdwbfresh)) AS asibdwbfresh,
                      trim(format('%11d', stg.ibdwbu)) AS ibdwbu,
                      trim(format('%11d', stg.ibdwbm)) AS ibdwbm,
                      trim(format('%11d', stg.ibdfviia)) AS ibdfviia,
                      trim(format('%11d', stg.ibdfviiad)) AS ibdfviiad,
                      trim(stg.cdatavrsn) AS cdatavrsn,
                      trim(stg.adatavrsn) AS adatavrsn,
                      trim(stg.tdatavrsn) AS tdatavrsn,
                      trim(format('%11d', stg.reasonforsupportid)) AS reasonforsupportid,
                      trim(format('%11d', stg.eclsmodeid)) AS eclsmodeid,
                      trim(format('%11d', stg.eclsopenchest)) AS eclsopenchest,
                      trim(format('%11d', stg.eclsbloodprime)) AS eclsbloodprime,
                      trim(format('%11d', stg.equipusedmemblungid)) AS equipusedmemblungid,
                      trim(format('%11d', stg.equipusedheatexchid)) AS equipusedheatexchid,
                      trim(format('%11d', stg.equipusedpumpid)) AS equipusedpumpid,
                      trim(format('%11d', stg.equipusedhemofilterid)) AS equipusedhemofilterid,
                      trim(format('%11d', stg.pumpflowunitsid)) AS pumpflowunitsid,
                      trim(stg.pumpflow4thhourflow) AS pumpflow4thhourflow,
                      trim(stg.pumpflow24thhourflow) AS pumpflow24thhourflow,
                      stg.createdate AS createdate,
                      stg.updatedate AS updatedate,
                      trim(stg.updateby) AS updateby,
                      trim(format('%11d', stg.eclsuniqueid)) AS eclsuniqueid,
                      trim(format('%11d', stg.eclsrunnumber)) AS eclsrunnumber,
                      trim(stg.eventguid) AS eventguid,
                      trim(stg.bloodgasguid) AS bloodgasguid,
                      trim(stg.hemodynamicguid) AS hemodynamicguid,
                      trim(stg.ventsettingguid) AS ventsettingguid,
                      trim(format('%11d', stg.opstatus)) AS opstatus,
                      trim(format('%11d', stg.chsselig)) AS chsselig,
                      trim(stg.ststlink) AS ststlink,
                      trim(stg.caselinknum) AS caselinknum,
                      trim(format('%11d', stg.cpbstandby)) AS cpbstandby,
                      trim(stg.notes) AS notes,
                      trim(stg.bloodlossest) AS bloodlossest,
                      trim(stg.specimens) AS specimens,
                      trim(format('%11d', stg.clintrial)) AS clintrial,
                      trim(stg.clintrialpatid) AS clintrialpatid,
                      trim(format('%11d', stg.anespresent)) AS anespresent,
                      trim(format('%11d', stg.cosurgeon)) AS cosurgeon,
                      trim(stg.server_name) AS SERVER_NAME,
                      trim(stg.full_server_nm) AS full_server_nm,
                      stg.dw_last_update_date_time AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_cases_stg AS stg
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ca_server.server_name)
      LEFT OUTER JOIN
        (SELECT c.patient_sk,
                c.source_patient_id,
                s.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.patid = a_0.source_patient_id
      AND upper(stg.full_server_nm) = upper(a_0.server_name)) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_patient_case.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_case) AS a
);

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count =0 AND tgt_rec_count = 0 Then 0
              ELSE tgt_rec_count
              END;


SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
 {{ params.param_ca_audit_dataset_name }}.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid AS int64), sourcesysnm, srctablename, tgttablename, audit_type, 
  src_rec_count, tgt_rec_count, control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount,
  cast(tableload_start_time AS DATETIME), cast(tableload_end_time AS DATETIME),
  tableload_run_time, job_name, audit_time, audit_status );
END;
