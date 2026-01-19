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
     (SELECT DISTINCT trim(format('%11d', ca_server.server_sk)) AS server_sk,
                      trim(format('%11d', stg.patid)) AS patid,
                      trim(format('%11d', stg.hospitalid)) AS hospitalid,
                      trim(stg.hospname) AS hospname,
                      trim(stg.patnationalid) AS patnationalid,
                      trim(stg.stsvendorid) AS stsvendorid,
                      trim(stg.medrecn) AS medrecn,
                      trim(stg.patlname) AS patlname,
                      trim(stg.patfname) AS patfname,
                      trim(stg.patminit) AS patminit,
                      stg.dob AS dob,
                      stg.timeofbirth AS timeofbirth,
                      trim(stg.ssn) AS ssn,
                      trim(format('%11d', stg.gender)) AS gender,
                      trim(stg.pataddr) AS pataddr,
                      trim(stg.pataddr2) AS pataddr2,
                      trim(stg.patcity) AS patcity,
                      trim(stg.patstate) AS patstate,
                      trim(stg.patzip) AS patzip,
                      trim(stg.patphone) AS patphone,
                      trim(stg.patfax) AS patfax,
                      trim(stg.patwphone) AS patwphone,
                      trim(stg.patcphone) AS patcphone,
                      trim(stg.patemail) AS patemail,
                      trim(stg.county) AS county,
                      trim(format('%11d', stg.country)) AS country,
                      trim(stg.patforeign) AS patforeign,
                      trim(stg.pager) AS pager,
                      trim(stg.demogdatavrsn) AS demogdatavrsn,
                      trim(stg.datavrsn) AS data_version_code,
                      trim(format('%11d', stg.ethnicity)) AS ethnicity_id,
                      trim(format('%11d', stg.race)) AS race_id,
                      trim(format('%11d', stg.racecaucasian)) AS racecaucasian,
                      trim(format('%11d', stg.raceblack)) AS raceblack,
                      trim(format('%11d', stg.racehispanic)) AS racehispanic,
                      trim(format('%11d', stg.raceasian)) AS raceasian,
                      trim(format('%11d', stg.racenativeam)) AS racenativeam,
                      trim(format('%11d', stg.racenativepacific)) AS racenativepacific,
                      trim(format('%11d', stg.raceother)) AS raceother,
                      trim(stg.raceothspcfy) AS raceothspcfy,
                      trim(stg.birthcit) AS birthcit,
                      trim(stg.birthsta) AS birthsta,
                      trim(format('%11d', stg.birthcou)) AS birthcou,
                      trim(CAST(stg.birthwtkg AS STRING)) AS birth_weight_kg_amt,
                      trim(stg.allergies) AS allergy_text,
                      trim(format('%11d', stg.premature)) AS premature_birth_id,
                      trim(format('%11d', stg.gestageweeks)) AS gestational_age_week_num,
                      trim(format('%11d', stg.organdonor)) AS organ_donor_id,
                      stg.createdate AS source_create_date_time,
                      stg.lastupdate AS source_last_update_date_time,
                      trim(stg.updatedby) AS updated_by_3_4_id,
                      trim(format('%11d', stg.zipcodena)) AS zipcodena,
                      trim(stg.patmname) AS patient_middle_name,
                      trim(format('%11d', stg.ssnna)) AS social_security_num,
                      trim(stg.aux1) AS aux1,
                      trim(stg.aux2) AS aux2,
                      trim(format('%11d', stg.accexclude)) AS accexclude,
                      trim(stg.patguid) AS patguid,
                      trim(format('%11d', stg.ethnicityrecorded)) AS ethnicityrecorded,
                      trim(stg.pc4vendorcode) AS pc4vendorcode,
                      trim(stg.pc4datavrsn) AS pc4datavrsn,
                      trim(stg.birthzip) AS birthzip,
                      trim(format('%11d', stg.matnameknown)) AS matnameknown,
                      trim(format('%11d', stg.stscongdvconv)) AS stscongdvconv,
                      trim(format('%11d', stg.racedocumented)) AS racedocumented,
                      trim(format('%11d', stg.birthlocknown)) AS birth_location_available_id,
                      trim(format('%11d', stg.raceasianindian)) AS raceasianindian,
                      trim(format('%11d', stg.racechinese)) AS racechinese,
                      trim(format('%11d', stg.racefilipino)) AS racefilipino,
                      trim(format('%11d', stg.racejapanese)) AS racejapanese,
                      trim(format('%11d', stg.racekorean)) AS racekorean,
                      trim(format('%11d', stg.racevietnamese)) AS racevietnamese,
                      trim(format('%11d', stg.raceasianother)) AS raceasianother,
                      trim(format('%11d', stg.racenativehawaii)) AS racenativehawaii,
                      trim(format('%11d', stg.raceguamchamorro)) AS raceguamchamorro,
                      trim(format('%11d', stg.racesamoan)) AS racesamoan,
                      trim(format('%11d', stg.racepacificislandother)) AS racepacificislandother,
                      trim(format('%11d', stg.hispethnicitymexican)) AS hispethnicitymexican,
                      trim(format('%11d', stg.hispethnicitypuertorico)) AS hispethnicitypuertorico,
                      trim(format('%11d', stg.hispethnicitycuban)) AS hispethnicitycuban,
                      trim(format('%11d', stg.hispethnicityotherorigin)) AS hispethnicityotherorigin,
                      trim(stg.server_name) AS SERVER_NAME,
                      trim(stg.full_server_nm) AS full_server_nm,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_demographics_stg AS stg
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ca_server.server_name)) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_patient.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient) AS a
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
