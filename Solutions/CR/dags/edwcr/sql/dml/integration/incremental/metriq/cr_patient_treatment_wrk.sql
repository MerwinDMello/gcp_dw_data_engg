DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_treatment_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CR_PATIENT_TREATMENT_WRK             	    #
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 				    #
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_TREATMENT		    #
-- #	                                                                           #
-- #  INITIAL RELEASE	   	: 						    #
-- #  PROJECT             	: 	 		    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                                  #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_TREATMENT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_TREATMENT_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_TREATMENT_STG1');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_wrk1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate WRK Table */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_wrk1 (treatmentid, tumorid, treatmenthospid, rxcode, description, groupid, othercode, surgmarg, rxtype, dtprotocol, dtrxstart, prottitle, rxtxt, rxmd1, source_system_code, dw_last_update_date_time)
SELECT trt.treatmentid,
       trt.tumorid,
       trt.treatmenthospid,
       trt.rxcode,
       substr(trt1.description, 1, 10) AS description,
       substr(format('%11d', trt1.groupid), 1, 10) AS groupid,
       trt.othercode,
       trt.surgmarg,
       trt.rxtype,
      --  (extract(YEAR
      --           FROM trt.dtprotocol) - 1900) * 10000 + extract(MONTH
      --                                                          FROM trt.dtprotocol) * 100 + extract(DAY
      --                                                                                               FROM trt.dtprotocol) AS dtprotocol,
      --  (extract(YEAR
      --           FROM trt.dtrxstart) - 1900) * 10000 + extract(MONTH
      --                                                         FROM trt.dtrxstart) * 100 + extract(DAY
      --                                                                                             FROM trt.dtrxstart) AS dtrxstart,
       trt.dtprotocol,
       trt.dtrxstart,
       trt.prottitle,
       trt.rxtxt,
       trt.rxmd1,
       'M' AS source_system_code,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_stg AS trt
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_stg1 AS trt1 ON trt.treatmentid = trt1.treatmentid
AND upper(rtrim(trt.rxcode)) = upper(rtrim(trt1.rxcode));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_TREATMENT_WRK1');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_wrk (treatment_id, tumor_id, treatment_hospital_id, treatment_type_id, surgical_site_id, surgical_margin_result_id, treatment_type_group_id, clinical_trial_start_date, treatment_start_date, clinical_trial_text, comment_text, treatment_performing_physician_code, source_system_code, dw_last_update_date_time)
SELECT src.treatment_id,
       src.tumor_id,
       rh.hospital_id AS treatment_hospital_id,
       tt.treatment_type_id AS treatment_type_id, -- COALESCE(RLC.MASTER_LOOKUP_SID,(SELECT MASTER_LOOKUP_SID FROM EDWCR.REF_LOOKUP_CODE WHERE LOOKUP_CODE=-99))  AS SURGICAL_SITE_ID,
 coalesce(lkp.master_lookup_sid,
            (SELECT rlc.master_lookup_sid
             FROM {{ params.param_cr_core_dataset_name }}.ref_lookup_name AS rln
             INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
             WHERE upper(rtrim(rln.lookup_name)) = 'SURGERY SITE'
               AND upper(rtrim(rlc.lookup_code)) = '-99' )) AS surgical_site_id, -- COALESCE(RLC1.MASTER_LOOKUP_SID,(SELECT MASTER_LOOKUP_SID FROM EDWCR.REF_LOOKUP_CODE WHERE LOOKUP_CODE=-99))  AS SURGICAL_MARGIN_RESULT_ID,
 coalesce(lkp1.master_lookup_sid,
            (SELECT rlc1.master_lookup_sid
             FROM {{ params.param_cr_core_dataset_name }}.ref_lookup_name AS rln1
             INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS rlc1 ON rlc1.lookup_id = rln1.lookup_sid
             WHERE upper(rtrim(rln1.lookup_name)) = 'SURGERY MARGIN RESULT'
               AND upper(rtrim(rlc1.lookup_code)) = '-99' )) AS surgical_margin_result_id,
 tgtp.treatment_type_group_id AS treatment_type_group_id,
--  date_add(date_add(date_add(DATE '1900-01-01', interval div(src.dtprotocol, 10000) YEAR), interval div(mod(src.dtprotocol, 10000), 100) - 1 MONTH), interval mod(src.dtprotocol, 100) - 1 DAY) AS clinical_trial_start_date,
--  date_add(date_add(date_add(DATE '1900-01-01', interval div(src.dtrxstart, 10000) YEAR), interval div(mod(src.dtrxstart, 10000), 100) - 1 MONTH), interval mod(src.dtrxstart, 100) - 1 DAY) AS treatment_start_date,
 src.dtprotocol AS clinical_trial_start_date,
 src.dtrxstart AS treatment_start_date,
 substr(src.clinical_trial_text, 1, 1000) AS clinical_trial_text,
 substr(src.comment_text, 1, 4000) AS comment_text,
 src.treatment_performing_physician AS treatment_performing_physician_code,
 src.source_system_code,
 src.dw_last_update_date_time
FROM
  (SELECT stg.treatmentid AS treatment_id,
          stg.tumorid AS tumor_id,
          trim(stg.treatmenthospid) AS treatmenthospid,
          trim(stg.rxcode) AS rxcode,
          trim(stg.description) AS description,
          trim(stg.groupid) AS groupid,
          trim(stg.othercode) AS othercode,
          trim(stg.surgmarg) AS surgmarg,
          trim(stg.rxtype) AS rxtype,
          stg.dtprotocol AS dtprotocol,
          stg.dtrxstart AS dtrxstart,
          trim(stg.prottitle) AS clinical_trial_text,
          trim(stg.rxtxt) AS comment_text,
          trim(stg.rxmd1) AS treatment_performing_physician,
          stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_wrk1 AS stg) AS src
LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_hospital AS rh ON upper(rtrim(src.treatmenthospid)) = upper(rtrim(rh.hospital_code))
LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_cr_treatment_type AS tt ON upper(rtrim(src.rxcode)) = upper(rtrim(tt.treatment_type_code))
AND CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(src.groupid) AS FLOAT64) = tt.treatment_group_id
LEFT OUTER JOIN -- AND SRC.DESCRIPTION=TT.TREATMENT_TYPE_DESC
 /*LEFT JOIN  EDWCR.REF_LOOKUP_CODE RLC
        ON SRC.OTHERCODE=RLC.LOOKUP_CODE
        LEFT JOIN EDWCR.REF_LOOKUP_NAME NM
        ON RLC.LOOKUP_ID=NM.LOOKUP_SID
        AND NM.LOOKUP_NAME='SURGERY SITE'*/ /*LEFT JOIN  EDWCR.REF_LOOKUP_CODE RLC1
        ON SRC.SURGMARG=RLC1.LOOKUP_CODE
        LEFT JOIN EDWCR.REF_LOOKUP_NAME NM1
        ON RLC1.LOOKUP_ID=NM1.LOOKUP_SID
        AND NM1.LOOKUP_NAME='SURGERY MARGIN RESULT'*/ {{ params.param_cr_core_dataset_name }}.ref_treatment_type_group AS tgtp ON upper(rtrim(src.rxtype)) = upper(rtrim(tgtp.treatment_type_group_code))
LEFT OUTER JOIN
  (SELECT rln.lookup_sid,
          rlc.lookup_code,
          rlc.lookup_sub_code,
          rlc.master_lookup_sid
   FROM {{ params.param_cr_core_dataset_name }}.ref_lookup_name AS rln
   INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
   WHERE upper(rtrim(rln.lookup_name)) = 'SURGERY SITE' ) AS lkp ON upper(rtrim(src.othercode)) = upper(rtrim(lkp.lookup_code))
LEFT OUTER JOIN
  (SELECT rln.lookup_sid,
          rlc.lookup_code,
          rlc.lookup_sub_code,
          rlc.master_lookup_sid
   FROM {{ params.param_cr_core_dataset_name }}.ref_lookup_name AS rln
   INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
   AND upper(rtrim(rln.lookup_name)) = 'SURGERY MARGIN RESULT') AS lkp1 ON upper(rtrim(src.surgmarg)) = upper(rtrim(lkp1.lookup_code));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_TREATMENT_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF