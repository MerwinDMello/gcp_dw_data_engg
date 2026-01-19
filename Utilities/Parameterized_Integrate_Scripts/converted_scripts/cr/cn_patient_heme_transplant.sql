-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_heme_transplant.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_PATIENT_HEME_TRANSPLANT                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :       EDWCR_staging.stg_PatientHemeTransplant     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_HEME_TRANSPLANT;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_PatientHemeTransplant');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_transplant AS mt USING (
    SELECT DISTINCT
        iq.cn_patient_heme_sid AS cn_patient_heme_sid,
        iq.transplant_type_id,
        iq.cellular_therapy_status_id,
        CAST(iq.nav_patient_id as NUMERIC) AS nav_patient_id,
        iq.tumor_type_id AS tumor_type_id,
        iq.diagnosis_result_id AS diagnosis_result_id,
        iq.nav_diagnosis_id AS nav_diagnosis_id,
        iq.navigator_id AS navigator_id,
        iq.med_spcl_physician_id AS med_spcl_physician_id,
        iq.coid AS coid,
        iq.company_code AS company_code,
        iq.cellular_therapy_status_date,
        substr(iq.cellular_therapy_comment_text, 1, 600) AS cellular_therapy_comment_text,
        iq.transfer_date,
        iq.transplant_date,
        substr(iq.transplant_comment_text, 1, 500) AS transplant_comment_text,
        substr(iq.hashbite_ssk, 1, 60) AS hashbite_ssk,
        iq.source_system_code AS source_system_code,
        iq.dw_last_update_date_time
      FROM
        (
          SELECT
              phd.patienthemefactid AS cn_patient_heme_sid,
              rtt.transplant_type_id AS transplant_type_id,
              rs.status_id AS cellular_therapy_status_id,
              phd.patientdimid AS nav_patient_id,
              phd.tumortypedimid AS tumor_type_id,
              phd.diagnosisresultid AS diagnosis_result_id,
              phd.diagnosisdimid AS nav_diagnosis_id,
              phd.navigatordimid AS navigator_id,
              phd.medicalspecialistdimid AS med_spcl_physician_id,
              trim(phd.coid) AS coid,
              'H' AS company_code,
              parse_date('%Y-%m-%d', trim(phd.statusdate)) AS cellular_therapy_status_date,
              trim(phd.candidacycomment) AS cellular_therapy_comment_text,
              parse_date('%Y-%m-%d', trim(phd.transferdate)) AS transfer_date,
              parse_date('%Y-%m-%d', trim(phd.transplantdate)) AS transplant_date,
              trim(phd.transplantcomments) AS transplant_comment_text,
              trim(phd.hbsource) AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_patienthemetransplant AS phd
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_transplant_type AS rtt ON upper(trim(phd.type_type)) = upper(trim(rtt.transplant_type_name))
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS rs ON upper(trim(phd.transplantcandidacystatus)) = upper(trim(rs.status_desc))
               AND upper(rtrim(rs.status_type_desc)) = 'CELLULAR THERAPY'
        ) AS iq
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_heme_transplant AS cpht ON upper(trim(iq.hashbite_ssk)) = upper(trim(cpht.hashbite_ssk))
      WHERE trim(cpht.hashbite_ssk) IS NULL
  ) AS ms
  ON mt.cn_patient_heme_sid = ms.cn_patient_heme_sid
   AND (coalesce(mt.transplant_type_id, 0) = coalesce(ms.transplant_type_id, 0)
   AND coalesce(mt.transplant_type_id, 1) = coalesce(ms.transplant_type_id, 1))
   AND (coalesce(mt.cellular_therapy_status_id, 0) = coalesce(ms.cellular_therapy_status_id, 0)
   AND coalesce(mt.cellular_therapy_status_id, 1) = coalesce(ms.cellular_therapy_status_id, 1))
   AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
   AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
   AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
   AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
   AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
   AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
   AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
   AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
   AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
   AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
   AND (coalesce(mt.med_spcl_physician_id, 0) = coalesce(ms.med_spcl_physician_id, 0)
   AND coalesce(mt.med_spcl_physician_id, 1) = coalesce(ms.med_spcl_physician_id, 1))
   AND mt.coid = ms.coid
   AND mt.company_code = ms.company_code
   AND (coalesce(mt.cellular_therapy_status_date, DATE '1970-01-01') = coalesce(ms.cellular_therapy_status_date, DATE '1970-01-01')
   AND coalesce(mt.cellular_therapy_status_date, DATE '1970-01-02') = coalesce(ms.cellular_therapy_status_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.cellular_therapy_comment_text, '0')) = upper(coalesce(ms.cellular_therapy_comment_text, '0'))
   AND upper(coalesce(mt.cellular_therapy_comment_text, '1')) = upper(coalesce(ms.cellular_therapy_comment_text, '1')))
   AND (coalesce(mt.transfer_date, DATE '1970-01-01') = coalesce(ms.transfer_date, DATE '1970-01-01')
   AND coalesce(mt.transfer_date, DATE '1970-01-02') = coalesce(ms.transfer_date, DATE '1970-01-02'))
   AND (coalesce(mt.transplant_date, DATE '1970-01-01') = coalesce(ms.transplant_date, DATE '1970-01-01')
   AND coalesce(mt.transplant_date, DATE '1970-01-02') = coalesce(ms.transplant_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.transplant_comment_text, '0')) = upper(coalesce(ms.transplant_comment_text, '0'))
   AND upper(coalesce(mt.transplant_comment_text, '1')) = upper(coalesce(ms.transplant_comment_text, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_heme_sid, transplant_type_id, cellular_therapy_status_id, nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id, med_spcl_physician_id, coid, company_code, cellular_therapy_status_date, cellular_therapy_comment_text, transfer_date, transplant_date, transplant_comment_text, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_heme_sid, ms.transplant_type_id, ms.cellular_therapy_status_id, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.med_spcl_physician_id, ms.coid, ms.company_code, ms.cellular_therapy_status_date, ms.cellular_therapy_comment_text, ms.transfer_date, ms.transplant_date, ms.transplant_comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Records From Core table if matching Hashbite_SSK not coming from source
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_transplant AS cpht WHERE NOT EXISTS (
    SELECT
        'X'
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.stg_patienthemetransplant AS sphd
      WHERE upper(trim(sphd.hbsource)) = upper(trim(cpht.hashbite_ssk))
  );
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_PATIENT_HEME_TRANSPLANT');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
