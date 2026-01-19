-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_heme_disease_assess.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_PATIENT_HEME_DISEASE_ASSESS                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :       EDWCR_staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_HEME_DISEASE_ASSESS;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','PATIENT_HEME_DISEASE_ASSESSMENT_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_disease_assess AS mt USING (
    SELECT DISTINCT
        CAST(iq.cn_patient_heme_sid as INT64) AS cn_patient_heme_sid,
        iq.test_type_id,
        iq.sample_type_id,
        iq.disease_assess_source_id,
        iq.disease_assess_facility_id,
        CAST(ROUND(iq.nav_patient_id, 0, 'ROUND_HALF_EVEN') as NUMERIC) AS nav_patient_id,
        CAST(iq.tumor_type_id as INT64) AS tumor_type_id,
        CAST(iq.diagnosis_result_id as INT64) AS diagnosis_result_id,
        CAST(iq.nav_diagnosis_id as INT64) AS nav_diagnosis_id,
        CAST(iq.navigator_id as INT64) AS navigator_id,
        iq.coid AS coid,
        iq.company_code AS company_code,
        iq.collection_date,
        iq.disease_status_id,
        iq.treatment_status_id,
        iq.initial_diagnosis_ind AS initial_diagnosis_ind,
        iq.disease_monitoring_ind AS disease_monitoring_ind,
        substr(iq.comment_text, 1, 1000) AS comment_text,
        substr(iq.hashbite_ssk, 1, 60) AS hashbite_ssk,
        iq.source_system_code AS source_system_code,
        iq.dw_last_update_date_time
      FROM
        (
          SELECT
              phd.patienthemefactid AS cn_patient_heme_sid,
              rt.test_type_id AS test_type_id,
              rst.sample_type_id AS sample_type_id,
              rda.disease_assess_source_id AS disease_assess_source_id,
              rf.facility_id AS disease_assess_facility_id,
              phd.patientdimid AS nav_patient_id,
              phd.tumortypedimid AS tumor_type_id,
              phd.diagnosisresultid AS diagnosis_result_id,
              phd.diagnosisdimid AS nav_diagnosis_id,
              phd.navigatordimid AS navigator_id,
              phd.coid AS coid,
              'H' AS company_code,
              parse_date('%Y-%m-%d', trim(phd.collectiondate)) AS collection_date,
              rs.status_id AS disease_status_id,
              rss.status_id AS treatment_status_id,
              CASE
                 upper(trim(phd.initialdiagnosis))
                WHEN 'YES' THEN 'Y'
                WHEN 'NO' THEN 'N'
                ELSE 'U'
              END AS initial_diagnosis_ind,
              CASE
                 upper(trim(phd.diseasemonitoring))
                WHEN 'YES' THEN 'Y'
                WHEN 'NO' THEN 'N'
                ELSE 'U'
              END AS disease_monitoring_ind,
              trim(phd.comments) AS comment_text,
              trim(phd.hbsource) AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg AS phd
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_test_type AS rt ON upper(trim(phd.testtype)) = upper(trim(rt.test_sub_type_desc))
               AND upper(trim(rt.test_type_desc)) = 'DISEASE ASSESSMENT'
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_sample_type AS rst ON upper(trim(phd.samplesourcetype)) = upper(trim(rst.sample_type_name))
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_disease_assess_source AS rda ON upper(trim(phd.source)) = upper(trim(rda.disease_assess_source_name))
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_facility AS rf ON upper(trim(phd.facilityname)) = upper(trim(rf.facility_name))
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS rs ON upper(trim(phd.diseasestatus)) = upper(trim(rs.status_desc))
               AND upper(trim(rs.status_type_desc)) = 'DISEASE'
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS rss ON upper(trim(phd.treatementstatus)) = upper(trim(rss.status_desc))
               AND upper(trim(rss.status_type_desc)) = 'TREATMENT'
        ) AS iq
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_heme_disease_assess AS cphd ON upper(trim(iq.hashbite_ssk)) = upper(trim(cphd.hashbite_ssk))
      WHERE trim(cphd.hashbite_ssk) IS NULL
  ) AS ms
  ON mt.cn_patient_heme_sid = ms.cn_patient_heme_sid
   AND (coalesce(mt.test_type_id, 0) = coalesce(ms.test_type_id, 0)
   AND coalesce(mt.test_type_id, 1) = coalesce(ms.test_type_id, 1))
   AND (coalesce(mt.sample_type_id, 0) = coalesce(ms.sample_type_id, 0)
   AND coalesce(mt.sample_type_id, 1) = coalesce(ms.sample_type_id, 1))
   AND (coalesce(mt.disease_assess_source_id, 0) = coalesce(ms.disease_assess_source_id, 0)
   AND coalesce(mt.disease_assess_source_id, 1) = coalesce(ms.disease_assess_source_id, 1))
   AND (coalesce(mt.disease_assess_facility_id, 0) = coalesce(ms.disease_assess_facility_id, 0)
   AND coalesce(mt.disease_assess_facility_id, 1) = coalesce(ms.disease_assess_facility_id, 1))
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
   AND mt.coid = ms.coid
   AND mt.company_code = ms.company_code
   AND (coalesce(mt.collection_date, DATE '1970-01-01') = coalesce(ms.collection_date, DATE '1970-01-01')
   AND coalesce(mt.collection_date, DATE '1970-01-02') = coalesce(ms.collection_date, DATE '1970-01-02'))
   AND (coalesce(mt.disease_status_id, 0) = coalesce(ms.disease_status_id, 0)
   AND coalesce(mt.disease_status_id, 1) = coalesce(ms.disease_status_id, 1))
   AND (coalesce(mt.treatment_status_id, 0) = coalesce(ms.treatment_status_id, 0)
   AND coalesce(mt.treatment_status_id, 1) = coalesce(ms.treatment_status_id, 1))
   AND (upper(coalesce(mt.initial_diagnosis_ind, '0')) = upper(coalesce(ms.initial_diagnosis_ind, '0'))
   AND upper(coalesce(mt.initial_diagnosis_ind, '1')) = upper(coalesce(ms.initial_diagnosis_ind, '1')))
   AND (upper(coalesce(mt.disease_monitoring_ind, '0')) = upper(coalesce(ms.disease_monitoring_ind, '0'))
   AND upper(coalesce(mt.disease_monitoring_ind, '1')) = upper(coalesce(ms.disease_monitoring_ind, '1')))
   AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
   AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_heme_sid, test_type_id, sample_type_id, disease_assess_source_id, disease_assess_facility_id, nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id, coid, company_code, collection_date, disease_status_id, treatment_status_id, initial_diagnosis_ind, disease_monitoring_ind, comment_text, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_heme_sid, ms.test_type_id, ms.sample_type_id, ms.disease_assess_source_id, ms.disease_assess_facility_id, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.collection_date, ms.disease_status_id, ms.treatment_status_id, ms.initial_diagnosis_ind, ms.disease_monitoring_ind, ms.comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
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
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_disease_assess AS cphd WHERE NOT EXISTS (
    SELECT
        'X'
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg AS sphd
      WHERE upper(trim(sphd.hbsource)) = upper(trim(cphd.hashbite_ssk))
  );
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_PATIENT_HEME_DISEASE_ASSESS');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
