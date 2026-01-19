-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_radiation_oncology.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_RADIATION_ONCOLOGY	              	#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_PATIENT_RADIATION_ONCOLOGY#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_RADIATION_ONCOLOGY;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_RADIATION_ONCOLOGY_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_radiation_oncology WHERE upper(trim(cn_patient_radiation_oncology.hashbite_ssk)) NOT IN(
    SELECT
        upper(trim(cn_patient_radiation_oncology_stg.hashbite_ssk))
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_radiation_oncology_stg
  );
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Insert the new records into the Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_radiation_oncology AS mt USING (
    SELECT DISTINCT
        stg.cn_patient_rad_oncology_sid,
        loc.site_location_id,
        stg.treatment_type_id,
        lob_loc.lung_lobe_location_id,
        fac.facility_id,
        stg.nav_patient_id,
        stg.core_record_type_id,
        stg.med_spcl_physician_id,
        stg.tumor_type_id,
        stg.diagnosis_result_id,
        stg.nav_diagnosis_id,
        stg.navigator_id,
        stg.coid AS coid,
        stg.company_code AS company_code,
        stg.core_record_date,
        stg.treatment_start_date,
        stg.treatment_end_date,
        stg.treatment_fractions_num,
        stg.elapse_ind AS elapse_ind,
        stg.elapse_start_date,
        stg.elapse_end_date,
        stg.radiation_oncology_reason_text,
        stg.palliative_ind AS palliative_ind,
        stg.treatment_therapy_schedule_cd AS treatment_therapy_schedule_code,
        substr(trim(stg.comment_text), 1, 4000) AS comment_text,
        stg.hashbite_ssk,
        stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_radiation_oncology_stg AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_site_location AS loc ON upper(trim(stg.treatment_site_location_id)) = upper(trim(loc.site_location_desc))
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lung_lobe_location AS lob_loc ON upper(trim(stg.lung_lobe_location_id)) = upper(trim(lob_loc.lung_lobe_location_desc))
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_facility AS fac ON upper(trim(stg.radiation_oncology_facility_id)) = upper(trim(fac.facility_name))
      WHERE upper(stg.hashbite_ssk) NOT IN(
        SELECT
            upper(cn_patient_radiation_oncology.hashbite_ssk) AS hashbite_ssk
          FROM
            `hca-hin-dev-cur-ops`.edwcr.cn_patient_radiation_oncology
      )
  ) AS ms
  ON mt.cn_patient_radiation_oncology_sid = ms.cn_patient_rad_oncology_sid
   AND (coalesce(mt.treatment_site_location_id, 0) = coalesce(ms.site_location_id, 0)
   AND coalesce(mt.treatment_site_location_id, 1) = coalesce(ms.site_location_id, 1))
   AND (coalesce(mt.treatment_type_id, 0) = coalesce(ms.treatment_type_id, 0)
   AND coalesce(mt.treatment_type_id, 1) = coalesce(ms.treatment_type_id, 1))
   AND (coalesce(mt.lung_lobe_location_id, 0) = coalesce(ms.lung_lobe_location_id, 0)
   AND coalesce(mt.lung_lobe_location_id, 1) = coalesce(ms.lung_lobe_location_id, 1))
   AND (coalesce(mt.radiation_oncology_facility_id, 0) = coalesce(ms.facility_id, 0)
   AND coalesce(mt.radiation_oncology_facility_id, 1) = coalesce(ms.facility_id, 1))
   AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
   AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
   AND (coalesce(mt.core_record_type_id, 0) = coalesce(ms.core_record_type_id, 0)
   AND coalesce(mt.core_record_type_id, 1) = coalesce(ms.core_record_type_id, 1))
   AND (coalesce(mt.med_spcl_physician_id, 0) = coalesce(ms.med_spcl_physician_id, 0)
   AND coalesce(mt.med_spcl_physician_id, 1) = coalesce(ms.med_spcl_physician_id, 1))
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
   AND (coalesce(mt.core_record_date, DATE '1970-01-01') = coalesce(ms.core_record_date, DATE '1970-01-01')
   AND coalesce(mt.core_record_date, DATE '1970-01-02') = coalesce(ms.core_record_date, DATE '1970-01-02'))
   AND (coalesce(mt.treatment_start_date, DATE '1970-01-01') = coalesce(ms.treatment_start_date, DATE '1970-01-01')
   AND coalesce(mt.treatment_start_date, DATE '1970-01-02') = coalesce(ms.treatment_start_date, DATE '1970-01-02'))
   AND (coalesce(mt.treatment_end_date, DATE '1970-01-01') = coalesce(ms.treatment_end_date, DATE '1970-01-01')
   AND coalesce(mt.treatment_end_date, DATE '1970-01-02') = coalesce(ms.treatment_end_date, DATE '1970-01-02'))
   AND (coalesce(mt.treatment_fractions_num, NUMERIC '0') = coalesce(ms.treatment_fractions_num, NUMERIC '0')
   AND coalesce(mt.treatment_fractions_num, NUMERIC '1') = coalesce(ms.treatment_fractions_num, NUMERIC '1'))
   AND (upper(coalesce(mt.elapse_ind, '0')) = upper(coalesce(ms.elapse_ind, '0'))
   AND upper(coalesce(mt.elapse_ind, '1')) = upper(coalesce(ms.elapse_ind, '1')))
   AND (coalesce(mt.elapse_start_date, DATE '1970-01-01') = coalesce(ms.elapse_start_date, DATE '1970-01-01')
   AND coalesce(mt.elapse_start_date, DATE '1970-01-02') = coalesce(ms.elapse_start_date, DATE '1970-01-02'))
   AND (coalesce(mt.elapse_end_date, DATE '1970-01-01') = coalesce(ms.elapse_end_date, DATE '1970-01-01')
   AND coalesce(mt.elapse_end_date, DATE '1970-01-02') = coalesce(ms.elapse_end_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.radiation_oncology_reason_text, '0')) = upper(coalesce(ms.radiation_oncology_reason_text, '0'))
   AND upper(coalesce(mt.radiation_oncology_reason_text, '1')) = upper(coalesce(ms.radiation_oncology_reason_text, '1')))
   AND (upper(coalesce(mt.palliative_ind, '0')) = upper(coalesce(ms.palliative_ind, '0'))
   AND upper(coalesce(mt.palliative_ind, '1')) = upper(coalesce(ms.palliative_ind, '1')))
   AND (upper(coalesce(mt.treatment_therapy_schedule_code, '0')) = upper(coalesce(ms.treatment_therapy_schedule_code, '0'))
   AND upper(coalesce(mt.treatment_therapy_schedule_code, '1')) = upper(coalesce(ms.treatment_therapy_schedule_code, '1')))
   AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
   AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_radiation_oncology_sid, treatment_site_location_id, treatment_type_id, lung_lobe_location_id, radiation_oncology_facility_id, nav_patient_id, core_record_type_id, med_spcl_physician_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id, coid, company_code, core_record_date, treatment_start_date, treatment_end_date, treatment_fractions_num, elapse_ind, elapse_start_date, elapse_end_date, radiation_oncology_reason_text, palliative_ind, treatment_therapy_schedule_code, comment_text, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_rad_oncology_sid, ms.site_location_id, ms.treatment_type_id, ms.lung_lobe_location_id, ms.facility_id, ms.nav_patient_id, ms.core_record_type_id, ms.med_spcl_physician_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.core_record_date, ms.treatment_start_date, ms.treatment_end_date, ms.treatment_fractions_num, ms.elapse_ind, ms.elapse_start_date, ms.elapse_end_date, ms.radiation_oncology_reason_text, ms.palliative_ind, ms.treatment_therapy_schedule_code, ms.comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_RADIATION_ONCOLOGY');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
