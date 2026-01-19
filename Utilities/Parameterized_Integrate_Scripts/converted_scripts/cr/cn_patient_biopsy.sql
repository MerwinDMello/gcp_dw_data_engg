-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_biopsy.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_BIOPSY	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_PATIENT_BIOPSY_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_BIOPSY;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_BIOPSY_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_biopsy WHERE upper(cn_patient_biopsy.hashbite_ssk) NOT IN(
    SELECT
        upper(cn_patient_biopsy_stg.hashbite_ssk) AS hashbite_ssk
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_biopsy_stg
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
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_biopsy AS mt USING (
    SELECT DISTINCT
        stg.cn_patient_biopsy_sid,
        stg.core_record_type_id,
        stg.med_spcl_physician_id,
        stg.referring_physician_id,
        stg.biopsy_type_id,
        stg.biopsy_result_id,
        fac.facility_id,
        l.site_location_id,
        s.physician_specialty_id,
        stg.nav_patient_id,
        stg.tumor_type_id,
        stg.diagnosis_result_id,
        stg.nav_diagnosis_id,
        stg.navigator_id,
        stg.coid AS coid,
        stg.company_code AS company_code,
        stg.biopsy_date,
        stg.biopsy_clip_sw,
        stg.biopsy_needle_sw,
        stg.general_biopsy_type_text,
        stg.comment_text,
        stg.hashbite_ssk,
        stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_biopsy_stg AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_facility AS fac ON upper(rtrim(stg.biopsyfacility)) = upper(rtrim(fac.facility_name))
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_site_location AS l ON upper(rtrim(stg.biopsysite)) = upper(rtrim(l.site_location_desc))
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_physician_specialty AS s ON upper(rtrim(stg.biopsyphysiciantype)) = upper(rtrim(s.physician_specialty_desc))
      WHERE upper(stg.hashbite_ssk) NOT IN(
        SELECT
            upper(cn_patient_biopsy.hashbite_ssk) AS hashbite_ssk
          FROM
            `hca-hin-dev-cur-ops`.edwcr.cn_patient_biopsy
      )
  ) AS ms
  ON mt.cn_patient_biopsy_sid = ms.cn_patient_biopsy_sid
   AND (coalesce(mt.core_record_type_id, 0) = coalesce(ms.core_record_type_id, 0)
   AND coalesce(mt.core_record_type_id, 1) = coalesce(ms.core_record_type_id, 1))
   AND (coalesce(mt.med_spcl_physician_id, 0) = coalesce(ms.med_spcl_physician_id, 0)
   AND coalesce(mt.med_spcl_physician_id, 1) = coalesce(ms.med_spcl_physician_id, 1))
   AND (coalesce(mt.referring_physician_id, 0) = coalesce(ms.referring_physician_id, 0)
   AND coalesce(mt.referring_physician_id, 1) = coalesce(ms.referring_physician_id, 1))
   AND (coalesce(mt.biopsy_type_id, 0) = coalesce(ms.biopsy_type_id, 0)
   AND coalesce(mt.biopsy_type_id, 1) = coalesce(ms.biopsy_type_id, 1))
   AND (coalesce(mt.biopsy_result_id, 0) = coalesce(ms.biopsy_result_id, 0)
   AND coalesce(mt.biopsy_result_id, 1) = coalesce(ms.biopsy_result_id, 1))
   AND (coalesce(mt.biopsy_facility_id, 0) = coalesce(ms.facility_id, 0)
   AND coalesce(mt.biopsy_facility_id, 1) = coalesce(ms.facility_id, 1))
   AND (coalesce(mt.biopsy_site_location_id, 0) = coalesce(ms.site_location_id, 0)
   AND coalesce(mt.biopsy_site_location_id, 1) = coalesce(ms.site_location_id, 1))
   AND (coalesce(mt.biopsy_physician_specialty_id, 0) = coalesce(ms.physician_specialty_id, 0)
   AND coalesce(mt.biopsy_physician_specialty_id, 1) = coalesce(ms.physician_specialty_id, 1))
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
   AND (coalesce(mt.biopsy_date, DATE '1970-01-01') = coalesce(ms.biopsy_date, DATE '1970-01-01')
   AND coalesce(mt.biopsy_date, DATE '1970-01-02') = coalesce(ms.biopsy_date, DATE '1970-01-02'))
   AND (coalesce(mt.biopsy_clip_sw, 0) = coalesce(ms.biopsy_clip_sw, 0)
   AND coalesce(mt.biopsy_clip_sw, 1) = coalesce(ms.biopsy_clip_sw, 1))
   AND (coalesce(mt.biopsy_needle_sw, 0) = coalesce(ms.biopsy_needle_sw, 0)
   AND coalesce(mt.biopsy_needle_sw, 1) = coalesce(ms.biopsy_needle_sw, 1))
   AND (upper(coalesce(mt.general_biopsy_type_text, '0')) = upper(coalesce(ms.general_biopsy_type_text, '0'))
   AND upper(coalesce(mt.general_biopsy_type_text, '1')) = upper(coalesce(ms.general_biopsy_type_text, '1')))
   AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
   AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_biopsy_sid, core_record_type_id, med_spcl_physician_id, referring_physician_id, biopsy_type_id, biopsy_result_id, biopsy_facility_id, biopsy_site_location_id, biopsy_physician_specialty_id, nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id, coid, company_code, biopsy_date, biopsy_clip_sw, biopsy_needle_sw, general_biopsy_type_text, comment_text, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_biopsy_sid, ms.core_record_type_id, ms.med_spcl_physician_id, ms.referring_physician_id, ms.biopsy_type_id, ms.biopsy_result_id, ms.facility_id, ms.site_location_id, ms.physician_specialty_id, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.biopsy_date, ms.biopsy_clip_sw, ms.biopsy_needle_sw, ms.general_biopsy_type_text, ms.comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_BIOPSY');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
