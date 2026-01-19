-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_person.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_PERSON	                        ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   : EDWCR_STAGING.CN_PERSON_Stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PERSON;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PERSON_Stg');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_person WHERE cn_person.nav_patient_id NOT IN(
    SELECT
        cn_person_stg.nav_patient_id
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_person_stg
  );
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_person AS mt USING (
    SELECT DISTINCT
        cn_person_stg.nav_patient_id,
        cn_person_stg.birth_date,
        cn_person_stg.first_name,
        cn_person_stg.last_name,
        cn_person_stg.middle_name,
        cn_person_stg.perferred_name,
        cn_person_stg.gender_code AS gender_code,
        cn_person_stg.preferred_langauage_text,
        cn_person_stg.death_date,
        cn_person_stg.patient_email_text,
        'N' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_person_stg
      WHERE cn_person_stg.nav_patient_id NOT IN(
        SELECT
            cn_person.nav_patient_id
          FROM
            `hca-hin-dev-cur-ops`.edwcr.cn_person
      )
  ) AS ms
  ON mt.nav_patient_id = ms.nav_patient_id
   AND (coalesce(mt.birth_date, DATE '1970-01-01') = coalesce(ms.birth_date, DATE '1970-01-01')
   AND coalesce(mt.birth_date, DATE '1970-01-02') = coalesce(ms.birth_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.first_name, '0')) = upper(coalesce(ms.first_name, '0'))
   AND upper(coalesce(mt.first_name, '1')) = upper(coalesce(ms.first_name, '1')))
   AND (upper(coalesce(mt.last_name, '0')) = upper(coalesce(ms.last_name, '0'))
   AND upper(coalesce(mt.last_name, '1')) = upper(coalesce(ms.last_name, '1')))
   AND (upper(coalesce(mt.middle_name, '0')) = upper(coalesce(ms.middle_name, '0'))
   AND upper(coalesce(mt.middle_name, '1')) = upper(coalesce(ms.middle_name, '1')))
   AND (upper(coalesce(mt.preferred_name, '0')) = upper(coalesce(ms.perferred_name, '0'))
   AND upper(coalesce(mt.preferred_name, '1')) = upper(coalesce(ms.perferred_name, '1')))
   AND (upper(coalesce(mt.gender_code, '0')) = upper(coalesce(ms.gender_code, '0'))
   AND upper(coalesce(mt.gender_code, '1')) = upper(coalesce(ms.gender_code, '1')))
   AND (upper(coalesce(mt.preferred_language_text, '0')) = upper(coalesce(ms.preferred_langauage_text, '0'))
   AND upper(coalesce(mt.preferred_language_text, '1')) = upper(coalesce(ms.preferred_langauage_text, '1')))
   AND (coalesce(mt.death_date, DATE '1970-01-01') = coalesce(ms.death_date, DATE '1970-01-01')
   AND coalesce(mt.death_date, DATE '1970-01-02') = coalesce(ms.death_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.patient_email_text, '0')) = upper(coalesce(ms.patient_email_text, '0'))
   AND upper(coalesce(mt.patient_email_text, '1')) = upper(coalesce(ms.patient_email_text, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (nav_patient_id, birth_date, first_name, last_name, middle_name, preferred_name, gender_code, preferred_language_text, death_date, patient_email_text, source_system_code, dw_last_update_date_time)
      VALUES (ms.nav_patient_id, ms.birth_date, ms.first_name, ms.last_name, ms.middle_name, ms.perferred_name, ms.gender_code, ms.preferred_langauage_text, ms.death_date, ms.patient_email_text, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_PERSON');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
