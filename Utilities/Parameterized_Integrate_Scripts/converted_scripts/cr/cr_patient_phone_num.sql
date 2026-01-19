-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_phone_num.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_Patient_Phone_Num             		    #
-- #  TARGET  DATABASE	   	: EDWCR	 					    #
-- #  SOURCE		   	: EDWCR_STAGING.CR_Patient_Phone_Num_Stg	    #
-- #	                                                                            #
-- #  INITIAL RELEASE	   	: 					            #
-- #  PROJECT             	: 	 		    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                              	    #
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_PHONE_NUM;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Phone_Num_Stg');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Truncate Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_phone_num;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Populate Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_phone_num AS mt USING (
    SELECT DISTINCT
        coalesce(cr_patient_phone_num_stg.patient_id, -99) AS patient_id,
        coalesce(cr_patient_phone_num_stg.contact_id, -99) AS contact_id,
        cr_patient_phone_num_stg.phone_num_type_code AS phone_num_type_code,
        cr_patient_phone_num_stg.phone_num,
        cr_patient_phone_num_stg.source_system_code AS source_system_code,
        cr_patient_phone_num_stg.dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_phone_num_stg
  ) AS ms
  ON mt.cr_patient_id = ms.patient_id
   AND mt.patient_contact_id = ms.contact_id
   AND mt.phone_num_type_code = ms.phone_num_type_code
   AND (upper(coalesce(mt.phone_num, '0')) = upper(coalesce(ms.phone_num, '0'))
   AND upper(coalesce(mt.phone_num, '1')) = upper(coalesce(ms.phone_num, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cr_patient_id, patient_contact_id, phone_num_type_code, phone_num, source_system_code, dw_last_update_date_time)
      VALUES (ms.patient_id, ms.contact_id, ms.phone_num_type_code, ms.phone_num, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_Patient_Phone_Num');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
