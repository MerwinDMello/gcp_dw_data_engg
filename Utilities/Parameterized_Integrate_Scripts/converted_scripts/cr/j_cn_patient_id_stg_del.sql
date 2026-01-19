-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_patient_id_stg_del.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Patient_ID_Input		   		   ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : EDWCR_staging.Scri_Patient_ID_History		   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 								   ##
-- ##  PROJECT            	   : 	 		    				   ##
-- ##  ------------------------------------------------------------------------	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_Patient_ID_Input;' FOR SESSION;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.scri_patient_id_history;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cdm_synthesys_patientid_results;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
