-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_ref_icd_oncology_diag_cd.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_ICD_Oncology_Diag_Cd		   ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : EDWCR_staging.Ref_ICD_Oncology_Diag_Cd_stg		   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 								   ##
-- ##  PROJECT            	   : 	 		    				   ##
-- ##  ------------------------------------------------------------------------	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_Ref_ICD_Oncology_Diag_Cd;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_ICD_Oncology_Diag_Cd_Stg');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.icd_oncology_diagnosis_code_xwalk WHERE upper(icd_oncology_diagnosis_code_xwalk.icd_oncology_code) NOT IN(
    SELECT
        upper(ref_icd_oncology_diag_cd_stg.icd_oncology_code) AS icd_oncology_code
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.ref_icd_oncology_diag_cd_stg
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
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.icd_oncology_diagnosis_code_xwalk AS mt USING (
    SELECT DISTINCT
        stg.icd_oncology_code AS icd_oncology_code,
        stg.icd_oncology_type_code AS icd_oncology_type_code,
        stg.icd_oncology_category_type_cd AS icd_oncology_category_type_code,
        stg.diagnosis_code AS diagnosis_code,
        stg.diagnosis_type_code AS diagnosis_type_code,
        stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.ref_icd_oncology_diag_cd_stg AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.icd_oncology_diagnosis_code_xwalk AS rio ON upper(rtrim(stg.icd_oncology_code)) = upper(rtrim(rio.icd_oncology_code))
      WHERE rio.icd_oncology_code IS NULL
  ) AS ms
  ON mt.icd_oncology_code = ms.icd_oncology_code
   AND mt.icd_oncology_type_code = ms.icd_oncology_type_code
   AND mt.icd_oncology_category_type_code = ms.icd_oncology_category_type_code
   AND mt.diagnosis_code = ms.diagnosis_code
   AND mt.diagnosis_type_code = ms.diagnosis_type_code
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (icd_oncology_code, icd_oncology_type_code, icd_oncology_category_type_code, diagnosis_code, diagnosis_type_code, source_system_code, dw_last_update_date_time)
      VALUES (ms.icd_oncology_code, ms.icd_oncology_type_code, ms.icd_oncology_category_type_code, ms.diagnosis_code, ms.diagnosis_type_code, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','ICD_Oncology_Diagnosis_Code_Xwalk');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
