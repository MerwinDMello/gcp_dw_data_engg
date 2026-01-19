-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_diagnosis_code.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Rad_Onc_Diagnosis_Code                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.DimDiagnosisCode_STG 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_RO_Ref_Rad_Onc_Diagnosis_Code;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','DimDiagnosisCode_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Data
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_diagnosis_code;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_diagnosis_code AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY dp.dimsiteid, dp.dimdiagnosiscodeid DESC) AS diagnosis_code_sk,
        rr.site_sk AS site_sk,
        dp.dimdiagnosiscodeid AS source_diagnosis_code_id,
        CASE
          WHEN upper(rtrim(dp.diagnosiscode)) = '' THEN CAST(NULL as STRING)
          ELSE dp.diagnosiscode
        END AS diagnosis_code,
        substr(sc.diagnosissites, 1, 50) AS diagnosis_site_text,
        dp.diagnosiscodeclsschemeid AS diagnosis_code_class_schema_id,
        substr(CASE
          WHEN upper(rtrim(dp.diagnosisclinicaldescriptionen)) = '' THEN CAST(NULL as STRING)
          ELSE dp.diagnosisclinicaldescriptionen
        END, 1, 255) AS diagnosis_clinical_desc,
        substr(CASE
          WHEN upper(rtrim(dp.diagnosisfulltitleenu)) = '' THEN CAST(NULL as STRING)
          ELSE dp.diagnosisfulltitleenu
        END, 1, 255) AS diagnosis_long_desc,
        CASE
          WHEN upper(rtrim(dp.diagnosistableenu)) = '' THEN CAST(NULL as STRING)
          ELSE dp.diagnosistableenu
        END AS diagnosis_type_code,
        dp.logid AS log_id,
        dp.runid AS run_id,
        'R' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              dimdiagnosiscode_stg.dimdiagnosiscodeid,
              dimdiagnosiscode_stg.dimsiteid,
              dimdiagnosiscode_stg.diagnosiscodeclsschemeid,
              trim(dimdiagnosiscode_stg.diagnosiscode) AS diagnosiscode,
              trim(dimdiagnosiscode_stg.diagnosisclinicaldescriptionen) AS diagnosisclinicaldescriptionen,
              trim(dimdiagnosiscode_stg.diagnosisfulltitleenu) AS diagnosisfulltitleenu,
              trim(dimdiagnosiscode_stg.diagnosistableenu) AS diagnosistableenu,
              dimdiagnosiscode_stg.logid,
              dimdiagnosiscode_stg.runid
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.dimdiagnosiscode_stg
        ) AS dp
        INNER JOIN (
          SELECT
              ref_rad_onc_site.source_site_id,
              ref_rad_onc_site.site_sk
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site
        ) AS rr ON rr.source_site_id = dp.dimsiteid
        LEFT OUTER JOIN (
          SELECT DISTINCT
              trim(cr_sc_diagnosissites_stg.diagnosiscode) AS diagnosiscode,
              cr_sc_diagnosissites_stg.diagnosissites
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.cr_sc_diagnosissites_stg
        ) AS sc ON rtrim(sc.diagnosiscode) = rtrim(dp.diagnosiscode)
  ) AS ms
  ON mt.diagnosis_code_sk = ms.diagnosis_code_sk
   AND mt.site_sk = ms.site_sk
   AND mt.source_diagnosis_code_id = ms.source_diagnosis_code_id
   AND (upper(coalesce(mt.diagnosis_code, '0')) = upper(coalesce(ms.diagnosis_code, '0'))
   AND upper(coalesce(mt.diagnosis_code, '1')) = upper(coalesce(ms.diagnosis_code, '1')))
   AND (upper(coalesce(mt.diagnosis_site_text, '0')) = upper(coalesce(ms.diagnosis_site_text, '0'))
   AND upper(coalesce(mt.diagnosis_site_text, '1')) = upper(coalesce(ms.diagnosis_site_text, '1')))
   AND (coalesce(mt.diagnosis_code_class_schema_id, 0) = coalesce(ms.diagnosis_code_class_schema_id, 0)
   AND coalesce(mt.diagnosis_code_class_schema_id, 1) = coalesce(ms.diagnosis_code_class_schema_id, 1))
   AND (upper(coalesce(mt.diagnosis_clinical_desc, '0')) = upper(coalesce(ms.diagnosis_clinical_desc, '0'))
   AND upper(coalesce(mt.diagnosis_clinical_desc, '1')) = upper(coalesce(ms.diagnosis_clinical_desc, '1')))
   AND (upper(coalesce(mt.diagnosis_long_desc, '0')) = upper(coalesce(ms.diagnosis_long_desc, '0'))
   AND upper(coalesce(mt.diagnosis_long_desc, '1')) = upper(coalesce(ms.diagnosis_long_desc, '1')))
   AND (upper(coalesce(mt.diagnosis_type_code, '0')) = upper(coalesce(ms.diagnosis_type_code, '0'))
   AND upper(coalesce(mt.diagnosis_type_code, '1')) = upper(coalesce(ms.diagnosis_type_code, '1')))
   AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
   AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
   AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
   AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (diagnosis_code_sk, site_sk, source_diagnosis_code_id, diagnosis_code, diagnosis_site_text, diagnosis_code_class_schema_id, diagnosis_clinical_desc, diagnosis_long_desc, diagnosis_type_code, log_id, run_id, source_system_code, dw_last_update_date_time)
      VALUES (ms.diagnosis_code_sk, ms.site_sk, ms.source_diagnosis_code_id, ms.diagnosis_code, ms.diagnosis_site_text, ms.diagnosis_code_class_schema_id, ms.diagnosis_clinical_desc, ms.diagnosis_long_desc, ms.diagnosis_type_code, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Rad_Onc_Diagnosis_Code');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
