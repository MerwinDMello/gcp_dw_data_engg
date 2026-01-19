-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_patient_heme_disease_assess_detail.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Heme_Disease_Assess_Detail	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_Patient_Heme_Disease_Assess_Dtl;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','PATIENT_HEME_DISEASE_ASSESSMENT_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_disease_assess_detail WHERE upper(cn_patient_heme_disease_assess_detail.hashbite_ssk) NOT IN(
    SELECT
        upper(patient_heme_disease_assessment_stg.hbsource) AS hbsource
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
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
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_disease_assess_detail AS mt USING (
    SELECT DISTINCT
        CAST(stg.cn_patient_contact_sid as INT64) AS cn_patient_contact_sid,
        CAST(stg.disease_assess_measure_type_id as INT64) AS disease_assess_measure_type_id,
        stg.disease_assess_measure_value_text AS disease_assess_measure_value_text,
        stg.hashbite_ssk AS hashbite_ssk,
        stg.source_system_code AS source_system_code,
        stg.dw_last_update_date_time
      FROM
        (
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '87' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.karyotype, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '88' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cytogenetic, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '89' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.immunophenotype, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '90' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.othermolecularmarker, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '91' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.othermolecularmarkerresult, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '92' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.ihc, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '93' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_wbc, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '94' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_hgb, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '95' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_plt, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '96' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_anc, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '97' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_seg, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '98' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_bands, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '99' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_monocyte, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '100' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cbc_blast, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '101' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cellularity, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '102' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_creatinine, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '103' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_uricacid, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '104' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_ldh, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '105' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_totalprotein, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '106' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_albumiun, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '107' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_calcium, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '108' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_bilirubin, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '109' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_ast, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '110' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_alt, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '111' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_ntprobmpresults, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '112' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.cmp_troponinresults, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '113' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.betamicroglobulin, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '114' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.sflckappa, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '115' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.sflclambda, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '116' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.sflc_ratio, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '117' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.upepspep_mproteinresuls, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '118' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.meratio, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '119' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_cebpa, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '120' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_ckitresult, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '121' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_dnmt3a, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '122' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_flt3method, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '123' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_flt3result, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '124' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_kit, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '125' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_npm1, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '126' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.mp_othermolecularmarkers, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '127' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.csf_csfresults, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '128' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.upepspep_iferesults, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '129' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.bmplasmacell, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '130' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.bmday, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '131' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.fish_molecularmarkers, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '132' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.fish_philadelphiachromosome, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '133' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.fish_othermolecularmarkers, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '134' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.fish_molecularmarkerssign, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '135' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.hepatitis_heparesult, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '136' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.hepatitis_hepbresult, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '137' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.hepatitis_hepcresult, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '138' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.hivtest_hivresults, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '139' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.nodallocation, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '140' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.lymphnode_ihcresults, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '141' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.lymphnode_lymphnoderesults, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '142' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.immunoglobulins_iga, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '143' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.immunoglobulins_igd, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '144' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.immunoglobulins_ige, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '145' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.immunoglobulins_igg, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '146' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.immunoglobulins_igm, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '147' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.other_othersource, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '148' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.other_diseaseassessmenttest, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
          UNION DISTINCT
          SELECT
              patient_heme_disease_assessment_stg.patienthemefactid AS cn_patient_contact_sid,
              NUMERIC '149' AS disease_assess_measure_type_id,
              disease_assess_measure_value_text AS disease_assess_measure_value_text,
              patient_heme_disease_assessment_stg.hbsource AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
              CROSS JOIN UNNEST(ARRAY[
                substr(patient_heme_disease_assessment_stg.other_result, 1, 255)
              ]) AS disease_assess_measure_value_text
            WHERE disease_assess_measure_value_text IS NOT NULL
             AND upper(rtrim(disease_assess_measure_value_text)) <> ''
        ) AS stg
      WHERE upper(stg.hashbite_ssk) NOT IN(
        SELECT
            upper(cn_patient_heme_disease_assess_detail.hashbite_ssk) AS hashbite_ssk
          FROM
            `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_heme_disease_assess_detail
      )
  ) AS ms
  ON mt.cn_patient_heme_sid = ms.cn_patient_contact_sid
   AND mt.disease_assess_measure_type_id = ms.disease_assess_measure_type_id
   AND (upper(coalesce(mt.disease_assess_measure_value_text, '0')) = upper(coalesce(ms.disease_assess_measure_value_text, '0'))
   AND upper(coalesce(mt.disease_assess_measure_value_text, '1')) = upper(coalesce(ms.disease_assess_measure_value_text, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_heme_sid, disease_assess_measure_type_id, disease_assess_measure_value_text, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_contact_sid, ms.disease_assess_measure_type_id, ms.disease_assess_measure_value_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF false THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Heme_Disease_Assess_Detail');
IF false THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
