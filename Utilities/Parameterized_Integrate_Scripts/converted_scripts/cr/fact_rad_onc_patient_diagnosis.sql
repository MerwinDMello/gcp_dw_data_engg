-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/fact_rad_onc_patient_diagnosis.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Fact_Rad_Onc_Patient_Diagnosis               ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.FactPatientDiagnosis 				##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_RO_Fact_Rad_Onc_Patient_Diagnosis;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_FactPatientDiagnosis');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Data
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient_diagnosis;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient_diagnosis AS mt USING (
    SELECT DISTINCT
        dp.fact_patient_sk,
        rpp.diagnosis_code_sk,
        rp.patient_sk,
        dp.diagnosis_status_id AS diagnosis_status_id,
        dp.cell_category_id AS cell_category_id,
        dp.cell_grade_id AS cell_grade_id,
        dp.laterality_id AS laterality_id,
        dp.stage_id AS stage_id,
        dp.stage_status_id AS stage_status_id,
        dp.recurrence_id AS recurrence_id,
        dp.invasion_id AS invasion_id,
        dp.confirmed_diagnosis_id AS confirmed_diagnosis_id,
        dp.diagnosis_type_id AS diagnosis_type_id,
        rr.site_sk,
        dp.source_fact_patient_diagnosis_id AS source_fact_patient_diagnosis_id,
        dp.diagnosis_status_date,
        substr(dp.diagnosis_text, 1, 255) AS diagnosis_text,
        substr(dp.clinical_text, 1, 255) AS clinical_text,
        substr(dp.pathology_comment_text, 1, 255) AS pathology_comment_text,
        dp.node_num AS node_num,
        dp.positive_node_num AS positive_node_num,
        dp.log_id,
        dp.run_id,
        'R' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              row_number() OVER (ORDER BY stg_factpatientdiagnosis.dimsiteid, stg_factpatientdiagnosis.factpatientdiagnosisid) AS fact_patient_sk,
              stg_factpatientdiagnosis.dimlookupid_diagnosisstatus AS diagnosis_status_id,
              stg_factpatientdiagnosis.dimlookupid_cellcategory AS cell_category_id,
              stg_factpatientdiagnosis.dimlookupid_cellgrade AS cell_grade_id,
              stg_factpatientdiagnosis.dimlookupid_laterality AS laterality_id,
              stg_factpatientdiagnosis.dimlookupid_stage AS stage_id,
              stg_factpatientdiagnosis.dimlookupid_stagestatus AS stage_status_id,
              stg_factpatientdiagnosis.dimlookupid_recurrence AS recurrence_id,
              stg_factpatientdiagnosis.dimlookupid_invasive AS invasion_id,
              stg_factpatientdiagnosis.dimlookupid_confirmeddx AS confirmed_diagnosis_id,
              stg_factpatientdiagnosis.dimlookupid_diagnosistype AS diagnosis_type_id,
              stg_factpatientdiagnosis.factpatientdiagnosisid AS source_fact_patient_diagnosis_id,
              DATE(CAST(trim(stg_factpatientdiagnosis.diagnosisstatusdate) as DATETIME)) AS diagnosis_status_date,
              stg_factpatientdiagnosis.diagnosisdescription AS diagnosis_text,
              stg_factpatientdiagnosis.clinicaldescription AS clinical_text,
              stg_factpatientdiagnosis.pathologycomments AS pathology_comment_text,
              stg_factpatientdiagnosis.nodes AS node_num,
              stg_factpatientdiagnosis.nodespositive AS positive_node_num,
              stg_factpatientdiagnosis.logid AS log_id,
              stg_factpatientdiagnosis.runid AS run_id,
              stg_factpatientdiagnosis.dimdiagnosiscodeid,
              stg_factpatientdiagnosis.dimpatientid,
              stg_factpatientdiagnosis.dimsiteid
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_factpatientdiagnosis
        ) AS dp
        INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON dp.dimsiteid = rr.source_site_id
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_diagnosis_code AS rpp ON rpp.source_diagnosis_code_id = dp.dimdiagnosiscodeid
         AND rpp.site_sk = rr.site_sk
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient AS rp ON rp.source_patient_id = dp.dimpatientid
         AND rp.site_sk = rr.site_sk
  ) AS ms
  ON mt.fact_patient_diagnosis_sk = ms.fact_patient_sk
   AND (coalesce(mt.diagnosis_code_sk, 0) = coalesce(ms.diagnosis_code_sk, 0)
   AND coalesce(mt.diagnosis_code_sk, 1) = coalesce(ms.diagnosis_code_sk, 1))
   AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
   AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
   AND (coalesce(mt.diagnosis_status_id, 0) = coalesce(ms.diagnosis_status_id, 0)
   AND coalesce(mt.diagnosis_status_id, 1) = coalesce(ms.diagnosis_status_id, 1))
   AND (coalesce(mt.cell_category_id, 0) = coalesce(ms.cell_category_id, 0)
   AND coalesce(mt.cell_category_id, 1) = coalesce(ms.cell_category_id, 1))
   AND (coalesce(mt.cell_grade_id, 0) = coalesce(ms.cell_grade_id, 0)
   AND coalesce(mt.cell_grade_id, 1) = coalesce(ms.cell_grade_id, 1))
   AND (coalesce(mt.laterality_id, 0) = coalesce(ms.laterality_id, 0)
   AND coalesce(mt.laterality_id, 1) = coalesce(ms.laterality_id, 1))
   AND (coalesce(mt.stage_id, 0) = coalesce(ms.stage_id, 0)
   AND coalesce(mt.stage_id, 1) = coalesce(ms.stage_id, 1))
   AND (coalesce(mt.stage_status_id, 0) = coalesce(ms.stage_status_id, 0)
   AND coalesce(mt.stage_status_id, 1) = coalesce(ms.stage_status_id, 1))
   AND (coalesce(mt.recurrence_id, 0) = coalesce(ms.recurrence_id, 0)
   AND coalesce(mt.recurrence_id, 1) = coalesce(ms.recurrence_id, 1))
   AND (coalesce(mt.invasion_id, 0) = coalesce(ms.invasion_id, 0)
   AND coalesce(mt.invasion_id, 1) = coalesce(ms.invasion_id, 1))
   AND (coalesce(mt.confirmed_diagnosis_id, 0) = coalesce(ms.confirmed_diagnosis_id, 0)
   AND coalesce(mt.confirmed_diagnosis_id, 1) = coalesce(ms.confirmed_diagnosis_id, 1))
   AND (coalesce(mt.diagnosis_type_id, 0) = coalesce(ms.diagnosis_type_id, 0)
   AND coalesce(mt.diagnosis_type_id, 1) = coalesce(ms.diagnosis_type_id, 1))
   AND mt.site_sk = ms.site_sk
   AND mt.source_fact_patient_diagnosis_id = ms.source_fact_patient_diagnosis_id
   AND (coalesce(mt.diagnosis_status_date, DATE '1970-01-01') = coalesce(ms.diagnosis_status_date, DATE '1970-01-01')
   AND coalesce(mt.diagnosis_status_date, DATE '1970-01-02') = coalesce(ms.diagnosis_status_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.diagnosis_text, '0')) = upper(coalesce(ms.diagnosis_text, '0'))
   AND upper(coalesce(mt.diagnosis_text, '1')) = upper(coalesce(ms.diagnosis_text, '1')))
   AND (upper(coalesce(mt.clinical_text, '0')) = upper(coalesce(ms.clinical_text, '0'))
   AND upper(coalesce(mt.clinical_text, '1')) = upper(coalesce(ms.clinical_text, '1')))
   AND (upper(coalesce(mt.pathology_comment_text, '0')) = upper(coalesce(ms.pathology_comment_text, '0'))
   AND upper(coalesce(mt.pathology_comment_text, '1')) = upper(coalesce(ms.pathology_comment_text, '1')))
   AND (coalesce(mt.node_num, 0) = coalesce(ms.node_num, 0)
   AND coalesce(mt.node_num, 1) = coalesce(ms.node_num, 1))
   AND (coalesce(mt.positive_node_num, 0) = coalesce(ms.positive_node_num, 0)
   AND coalesce(mt.positive_node_num, 1) = coalesce(ms.positive_node_num, 1))
   AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
   AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
   AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
   AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (fact_patient_diagnosis_sk, diagnosis_code_sk, patient_sk, diagnosis_status_id, cell_category_id, cell_grade_id, laterality_id, stage_id, stage_status_id, recurrence_id, invasion_id, confirmed_diagnosis_id, diagnosis_type_id, site_sk, source_fact_patient_diagnosis_id, diagnosis_status_date, diagnosis_text, clinical_text, pathology_comment_text, node_num, positive_node_num, log_id, run_id, source_system_code, dw_last_update_date_time)
      VALUES (ms.fact_patient_sk, ms.diagnosis_code_sk, ms.patient_sk, ms.diagnosis_status_id, ms.cell_category_id, ms.cell_grade_id, ms.laterality_id, ms.stage_id, ms.stage_status_id, ms.recurrence_id, ms.invasion_id, ms.confirmed_diagnosis_id, ms.diagnosis_type_id, ms.site_sk, ms.source_fact_patient_diagnosis_id, ms.diagnosis_status_date, ms.diagnosis_text, ms.clinical_text, ms.pathology_comment_text, ms.node_num, ms.positive_node_num, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Fact_Rad_Onc_Patient_Diagnosis');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
