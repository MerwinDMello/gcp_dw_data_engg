-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_fact_rad_onc_patient_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT dp.fact_patient_sk,
          rpp.diagnosis_code_sk,
          rp.patient_sk,
          dp.diagnosis_status_id,
          dp.cell_category_id,
          dp.cell_grade_id,
          dp.laterality_id,
          dp.stage_id,
          dp.stage_status_id,
          dp.recurrence_id,
          dp.invasion_id,
          dp.confirmed_diagnosis_id,
          dp.diagnosis_type_id,
          rr.site_sk,
          dp.source_fact_patient_diagnosis_id,
          dp.diagnosis_status_date,
          dp.diagnosis_text,
          dp.clinical_text,
          dp.pathology_comment_text,
          dp.node_num,
          dp.positive_node_num,
          dp.log_id,
          dp.run_id,
          'R' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT row_number() OVER (
                                ORDER BY stg_factpatientdiagnosis.dimsiteid,
                                         stg_factpatientdiagnosis.factpatientdiagnosisid) AS fact_patient_sk,
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
                               DATE(CAST(trim(stg_factpatientdiagnosis.diagnosisstatusdate) AS DATETIME)) AS diagnosis_status_date,
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
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_factpatientdiagnosis) AS dp
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON dp.dimsiteid = rr.source_site_id
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_diagnosis_code AS rpp ON rpp.source_diagnosis_code_id = dp.dimdiagnosiscodeid
   AND rpp.site_sk = rr.site_sk
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient AS rp ON rp.source_patient_id = dp.dimpatientid
   AND rp.site_sk = rr.site_sk) AS stg