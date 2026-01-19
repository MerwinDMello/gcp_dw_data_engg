-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_medical_oncology.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', count(*)), ',\t') AS source_string
  FROM
    (
      SELECT
          stg.cn_patient_medical_oncology_id,
          stg.treatment_type_id,
          ref.facility_id,
          stg.core_record_type_id,
          stg.med_spcl_physician_id,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.core_record_date,
          stg.treatment_start_date,
          stg.treatment_end_date,
          stg.estimated_end_date,
          stg.drug_name,
          stg.dose_dense_chemo_ind,
          stg.drug_dose_amt_text,
          stg.drug_dose_measurement_text,
          stg.drug_available_ind,
          stg.drug_qty,
          stg.cycle_num,
          stg.cycle_frequency_text,
          stg.medical_oncology_reason_text,
          stg.terminated_ind,
          stg.treatment_therapy_schedule_cd,
          stg.comment_text,
          stg.hashbite_ssk,
          stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_medical_oncology_stg AS stg
          LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_facility AS ref ON upper(rtrim(stg.medical_oncology_facility_id)) = upper(rtrim(ref.facility_name))
        WHERE upper(stg.hashbite_ssk) NOT IN(
          SELECT
              upper(cn_patient_medical_oncology.hashbite_ssk) AS hashbite_ssk
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_medical_oncology
        )
    ) AS a
;
