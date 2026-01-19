-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_medical_oncology.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_medical_oncology AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    coalesce(cpmo.treatment_end_date, cnmo.treatment_end_date) AS treatment_end_date,
    cnmo.comment_text,
    rcrt.core_record_type_desc,
    cnmo.core_record_date,
    coalesce(cpmo.cycle_num_text, CAST(cnmo.cycle_num AS String)) AS cycle_num_text,
    coalesce(cpmo.total_drug_dose_amt, CAST(cnmo.drug_dose_amt_text AS Numeric)) AS drug_dose_amt_text,
    CASE
      WHEN cpmo.total_drug_dose_amt IS NOT NULL
       AND cpmo.total_drug_dose_amt > 0 THEN 'Y'
      ELSE cnmo.drug_available_ind
    END AS drug_available_ind,
    coalesce(nsc.nsc_desc, cnmo.drug_name) AS drug_name,
    cnmo.estimated_end_date,
    coalesce(rh.hospital_name, rf.facility_name) AS facility_name,
    coalesce(t48.lookup_desc, cnmo.drug_dose_measurement_text) AS drug_dose_measurement_text,
    cnmo.treatment_therapy_schedule_code,
    coalesce(cr.treatment_performing_physician_name, cpd.physician_name) AS treatment_performing_physician_name,
    cnmo.drug_qty,
    coalesce(cpmo.treatment_start_date, cnmo.treatment_start_date) AS treatment_start_date,
    cnmo.terminated_ind,
    cnmo.medical_oncology_reason_text,
    coalesce(cpmo.drug_frequency_num, CAST(cnmo.cycle_frequency_text AS NUMERIC)) AS drug_frequency_text,
    cnmo.dose_dense_chemo_ind,
    coalesce(rtt.treatment_type_desc, rtt1.treatment_type_desc) AS treatment_type_desc,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_tumor AS cpt ON cptd.cr_patient_id = cpt.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_treatment AS cptt ON cpt.tumor_id = cptt.tumor_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_cr_treatment_type AS rtt ON cptt.treatment_type_id = rtt.treatment_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_medical_oncology AS cpmo ON cptt.treatment_id = cpmo.treatment_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_medical_oncology AS cnmo ON cptd.cn_patient_id = cnmo.nav_patient_id
     AND cptd.cn_tumor_type_id = cnmo.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_national_service_center AS nsc ON cpmo.nsc_id = nsc.nsc_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_core_record_type AS rcrt ON cnmo.core_record_type_id = rcrt.core_record_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS t48 ON cpmo.drug_dose_unit_id = t48.master_lookup_sid
     AND t48.lookup_id = 48
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_treatment_type AS rtt1 ON cnmo.treatment_type_id = rtt1.treatment_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_hospital AS rh ON cpmo.drug_hospital_id = rh.hospital_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_facility AS rf ON cnmo.medical_oncology_facility_id = rf.facility_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS cpd ON cnmo.med_spcl_physician_id = cpd.physician_id
    LEFT OUTER JOIN (
      SELECT DISTINCT
          cptt1.treatment_id,
          concat(cpc.contact_last_name, ', ', cpc.contact_first_name) AS treatment_performing_physician_name
        FROM
          {{ params.param_cr_core_dataset_name }}.cr_patient_treatment AS cptt1
          LEFT OUTER JOIN (
            SELECT DISTINCT
                cpc1.contact_num_code,
                cpc1.contact_last_name,
                cpc1.contact_first_name
              FROM
                {{ params.param_cr_core_dataset_name }}.cr_patient_contact AS cpc1
                INNER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS rlc ON cpc1.contact_type_id = rlc.master_lookup_sid
                 AND rlc.lookup_id = 10
                 AND upper(rlc.lookup_code) = 'D'
          ) AS cpc ON cptt1.treatment_performing_physician_code = cpc.contact_num_code
    ) AS cr ON cptt.treatment_id = cr.treatment_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cpmo.treatment_id IS NOT NULL
   OR cnmo.nav_patient_id IS NOT NULL
;