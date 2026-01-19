BEGIN
declare  
    current_ts datetime;
set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk2;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk2 (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, resp, ptype, qtype, category, dw_last_update_date_time, flag)
    SELECT
        hca_pat_resp.hca_unique,
        hca_pat_resp.surv_id,
        hca_pat_resp.adj_samp,
        hca_pat_resp.survey_type,
        hca_pat_resp.pg_unit,
        hca_pat_resp.disdate,
        hca_pat_resp.recdate,
        hca_pat_resp.mde,
        hca_pat_resp.question_id,
        hca_pat_resp.coid,
        hca_pat_resp.resp,
        hca_pat_resp.ptype,
        hca_pat_resp.qtype,
        hca_pat_resp.category,
        hca_pat_resp.dw_last_update_date_time,
        'S' AS flag
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk2 (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, resp, ptype, qtype, category, dw_last_update_date_time, flag)
    SELECT
        fixed_resp_pat_dtl_core_rej.hca_unique,
        fixed_resp_pat_dtl_core_rej.surv_id,
        fixed_resp_pat_dtl_core_rej.adj_samp,
        fixed_resp_pat_dtl_core_rej.survey_type,
        fixed_resp_pat_dtl_core_rej.pg_unit,
        fixed_resp_pat_dtl_core_rej.disdate,
        fixed_resp_pat_dtl_core_rej.recdate,
        fixed_resp_pat_dtl_core_rej.mde,
        fixed_resp_pat_dtl_core_rej.question_id,
        fixed_resp_pat_dtl_core_rej.coid,
        fixed_resp_pat_dtl_core_rej.resp,
        fixed_resp_pat_dtl_core_rej.ptype,
        fixed_resp_pat_dtl_core_rej.qtype,
        fixed_resp_pat_dtl_core_rej.category,
        fixed_resp_pat_dtl_core_rej.dw_last_update_date_time,
        'R' AS flag
      FROM
        {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_rej
      WHERE fixed_resp_pat_dtl_core_rej.coid IS NOT NULL
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk1;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk1 (respondent_id, survey_receive_date, respondent_type_code, survey_sid, company_code, coid, pat_acct_num,patient_dw_id, last_name, middle_initial_text, first_name, address_1_text, address_2_text, city_name, state_code, zip_code, phone_num, drg_code, gender_code, race_code, birth_date, native_language_code, medical_record_num, location_code, attending_physician_id, admission_source_code, admission_date, admission_time, discharge_date, discharge_time, discharge_status_code, unit_code, service_code, payor_code, length_of_stay_days_num, room_id_text, bed_id_text, hospitalist_ind, fast_track_code, email_text, hospitalist_1_id, hospitalist_2_id, er_admission_ind, parent_coid, patient_type_code, medicare_provider_ccn_num, medicare_provider_npi_num, coid_name, nurse_station_code, drg_type_code, service_category_code, mdc_code, diagnosis_category_code, patient_age_num, eligible_code, exclusion_flag, financial_plan_text, admitting_physician_num, primary_icd_9_code, icd_code_version_num, cpt_code, admission_type_code, diagnosis_1_code, diagnosis_2_code, diagnosis_3_code, diagnosis_4_code, diagnosis_5_code, diagnosis_6_code, diagnosis_7_code, diagnosis_8_code, diagnosis_9_code, diagnosis_10_code, diagnosis_11_code, diagnosis_12_code, diagnosis_13_code, diagnosis_14_code, diagnosis_15_code, bill_generated_date, surgery_procedure_1_code, surgery_procedure_2_code, surgery_procedure_3_code, surgery_procedure_4_code, surgeon_1_name, surgeon_2_name, surgeon_3_name, attending_physician_name, hospitalist_name, consulting_physician_id, consulting_physician_name, ip_ed_flag, cpt_ind, exclusion_reason_code, cms_exclusion_ind, ethinicity_code, hipps_code, arrival_mode_text, day_of_week_name, cpt_2_code, cpt_3_code, cpt_4_code, cpt_5_code, cpt_6_code, deceased_ind, publicity_ind, primary_cpt_service_date, final_record_ind, source_system_code, dw_last_update_date_time, flag)
    SELECT
        stg.respondent_id,
        stg.survey_receive_date,
        stg.respondent_type_code,
        stg.survey_sid,
        stg.company_code,
        stg.coid,
        stg.pat_acct_num,
        stg.patient_dw_id,
        stg.last_name,
        stg.middle_initial_text,
        stg.first_name,
        stg.address_1_text,
        stg.address_2_text,
        stg.city_name,
        stg.state_code,
        stg.zip_code,
        stg.phone_num,
        stg.drg_code,
        stg.gender_code,
        stg.race_code,
        stg.birth_date,
        stg.native_language_code,
        stg.medical_record_num,
        stg.location_code,
        stg.attending_physician_id,
        stg.admission_source_code,
        stg.admission_date,
        stg.admission_time,
        stg.discharge_date,
        stg.discharge_time,
        stg.discharge_status_code,
        stg.unit_code,
        stg.service_code,
        stg.payor_code,
        stg.length_of_stay_days_num,
        stg.room_id_text,
        stg.bed_id_text,
        stg.hospitalist_ind,
        stg.fast_track_code,
        stg.email_text,
        stg.hospitalist_1_id,
        stg.hospitalist_2_id,
        stg.er_admission_ind,
        stg.parent_coid,
        stg.patient_type_code,
        stg.medicare_provider_ccn_num,
        stg.medicare_provider_npi_num,
        stg.coid_name,
        stg.nurse_station_code,
        stg.drg_type_code,
        stg.service_category_code,
        stg.mdc_code,
        stg.diagnosis_category_code,
        stg.patient_age_num,
        stg.eligible_code,
        stg.exclusion_flag,
        stg.financial_plan_text,
        stg.admitting_physician_num,
        stg.primary_icd_9_code,
        stg.icd_code_version_num,
        stg.cpt_code,
        stg.admission_type_code,
        stg.diagnosis_1_code,
        stg.diagnosis_2_code,
        stg.diagnosis_3_code,
        stg.diagnosis_4_code,
        stg.diagnosis_5_code,
        stg.diagnosis_6_code,
        stg.diagnosis_7_code,
        stg.diagnosis_8_code,
        stg.diagnosis_9_code,
        stg.diagnosis_10_code,
        stg.diagnosis_11_code,
        stg.diagnosis_12_code,
        stg.diagnosis_13_code,
        stg.diagnosis_14_code,
        stg.diagnosis_15_code,
        stg.bill_generated_date,
        stg.surgery_procedure_1_code,
        stg.surgery_procedure_2_code,
        stg.surgery_procedure_3_code,
        cast(stg.surgery_procedure_4_code as string),
        stg.surgeon_1_name,
        stg.surgeon_2_name,
        stg.surgeon_3_name,
        stg.attending_physician_name,
        stg.hospitalist_name,
        stg.consulting_physician_id,
        stg.consulting_physician_name,
        stg.ip_ed_flag,
        stg.cpt_ind,
        stg.exclusion_reason_code,
        stg.cms_exclusion_ind,
        stg.ethinicity_code,
        stg.hipps_code,
        stg.arrival_mode_text,
        stg.day_of_week_name,
        stg.cpt_2_code,
        stg.cpt_3_code,
        stg.cpt_4_code,
        stg.cpt_5_code,
        stg.cpt_6_code,
        stg.deceased_ind,
        stg.publicity_ind,
        stg.primary_cpt_service_date,
        stg.final_record_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time,
        stg.flag
      FROM
        (
          SELECT
              prd.survey_id AS respondent_id,
              prd.rec_date AS survey_receive_date,
              'P' AS respondent_type_code,
              sq.survey_sid AS survey_sid,
              ff.company_code,
              g.coid,
              CASE
          WHEN substr(prd.survey_type, 2, 1) = 'Y' THEN CAST(NULL as NUMERIC)
          ELSE (CASE
             substr(prd.hca_unique, 6, 15)
            WHEN '' THEN NUMERIC '0'
            ELSE CAST(substr(regexp_replace(prd.hca_unique,'-',''), 6, 15) as NUMERIC)
          END)
        END AS pat_acct_num,
              ca.patient_dw_id,
              prd.last_name AS last_name,
              prd.middle_name AS middle_initial_text,
              prd.first_name AS first_name,
              prd.address_line1 AS address_1_text,
              prd.address_line2 AS address_2_text,
              prd.city AS city_name,
              prd.state AS state_code,
              prd.zipcode AS zip_code,
              prd.phone AS phone_num,
              prd.drg AS drg_code,
              prd.gender AS gender_code,
              prd.race AS race_code,
              prd.dob AS birth_date,
              prd.lang AS native_language_code,
              prd.mrn AS medical_record_num,
              prd.loc_code AS location_code,
              prd.attending_phys_id AS attending_physician_id,
              prd.admission_source AS admission_source_code,
              prd.admission_date AS admission_date,
              CASE
                WHEN upper(trim(coalesce(prd.admission_time, ''))) = '' THEN CAST(NULL as TIME)
                ELSE parse_time('%H%M', lpad(trim(prd.admission_time), 4, '0'))
              END AS admission_time,
              prd.discharge_date AS discharge_date,
              CASE
                WHEN upper(trim(coalesce(prd.discharge_time, ''))) = '' THEN CAST(NULL as TIME)
                ELSE parse_time('%H%M', lpad(trim(prd.discharge_time), 4, '0'))
              END AS discharge_time,
              prd.discharge_status AS discharge_status_code,
              prd.unit AS unit_code,
              prd.service AS service_code,
              prd.payor AS payor_code,
              prd.length_of_stay AS length_of_stay_days_num,
              prd.room AS room_id_text,
              prd.bed AS bed_id_text,
              prd.is_hospitalist AS hospitalist_ind,
              prd.fasttrack AS fast_track_code,
              prd.email AS email_text,
              prd.hospitalist_id_1 AS hospitalist_1_id,
              prd.hospitalist_id_2 AS hospitalist_2_id,
              prd.is_er_admission AS er_admission_ind,
              g.parent_coid AS parent_coid,
              prd.patient_type AS patient_type_code,
              prd.medicare_provider_num AS medicare_provider_ccn_num,
              prd.medicare_provider_npi AS medicare_provider_npi_num,
              prd.facility_name AS coid_name,
              prd.nurse_station AS nurse_station_code,
              prd.drg_type AS drg_type_code,
              prd.service_category AS service_category_code,
              prd.mdc AS mdc_code,
              prd.diag_category AS diagnosis_category_code,
              prd.patient_age AS patient_age_num,
              prd.eligible AS eligible_code,
              prd.exclusion_flag AS exclusion_flag,
              prd.financial_plan AS financial_plan_text,
              prd.admission_phys_code AS admitting_physician_num,
              prd.primary_icd9_code AS primary_icd_9_code,
              prd.icd_code_version AS icd_code_version_num,
              prd.cpt_code AS cpt_code,
              prd.admission_type AS admission_type_code,
              prd.diag_1 AS diagnosis_1_code,
              prd.diag_2 AS diagnosis_2_code,
              prd.diag_3 AS diagnosis_3_code,
              prd.diag_4 AS diagnosis_4_code,
              prd.diag_5 AS diagnosis_5_code,
              prd.diag_6 AS diagnosis_6_code,
              prd.diag_7 AS diagnosis_7_code,
              prd.diag_8 AS diagnosis_8_code,
              prd.diag_9 AS diagnosis_9_code,
              prd.diag_10 AS diagnosis_10_code,
              prd.diag_11 AS diagnosis_11_code,
              prd.diag_12 AS diagnosis_12_code,
              prd.diag_13 AS diagnosis_13_code,
              prd.diag_14 AS diagnosis_14_code,
              prd.diag_15 AS diagnosis_15_code,
              prd.bill_generated_date AS bill_generated_date,
              prd.surg_proc_code_1 AS surgery_procedure_1_code,
              prd.surg_proc_code_2 AS surgery_procedure_2_code,
              prd.surg_proc_code_3 AS surgery_procedure_3_code,
              NULL AS surgery_procedure_4_code,
              prd.surgeon_name_1 AS surgeon_1_name,
              prd.surgeon_name_2 AS surgeon_2_name,
              prd.surgeon_name_3 AS surgeon_3_name,
              prd.attending_phys_name AS attending_physician_name,
              prd.hospitalist_flag AS hospitalist_name,
              prd.consulting_phys_id AS consulting_physician_id,
              prd.consulting_phys_name AS consulting_physician_name,
              prd.iped AS ip_ed_flag,
              prd.cpt_flag AS cpt_ind,
              CASE
                WHEN upper(rs.survey_category_text) = 'CAHPS'
                 AND CASE
                  WHEN extract(MONTH from CAST(prd.admission_date as TIMESTAMP)) > extract(MONTH from CAST(prd.dob as TIMESTAMP)) THEN extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP))
                  ELSE CASE
                    WHEN extract(MONTH from CAST(prd.admission_date as TIMESTAMP)) = extract(MONTH from CAST(prd.dob as TIMESTAMP)) THEN CASE
                      WHEN extract(DAY from CAST(prd.admission_date as TIMESTAMP)) >= extract(DAY from CAST(prd.dob as TIMESTAMP)) THEN extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP))
                      ELSE extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP)) - 1
                    END
                    ELSE extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP)) - 1
                  END
                END < 18 THEN 'Patient Under 18'
                WHEN upper(rs.survey_category_text) = 'CAHPS'
                 AND prd.discharge_date = prd.admission_date THEN 'Admission = Discharge'
                WHEN upper(rs.survey_category_text) = 'CAHPS'
                 AND (cast(prd.drg as int64) BETWEEN 283 AND 285
                 OR cast(prd.drg as int64) BETWEEN 789 AND 795) THEN concat(prd.discharge_status, '_DischargeNurse')
                WHEN upper(rs.survey_category_text) = 'CAHPS'
                 AND prd.discharge_status IN(
                  '21', '87'
                ) THEN concat(prd.discharge_status, '_DischargeLaw')
                WHEN upper(rs.survey_category_text) = 'CAHPS'
                 AND prd.discharge_status IN(
                  '50', '51'
                ) THEN concat(prd.discharge_status, '_DischargeHospice')
                WHEN upper(rs.survey_category_text) = 'CAHPS'
                 AND prd.discharge_status IN(
                  '20'
                ) THEN concat(prd.discharge_status, '_DischargeDeath')
                ELSE CAST(NULL as STRING)
              END AS exclusion_reason_code,
              CASE
                WHEN CASE
                  WHEN upper(rs.survey_category_text) = 'CAHPS'
                   AND CASE
                    WHEN extract(MONTH from CAST(prd.admission_date as TIMESTAMP)) > extract(MONTH from CAST(prd.dob as TIMESTAMP)) THEN extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP))
                    ELSE CASE
                      WHEN extract(MONTH from CAST(prd.admission_date as TIMESTAMP)) = extract(MONTH from CAST(prd.dob as TIMESTAMP)) THEN CASE
                        WHEN extract(DAY from CAST(prd.admission_date as TIMESTAMP)) >= extract(DAY from CAST(prd.dob as TIMESTAMP)) THEN extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP))
                        ELSE extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP)) - 1
                      END
                      ELSE extract(YEAR from CAST(prd.admission_date as TIMESTAMP)) - extract(YEAR from CAST(prd.dob as TIMESTAMP)) - 1
                    END
                  END < 18 THEN 'Patient Under 18'
                  WHEN upper(rs.survey_category_text) = 'CAHPS'
                   AND prd.discharge_date = prd.admission_date THEN 'Admission = Discharge'
                  WHEN upper(rs.survey_category_text) = 'CAHPS'
                   AND (cast(prd.drg as int64) BETWEEN 283 AND 285
                   OR cast(prd.drg as int64) BETWEEN 789 AND 795) THEN concat(prd.discharge_status, '_DischargeNurse')
                  WHEN upper(rs.survey_category_text) = 'CAHPS'
                   AND prd.discharge_status IN(
                    '21', '87'
                  ) THEN concat(prd.discharge_status, '_DischargeLaw')
                  WHEN upper(rs.survey_category_text) = 'CAHPS'
                   AND prd.discharge_status IN(
                    '50', '51'
                  ) THEN concat(prd.discharge_status, '_DischargeHospice')
                  WHEN upper(rs.survey_category_text) = 'CAHPS'
                   AND prd.discharge_status IN(
                    '20'
                  ) THEN concat(prd.discharge_status, '_DischargeDeath')
                  ELSE CAST(NULL as STRING)
                END IS NOT NULL THEN 'Y'
                ELSE 'N'
              END AS cms_exclusion_ind,
              prd.ethnicity AS ethinicity_code,
              prd.hipps_code AS hipps_code,
              prd.mode_of_arrivl AS arrival_mode_text,
              prd.day_of_week AS day_of_week_name,
              prd.cpt_code_2 AS cpt_2_code,
              prd.cpt_code_3 AS cpt_3_code,
              prd.cpt_code_4 AS cpt_4_code,
              prd.cpt_code_5 AS cpt_5_code,
              prd.cpt_code_6 AS cpt_6_code,
              prd.deceased_flag AS deceased_ind,
              prd.no_publicity AS publicity_ind,
              prd.prim_cpt_svc_date AS primary_cpt_service_date,
              'N' AS final_record_ind,
              'H' AS source_system_code,
              current_ts AS dw_last_update_date_time,
              presp.flag
            FROM
              {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk2 AS presp
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_detail AS prd ON upper(coalesce(presp.hca_unique, 'H')) = upper(coalesce(prd.hca_unique, 'H'))
               AND presp.surv_id = prd.survey_id
              LEFT OUTER JOIN 
              {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON presp.question_id = sq.question_id
               AND upper(sq.source_system_code) = 'H'
              INNER JOIN 
              {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON sq.survey_sid = rs.survey_sid
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS g ON prd.coid = g.coid
              LEFT OUTER JOIN 
              {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = prd.coid
              LEFT JOIN
              {{ params.param_auth_base_views_dataset_name }}.hl7_clinical_keys ca
              --ON(CASE WHEN SUBSTR(PRESP.SURVEY_TYPE,2,1)='Y' THEN SUBSTR(PRESP.HCA_UNIQUE,3,5) ELSE SUBSTR(PRESP.HCA_UNIQUE,1,5) END=CA.COID AND
              on(g.parent_coid = ca.coid and
                  CASE
                  substr(presp.hca_unique, 6, 15)
                  WHEN '' THEN  NUMERIC '0'
                  ELSE CAST(substr(regexp_replace(presp.hca_unique,'-',''), 6, 15) as NUMERIC)
                  END = ca.patient_account_num)
              
            WHERE upper(survey_group_text) = 'PATIENT_SATISFACTION'
             AND rs.eff_to_date = '9999-12-31'
             AND sq.eff_to_date = '9999-12-31'
        ) AS stg
      WHERE stg.respondent_id IS NOT NULL
       AND stg.coid IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.respondent_id, stg.survey_receive_date, upper(stg.respondent_type_code), stg.survey_sid, stg.company_code, stg.coid, stg.pat_acct_num, stg.last_name, stg.middle_initial_text, stg.first_name, stg.address_1_text, stg.address_2_text, stg.city_name, stg.state_code, stg.zip_code, stg.phone_num, stg.drg_code, stg.gender_code, stg.race_code, stg.birth_date, stg.native_language_code, stg.medical_record_num, stg.location_code, stg.attending_physician_id, stg.admission_source_code, stg.admission_date, stg.admission_time, stg.discharge_date, stg.discharge_time, stg.discharge_status_code, stg.unit_code, stg.service_code, stg.payor_code, stg.length_of_stay_days_num, stg.room_id_text, stg.bed_id_text, stg.hospitalist_ind, stg.fast_track_code, stg.email_text, stg.hospitalist_1_id, stg.hospitalist_2_id, stg.er_admission_ind, stg.parent_coid, stg.patient_type_code, stg.medicare_provider_ccn_num, stg.medicare_provider_npi_num, stg.coid_name, stg.nurse_station_code, stg.drg_type_code, stg.service_category_code, stg.mdc_code, stg.diagnosis_category_code, stg.patient_age_num, stg.eligible_code, stg.exclusion_flag, stg.financial_plan_text, stg.admitting_physician_num, stg.primary_icd_9_code, stg.icd_code_version_num, stg.cpt_code, stg.admission_type_code, stg.diagnosis_1_code, stg.diagnosis_2_code, stg.diagnosis_3_code, stg.diagnosis_4_code, stg.diagnosis_5_code, stg.diagnosis_6_code, stg.diagnosis_7_code, stg.diagnosis_8_code, stg.diagnosis_9_code, stg.diagnosis_10_code, stg.diagnosis_11_code, stg.diagnosis_12_code, stg.diagnosis_13_code, stg.diagnosis_14_code, stg.diagnosis_15_code, stg.bill_generated_date, stg.surgery_procedure_1_code, stg.surgery_procedure_2_code, stg.surgery_procedure_3_code, stg.surgery_procedure_4_code, stg.surgeon_1_name, stg.surgeon_2_name, stg.surgeon_3_name, stg.attending_physician_name, stg.hospitalist_name, stg.consulting_physician_id, stg.consulting_physician_name, stg.ip_ed_flag, stg.cpt_ind, upper(stg.exclusion_reason_code), upper(stg.cms_exclusion_ind), stg.ethinicity_code, stg.hipps_code, stg.arrival_mode_text, stg.day_of_week_name, stg.cpt_2_code, stg.cpt_3_code, stg.cpt_4_code, stg.cpt_5_code, stg.cpt_6_code, stg.deceased_ind, stg.publicity_ind, stg.primary_cpt_service_date, upper(stg.final_record_ind), upper(stg.source_system_code), stg.dw_last_update_date_time ORDER BY stg.flag) = 1
  ;

END;