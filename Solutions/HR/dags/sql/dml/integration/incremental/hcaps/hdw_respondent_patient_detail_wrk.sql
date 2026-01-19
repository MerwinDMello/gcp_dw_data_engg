BEGIN
declare  
    current_ts datetime;
set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk1;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk1 (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, resp, coid, dw_last_update_date_time)
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
        hca_pat_resp.resp,
        hca_pat_resp.coid,
        hca_pat_resp.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk1 (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, resp, coid, dw_last_update_date_time)
    SELECT
        rspndnt_pat_dtl_rej.hca_unique,
        rspndnt_pat_dtl_rej.surv_id,
        rspndnt_pat_dtl_rej.adj_samp,
        rspndnt_pat_dtl_rej.survey_type,
        rspndnt_pat_dtl_rej.pg_unit,
        rspndnt_pat_dtl_rej.disdate,
        rspndnt_pat_dtl_rej.recdate,
        rspndnt_pat_dtl_rej.mde,
        rspndnt_pat_dtl_rej.question_id,
        rspndnt_pat_dtl_rej.resp,
        rspndnt_pat_dtl_rej.coid,
        rspndnt_pat_dtl_rej.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.rspndnt_pat_dtl_rej
      WHERE rspndnt_pat_dtl_rej.coid IS NOT NULL
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk0;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk0 (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, survey_receive_date, mde, question_id, resp, pat_acct_num, survey_type_ind, coid, dw_last_update_date_time)
    SELECT
        hqm.hca_unique,
        hqm.surv_id,
        hqm.adj_samp AS adj_samp,
        hqm.survey_type AS survey_type,
        hqm.pg_unit AS pg_unit,
        hqm.disdate AS disdate,
        hqm.recdate AS survey_receive_date,
        hqm.mde AS mde,
        hqm.question_id,
        hqm.resp AS resp,
        CASE
          WHEN substr(survey_type, 2, 1) = 'Y' THEN CAST(NULL as NUMERIC)
          ELSE (CASE
             substr(hca_unique, 6, 15)
            WHEN '' THEN NUMERIC '0'
            ELSE CAST(substr(regexp_replace(hca_unique,'-',''), 6, 15) as NUMERIC)
          END)
        END AS pat_acct_num,
        substr(hqm.survey_type, 2, 1) AS survey_type_ind,
        hqm.coid AS coid,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk1 AS hqm
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_wrk (respondent_id, survey_receive_date, respondent_type_code, survey_sid, company_code, coid, parent_coid, pat_acct_num, patient_dw_id, discharge_date, exclusion_reason_code, cms_exclusion_ind, final_record_ind, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        ppd.respondent_id,
        ppd.survey_receive_date,
        ppd.respondent_type_code,
        ppd.survey_sid,
        ppd.company_code,
        ppd.coid,
        ppd.parent_coid,
        ppd.pat_acct_num,
        ppd.patient_dw_id,
        ppd.discharge_date,
        -- ppd.facility_claim_control_num,
        ppd.exclusion_reason_code,
        ppd.cms_exclusion_ind,
        ppd.final_record_ind,
        ppd.source_system_code,
        ppd.dw_last_update_date_time
      FROM
        (
          SELECT
              hqm.surv_id AS respondent_id,
              hqm.survey_receive_date,
              'P' AS respondent_type_code,
              rs.survey_sid AS survey_sid,
              ff.company_code,
              g.coid,
              g.parent_coid AS parent_coid,
              CASE
                WHEN upper(hqm.survey_type_ind) = 'Y' THEN NULL
                ELSE hqm.pat_acct_num
              END AS pat_acct_num,
              ca.patient_dw_id,
              -- CASE WHEN  HQM.SURVEY_TYPE_IND ='Y' then NULL
              -- 	else PRD.Discharge_Date  END	AS Discharge_Date
              hqm.disdate AS discharge_date,
              -- rbf.ccn as facility_claim_control_num,
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
              'N' AS final_record_ind,
              'H' AS source_system_code,
              current_ts AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk0 AS hqm
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON hqm.question_id = sq.question_id
               AND upper(sq.source_system_code) = 'H'
              INNER JOIN -- Added by MP on 06/14/17
              {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS g ON hqm.coid = g.coid
              LEFT OUTER JOIN -- INNER JOIN EDW_PUB_VIEWS.FACT_FACILITY FF
              {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = hqm.coid
              LEFT OUTER JOIN -- 	ON FF.COID=G.COID
              {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_detail AS prd ON prd.hca_unique = hqm.hca_unique
               AND hqm.surv_id = prd.survey_id
               LEFT OUTER JOIN
              {{ params.param_auth_base_views_dataset_name }}.hl7_clinical_keys AS ca ON g.parent_coid = ca.coid
               AND hqm.pat_acct_num = ca.patient_account_num
              -- LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}_copy.cdm_facility_detail AS rbf ON hqm.coid = rbf.coid
              --  AND ff.company_code = rbf.company_code
            WHERE upper(rs.survey_group_text) = 'PATIENT_SATISFACTION'
             AND sq.eff_to_date = '9999-12-31'
             AND rs.eff_to_date = '9999-12-31'
             AND g.coid IS NOT NULL
        ) AS ppd
  ;
END;