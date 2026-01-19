-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/position_tenure.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.position_tenure AS SELECT
    last_day(DATE(fhrm.date_id)) AS pe_date,
    fhrm.coid,
    fhrm.process_level_code,
    concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num) AS pl_uid,
    fhrm.employee_sid,
    fhrm.employee_num,
    fhrm.position_sid,
    emp_tenure.position_key,
    emp_tenure.eff_from_date,
    emp_tenure.eff_to_date,
    fhrm.key_talent_id,
    fhrm.integrated_lob_id,
    fhrm.employee_status_sid,
    fhrm.auxiliary_status_sid,
    fhrm.job_class_sid,
    fhrm.job_code_sid,
    `{{ params.param_hr_bi_views_dataset_name }}`.months_between(fhrm.Date_Id, emp_tenure.Eff_From_Date) AS tenure_months
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS fhrm
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON fhrm.process_level_code = hrdr.process_level_code
     AND fhrm.coid = hrdr.coid
     AND fhrm.dept_sid = hrdr.dept_sid
     AND fhrm.lawson_company_num = hrdr.lawson_company_num
    INNER JOIN (
      SELECT
          emp_pos.lawson_company_num,
          emp_pos.employee_sid,
          emp_pos.position_sid,
          concat(emp_pos.position_sid, CAST(emp_pos.eff_to_date as STRING)) AS position_key,
          emp_pos.eff_from_date,
          emp_pos.eff_to_date
        FROM
          -- CASE WHEN emp_pos.Eff_To_Date = '9999-12-31' THEN Months_Between(hc.PE_Date, emp_pos.Eff_From_Date) ELSE Months_Between(emp_pos.Eff_To_Date, emp_pos.Eff_From_Date) END AS Tenure_Months
          (
            SELECT
                ep.lawson_company_num,
                ep.employee_sid,
                ep.position_sid,
                min(ep.eff_from_date) AS eff_from_date,
                max(ep.eff_to_date) AS eff_to_date
              FROM
                {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep
                INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON ep.position_sid = jp.position_sid
                 AND date(jp.valid_to_date) = '9999-12-31'
                 AND ep.eff_from_date BETWEEN jp.eff_from_date AND jp.eff_to_date
              WHERE date(ep.valid_to_date) = '9999-12-31'
              GROUP BY 1, 2, 3
          ) AS emp_pos
    ) AS emp_tenure ON emp_tenure.employee_sid = fhrm.employee_sid
     AND emp_tenure.position_sid = fhrm.position_sid
     AND DATE((extract(YEAR from last_day(DATE(fhrm.date_id))) - 1900) * 10000 , extract(MONTH from last_day(DATE(fhrm.date_id))) * 100 , extract(DAY from last_day(DATE(fhrm.date_id)))) BETWEEN emp_tenure.eff_from_date AND emp_tenure.eff_to_date
  WHERE fhrm.analytics_msr_sid = 80100
   AND last_day(DATE(fhrm.date_id)) = fhrm.date_id
   AND fhrm.date_id BETWEEN DATE'2018-01-01' AND date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
;
