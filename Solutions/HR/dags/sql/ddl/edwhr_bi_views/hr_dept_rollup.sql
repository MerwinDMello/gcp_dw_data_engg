create or replace view {{ params.param_hr_bi_views_dataset_name }}.hr_dept_rollup as
SELECT DISTINCT
    hrmet.coid,
    hrmet.process_level_code,
    hrmet.lawson_company_num,
    hrmet.dept_sid,
    dept.dept_code,
    xwalk.dept_num AS cost_center,
    xwalk.gl_company_num,
    xwalk.account_unit_num
  FROM
    (
      SELECT
          upper(fact_hr_metric.coid) AS coid,
          upper(fact_hr_metric.process_level_code) AS process_level_code,
          fact_hr_metric.lawson_company_num,
          fact_hr_metric.dept_sid
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric
        GROUP BY 1, 2, 3, 4
    ) AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND DATE(dept.valid_to_date) = DATE '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON upper(hrmet.coid) = upper(xwalk.coid)
     AND upper(hrmet.process_level_code) = upper(xwalk.process_level_code)
     AND upper(dept.dept_code) = upper(xwalk.account_unit_num)
     AND DATE(xwalk.valid_to_date) = DATE '9999-12-31'