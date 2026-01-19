--  5/12/2021 ACC updating this to reduce spool space as suggested by DBAs (R.Gudde)... reduces by 142 gb and will help cut down on spool issues
--  replacing view in all databases
-- 
-- ----------- EDWHR_VIEWS

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS SELECT DISTINCT
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
            fact_hr_metric.coid,
            fact_hr_metric.process_level_code,
            fact_hr_metric.lawson_company_num,
            fact_hr_metric.dept_sid
          FROM
            {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric
          GROUP BY 1, 2, 3, 4
      ) AS hrmet
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
       AND date(dept.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON hrmet.coid = xwalk.coid
       AND hrmet.process_level_code = xwalk.process_level_code
       AND dept.dept_code = xwalk.account_unit_num
       AND date(xwalk.valid_to_date) = '9999-12-31'
  ;

