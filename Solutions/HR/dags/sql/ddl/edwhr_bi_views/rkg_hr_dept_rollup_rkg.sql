-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/rkg_hr_dept_rollup_rkg.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.rkg_hr_dept_rollup_rkg AS SELECT
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
          fact_hr_metric_snapshot.coid,
          fact_hr_metric_snapshot.process_level_code,
          fact_hr_metric_snapshot.lawson_company_num,
          fact_hr_metric_snapshot.dept_sid
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot
        GROUP BY 1, 2, 3, 4
    ) AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON hrmet.coid = xwalk.coid
     AND hrmet.process_level_code = xwalk.process_level_code
     AND dept.dept_code = xwalk.account_unit_num
     AND date(xwalk.valid_to_date) = '9999-12-31'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
;
