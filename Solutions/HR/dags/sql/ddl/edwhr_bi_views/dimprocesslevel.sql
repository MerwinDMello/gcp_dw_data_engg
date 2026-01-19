CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimprocesslevel AS SELECT DISTINCT
    concat(coalesce(t.coid, '00000'), t.process_level_code, coalesce(t.dept_code, '00000'), t.lawson_company_num) AS pl_uid,
    concat(coalesce(t.coid, '00000'), coalesce(t.cost_center, '000')) AS coid_uid,
    CASE
      WHEN upper(hrop.business_unit_name) = 'HEALTHTRUST WORKFORCE SOLN' THEN 'HWS'
      WHEN upper(hrop.business_unit_name) = 'HUMAN RESOURCES GROUP' THEN 'HRG'
      ELSE hrop.business_unit_name
    END AS business_unit_name,
    hrop.business_unit_segment_name,
    t.dept_code,
    dept.dept_name,
    t.lawson_company_num,
    t.process_level_code,
    pl.process_level_name,
    t.coid,
    t.cost_center
  FROM
    (
      SELECT
          hrdr.coid,
          hrdr.cost_center,
          hrdr.process_level_code,
          hrdr.lawson_company_num,
          hrdr.dept_code
        FROM
          {{ params.param_hr_bi_views_dataset_name }}.hr_dept_rollup AS hrdr
    ) AS t
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON t.process_level_code = pl.process_level_code
     AND t.lawson_company_num = pl.lawson_company_num
     AND date(pl.valid_to_date) = date('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON t.dept_code = dept.dept_code
     AND t.process_level_code = dept.process_level_code
     AND t.lawson_company_num = dept.lawson_company_num
     AND date(dept.valid_to_date) = date('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hrop ON t.process_level_code = hrop.process_level_code
;
