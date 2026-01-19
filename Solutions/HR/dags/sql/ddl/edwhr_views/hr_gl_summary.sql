
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_gl_summary AS SELECT
      gl.pe_date,
      gl.company_code,
      gl.coid,
      fs.fs_code,
      fs.fs_code_desc,
      gl.gl_dept_num,
      gl.gl_sub_account_num,
      gl.gl_cm_actual,
      gl.gl_cm_budget,
      gl.gl_cm_prior_year,
      gl.gl_qtd_actual,
      gl.gl_qtd_budget,
      gl.gl_qtd_prior_year,
      gl.gl_ytd_actual,
      gl.gl_ytd_budget,
      gl.gl_ytd_prior_year
    FROM
      {{ params.param_hr_base_views_dataset_name }}.gl_summary AS gl
      INNER JOIN {{ params.param_hr_views_dataset_name }}.bi_fs_code AS fs ON gl.coid = fs.coid
       AND gl.gl_dept_num = fs.gl_dept_num
       AND gl.gl_sub_account_num = fs.gl_sub_account_num
       AND gl.company_code = fs.company_code
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.secref_facility AS b ON gl.company_code = b.company_code
       AND gl.coid = b.co_id
       AND b.user_id = session_user()
    WHERE date(gl.pe_date) >= '2015-01-01'
     AND fs.fs_code IN(
      '50100', '50200', '50400', '50900', '65050', '65070', '65100'
    )
  ;

