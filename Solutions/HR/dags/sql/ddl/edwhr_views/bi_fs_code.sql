
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.bi_fs_code AS SELECT
      g.company_code,
      g.coid,
      g.gl_dept_num,
      g.gl_sub_account_num,
      g.source_system_code,
      g.gl_account_rollup_group_code AS fs_code,
      r.gl_account_rollup_code_desc AS fs_code_desc
    FROM
      {{ params.param_pub_views_dataset_name }}.gl_coa_gl_acct_rollup AS g
      INNER JOIN {{ params.param_pub_views_dataset_name }}.ref_gl_account_rollup AS r ON g.gl_account_rollup_group_code = r.gl_account_rollup_group_code
       AND g.gl_account_rollup_set_code = r.gl_account_rollup_set_code
    WHERE upper(g.gl_account_rollup_set_code) = 'FS'
  ;

