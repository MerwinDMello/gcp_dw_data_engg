CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factcontractlaborspend AS
SELECT
    gls.pe_date,
    concat(gls.coid, gls.gl_dept_num) AS coid_uid,
    fundept.functional_dept_desc,
    gls.gl_dept_num,
    gls.gl_sub_account_num,
    gls.coid,
    gls.fs_code_desc,
    gls.fs_code,
    CASE
      WHEN upper(fundept.functional_dept_desc) LIKE 'NURSING MEDICAL/SURGICAL%'
       OR upper(fundept.functional_dept_desc) LIKE '%NURSING-OTHER%'
       OR upper(fundept.functional_dept_desc) LIKE '%ICU/CC%'
       OR upper(fundept.functional_dept_desc) LIKE '%PEDIATRIC%'
       OR upper(fundept.functional_dept_desc) LIKE '%PSYCHIA%'
       OR upper(fundept.functional_dept_desc) LIKE '%NURSERY/OB%'
       OR upper(fundept.functional_dept_desc) LIKE '%NEONATA%'
       OR upper(fundept.functional_dept_desc) LIKE '%SURGERY%'
       OR upper(fundept.functional_dept_desc) LIKE '%EMERGENCY ROO%' THEN 'RN Spend'
      ELSE 'Non-nursing Spend'
    END AS nursing_spend_flag,
    gls.gl_cm_actual
  FROM
    {{ params.param_hr_views_dataset_name }}.hr_gl_summary AS gls
    INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON upper(gls.coid) = upper(ff.coid)
     AND upper(ff.company_code) = 'H'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.department AS dept ON upper(gls.gl_dept_num) = upper(dept.dept_num)
     AND upper(gls.coid) = upper(dept.coid)
     AND gls.pe_date BETWEEN dept.eff_from_date AND dept.eff_to_date
     AND upper(dept.source_system_code) = 'Q'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON upper(dept.functional_dept_num) = upper(fundept.functional_dept_num)
  WHERE gls.pe_date >= date_add(current_date('US/Central'), interval -36 MONTH)
   AND CASE
     gls.fs_code
    WHEN '' THEN 0.0
    ELSE CAST(gls.fs_code as FLOAT64)
  END IN(
    65050, 65100, 65070
  )
   AND gls.gl_cm_actual <> 0
   AND upper(gls.coid) NOT IN(
    SELECT
        upper(ff_0.coid) AS coid
      FROM
        {{ params.param_pub_views_dataset_name }}.fact_facility AS ff_0
      WHERE upper(ff_0.group_name) = 'INTERNATIONAL GROUP'
  );