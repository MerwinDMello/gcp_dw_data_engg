CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factcontractovertime AS SELECT
    a.pe_date,
    concat(a.coid, a.dept_num) AS coid_uid,
    a.coid_name,
    a.coid,
    a.functional_dept_desc,
    a.dept_num,
    sum(a.ot_spend) AS ot_spend,
    sum(a.ot_paid_hours) AS ot_paid_hours,
    sum(a.total_paid_hours) AS total_paid_hours
  FROM
    (
      SELECT
          s.pe_date,
          ff.coid_name,
          ff.coid,
          fd.functional_dept_desc,
          s.dept_num,
          sum(CASE
            WHEN upper(s.stat_code) = '9310' THEN s.stat_cm_actual
            ELSE 0
          END) AS ot_spend,
          sum(CASE
            WHEN upper(s.stat_code) = '9110' THEN s.stat_cm_actual
            ELSE 0
          END) AS ot_paid_hours,
          sum(CASE
            WHEN upper(s.stat_code) = '9100' THEN s.stat_cm_actual
            ELSE 0
          END) AS total_paid_hours
        FROM
          {{ params.param_fs_base_views_dataset_name }}.stats AS s
          INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON s.pe_date = lud.date_id
          INNER JOIN {{ params.param_pub_views_dataset_name }}.department AS pubd ON s.coid = pubd.coid
           AND s.dept_num = pubd.dept_num
          INNER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fd ON pubd.functional_dept_num = fd.functional_dept_num
          INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON s.coid = ff.coid
        WHERE extract(YEAR from CAST(s.pe_date as TIMESTAMP)) >= 2017.0
         AND upper(ff.lob_code) = 'HOS'
         AND upper(ff.company_code) = 'H'
         AND upper(ff.coid_status_code) = 'F'
         AND (upper(fd.functional_dept_desc) LIKE 'NURSING MEDICAL/SURGICAL%'
         OR upper(fd.functional_dept_desc) LIKE '%NURSING-OTHER%'
         OR upper(fd.functional_dept_desc) LIKE '%ICU/CC%'
         OR upper(fd.functional_dept_desc) LIKE '%PEDIATRIC%'
         OR upper(fd.functional_dept_desc) LIKE '%PSYCHIA%'
         OR upper(fd.functional_dept_desc) LIKE '%NURSERY/OB%'
         OR upper(fd.functional_dept_desc) LIKE '%NEONATA%'
         OR upper(fd.functional_dept_desc) LIKE '%SURGERY%'
         OR upper(fd.functional_dept_desc) LIKE '%EMERGENCY ROO%')
        GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
          s.pe_date,
          ff.coid_name,
          ff.coid,
          fd.functional_dept_desc,
          s.dept_num,
          sum(CASE
            WHEN upper(s.stat_code) = '9310' THEN s.stat_cm_actual
            ELSE 0
          END) AS ot_spend,
          sum(CASE
            WHEN upper(s.stat_code) = '9110' THEN s.stat_cm_actual
            ELSE 0
          END) AS ot_paid_hours,
          sum(CASE
            WHEN upper(s.stat_code) = '9100' THEN s.stat_cm_actual
            ELSE 0
          END) AS total_paid_hours
        FROM
          {{ params.param_hr_base_views_dataset_name }}.stats AS s
          INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON s.pe_date = lud.date_id
          INNER JOIN {{ params.param_pub_views_dataset_name }}.department AS pubd ON s.coid = pubd.coid
           AND s.dept_num = pubd.dept_num
          INNER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fd ON pubd.functional_dept_num = fd.functional_dept_num
          INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON s.coid = ff.coid
        WHERE extract(YEAR from CAST(s.pe_date as TIMESTAMP)) >= 2017.0
         AND upper(ff.lob_code) = 'HOS'
         AND upper(ff.company_code) = 'H'
         AND upper(ff.coid_status_code) = 'F'
        GROUP BY 1, 2, 3, 4, 5
    ) AS a
  GROUP BY 1, 2, 3, 4, 5, 6
;
