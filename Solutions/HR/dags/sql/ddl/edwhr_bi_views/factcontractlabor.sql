CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factcontractlabor AS SELECT
    a.pe_date,
    concat(a.coid, a.dept_num) AS coid_uid,
    a.coid_name,
    a.coid,
    a.dept_num,
    a.functional_dept_desc,
    sum(a.rn_contr_hrs) AS rn_contr_hrs,
    sum(a.rn_total_hrs) AS rn_total_hrs,
    sum(a.contr_hrs) AS contr_hrs,
    sum(a.total_hrs) AS total_hrs
  FROM
    (
      SELECT
          s.pe_date,
          ff.coid_name,
          ff.coid,
          s.dept_num,
          fd.functional_dept_desc,
          --  pull in hours for two stat codes: TOTL ACCR DEPT MANHRS (9000) and MANHOURS CONTRACT (9010) (sourced from EDWFS.Ref_Stat_Type)
          sum(CASE
            WHEN upper(s.stat_code) = '9010' THEN s.stat_cm_actual
            ELSE 0
          END) AS rn_contr_hrs,
          sum(CASE
            WHEN upper(s.stat_code) = '9000' THEN s.stat_cm_actual
            ELSE 0
          END) AS rn_total_hrs,
          sum(0) AS contr_hrs,
          sum(0) AS total_hrs
        FROM
          {{ params.param_hr_base_views_dataset_name }}.stats AS s
          INNER JOIN {{ params.param_pub_views_dataset_name }}.department AS pubd ON s.coid = pubd.coid
           AND s.dept_num = pubd.dept_num
          INNER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fd ON pubd.functional_dept_num = fd.functional_dept_num
          INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON s.coid = ff.coid
        WHERE extract(YEAR from CAST(s.pe_date as TIMESTAMP)) >= 2017.0
         AND upper(ff.lob_code) = 'HOS'
         AND upper(ff.company_code) = 'H'
         AND upper(ff.coid_status_code) = 'F'
         AND s.stat_code IN(
          '9010', '9000'
        )
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
          s.dept_num,
          fd.functional_dept_desc,
          sum(0) AS rn_contr_hrs,
          sum(0) AS rn_total_hrs,
          sum(CASE
            WHEN upper(s.stat_code) = '9010' THEN s.stat_cm_actual
            ELSE 0
          END) AS contr_hrs,
          sum(CASE
            WHEN upper(s.stat_code) = '9000' THEN s.stat_cm_actual
            ELSE 0
          END) AS total_hrs
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
         AND s.stat_code IN(
          '9010', '9000'
        )
        GROUP BY 1, 2, 3, 4, 5
    ) AS a
  GROUP BY 1, 2, 3, 4, 5, 6
;
