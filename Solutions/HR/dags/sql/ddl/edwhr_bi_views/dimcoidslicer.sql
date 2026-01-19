-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/dimcoidslicer.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimcoidslicer AS SELECT
    a.coid_uid,
    a.group_name,
    a.division_name,
    a.market_name,
    a.coid,
    a.coid_name,
    a.process_level_name,
    a.process_level,
    a.business_unit_name,
    a.business_unit_segment_name,
    a.business_unit_name_short,
    a.lob_code,
    a.sub_lob_code,
    a.cons_facility_code,
    a.cons_facility_name,
    a.hospital_level_code,
    a.same_store_type_code,
    a.dept_code,
    a.dept_name,
    a.cost_center,
    a.cost_center_desc,
    a.functional_dept_desc,
    a.sub_functional_dept_desc,
    CASE
      WHEN upper(a.division_name) = 'CENTRAL AND WEST TEXAS DIVISION' THEN 'CWT'
      WHEN upper(a.division_name) = 'NORTH TEXAS DIVISION' THEN 'NTX'
      WHEN upper(a.division_name) = 'CAPITAL DIVISION' THEN 'CAP'
      WHEN upper(a.division_name) = 'CONTINENTAL DIVISION' THEN 'CON'
      WHEN upper(a.division_name) = 'WEST FLORIDA DIVISION' THEN 'WFL'
      WHEN upper(a.division_name) = 'NORTH FLORIDA DIVISION' THEN 'NFL'
      WHEN upper(a.division_name) = 'EAST FLORIDA DIVISION' THEN 'EFL'
      WHEN upper(a.division_name) = 'SOUTH ATLANTIC DIVISION' THEN 'SAD'
      WHEN upper(a.division_name) = 'FAR WEST DIVISION' THEN 'FWD'
      WHEN upper(a.division_name) = 'MOUNTAIN DIVISION' THEN 'MTN'
      WHEN upper(a.division_name) = 'SAN ANTONIO DIVISION' THEN 'SAN'
      WHEN upper(a.division_name) = 'GULF COAST DIVISION' THEN 'GCD'
      WHEN upper(a.division_name) = 'TRISTAR DIVISION' THEN 'TRI'
      WHEN upper(a.division_name) = 'MIDAMERICA DIVISION' THEN 'MID'
      WHEN upper(a.division_name) = 'NORTH CAROLINA DIVISION' THEN 'NCD'
      ELSE 'UNKNOWN'
    END AS division_abbreviation,
    a.year_num,
    a.quartile_rank_num,
    a.total_rank_num,
    a.postal_code,
    a.custom_group
  FROM
    (
      SELECT DISTINCT
          concat(coalesce(t.coid, '00000'), coalesce(t.cost_center, '000')) AS coid_uid,
          ff.group_name,
          ff.division_name,
          ff.market_name,
          ff.coid,
          ff.coid_name,
          pl.process_level_name,
          ff.lob_code,
          ff.sub_lob_code,
          ff.cons_facility_code,
          ff.cons_facility_name,
          fac.same_store_type_code,
          rchc.hospital_level_code,
          t.dept_code,
          dept.dept_name,
          t.cost_center,
          pvdept.dept_name AS cost_center_desc,
          t.process_level_code AS process_level,
          hrop.business_unit_name AS business_unit_name,
          hrop.business_unit_segment_name AS business_unit_segment_name,
          CASE
            WHEN upper(hrop.business_unit_name) = 'HEALTHTRUST WORKFORCE SOLN' THEN 'HWS'
            ELSE hrop.business_unit_name
          END AS business_unit_name_short,
          fsfd.functional_dept_desc,
          fsfd.sub_functional_dept_desc,
          fa.postal_code,
          rchc.year_num,
          rchc.quartile_rank_num,
          rchc.total_rank_num,
          CASE
            WHEN xwalk.lawson_company_num = 5920 THEN 'HRG'
            WHEN upper(ff.lob_code) = 'OPS'
             AND upper(ff.sub_lob_code) = 'OTH'
             AND pl.process_level_code = '26149' THEN 'OSG'
            WHEN upper(ff.lob_code) = 'CSC'
             AND upper(ff.sub_lob_code) = 'OTH'
             AND pl.process_level_code IN(
              '25505', '8452', '9375'
            ) THEN 'OSG'
            WHEN upper(ff.lob_code) = 'CSC'
             AND upper(ff.sub_lob_code) = 'CBO' THEN 'OSG'
            WHEN upper(ff.lob_code) = 'OPS'
             AND upper(ff.sub_lob_code) = 'GRP' THEN 'OSG'
            WHEN upper(ff.lob_code) = 'OPS'
             AND upper(ff.sub_lob_code) = 'DIV' THEN 'OSG'
            WHEN upper(ff.lob_code) = 'ONC'
             AND upper(ff.sub_lob_code) = 'OTH' THEN 'OSG'
            WHEN upper(ff.lob_code) = 'CSC'
             AND upper(ff.sub_lob_code) = 'GPO' THEN 'Parallon/HealthTrust'
            WHEN upper(ff.lob_code) = 'SHA' THEN 'Parallon/HealthTrust'
            WHEN upper(ff.lob_code) = 'SGR' THEN 'OSG'
            WHEN ff.lob_code IN(
              'PHY', 'ECH'
            ) THEN 'PSG'
            WHEN upper(ff.lob_code) = 'HOS'
             AND upper(ff.group_name) = 'AMERICAN GROUP' THEN 'American HOS Only'
            WHEN upper(ff.lob_code) = 'HOS'
             AND upper(ff.group_name) = 'NATIONAL GROUP' THEN 'National HOS Only'
            ELSE 'Other'
          END AS custom_group,
          row_number() OVER (PARTITION BY upper(concat(coalesce(t.coid, '00000'), coalesce(t.cost_center, '000'))) ORDER BY upper(concat(coalesce(t.coid, '00000'), coalesce(t.cost_center, '000'))), rchc.year_num DESC) AS coid_count
        FROM
          (
            SELECT DISTINCT
                hrdr.coid,
                hrdr.cost_center,
                hrdr.process_level_code,
                hrdr.dept_code
              FROM
                {{ params.param_hr_bi_views_dataset_name }}.hr_dept_rollup AS hrdr
            UNION DISTINCT
            SELECT DISTINCT
                gls.coid,
                gls.gl_dept_num AS cost_center,
                xwalk_0.process_level_code,
                xwalk_0.account_unit_num AS dept_code
              FROM
                {{ params.param_hr_views_dataset_name }}.hr_gl_summary AS gls
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk_0 ON gls.coid = xwalk_0.coid
                 AND gls.gl_dept_num = xwalk_0.dept_num
                 AND date(xwalk_0.valid_to_date) = '9999-12-31'
              WHERE extract(YEAR from CAST(gls.pe_date as TIMESTAMP)) >= extract(YEAR from date_add(current_date('US/Central'), interval -37 MONTH))
               AND gls.fs_code IN(
                '65050', '65100', '65070'
              )
              GROUP BY 1, 2, 3, 4
              HAVING sum(gls.gl_cm_actual) <> 0
          ) AS t
          INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON t.coid = ff.coid
          INNER JOIN {{ params.param_pub_views_dataset_name }}.facility AS fac ON t.coid = fac.coid
           AND date(fac.eff_to_date) >= '2016-01-01'
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fsfd ON t.coid = fsfd.coid
           AND t.cost_center = fsfd.dept_num
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_coid_hospital_category AS rchc ON t.coid = rchc.coid
           AND ff.company_code = rchc.company_code
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON t.process_level_code = pl.process_level_code
           AND date(pl.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON t.dept_code = dept.dept_code
           AND t.process_level_code = dept.process_level_code
           AND date(dept.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.facility_address AS fa ON t.coid = fa.coid
           AND date(fa.eff_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON t.cost_center = xwalk.dept_num
           AND t.coid = xwalk.coid
           AND t.process_level_code = xwalk.process_level_code
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.department AS pvdept ON t.cost_center = pvdept.dept_num
           AND t.coid = pvdept.coid
           AND date(pvdept.eff_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hrop ON t.process_level_code = hrop.process_level_code
    ) AS a
  WHERE a.coid_count = 1
;
