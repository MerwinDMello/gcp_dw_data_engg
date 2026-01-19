CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimcoid AS SELECT
    a.coid_uid,
    a.group_name,
    a.division_name,
    a.market_name,
    a.coid,
    a.coid_name,
    a.lob_code,
    a.sub_lob_code,
    a.cons_facility_code,
    a.cons_facility_name,
    a.hospital_level_code,
    a.same_store_type_code,
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
    a.state_code,
    a.county_name
  FROM
    (
      SELECT DISTINCT
          --  modifying to allow null COIDs and cost centers to eliminate nulls while we research what's causing base issue - 10/1/2021 ACC
          concat(coalesce(t.coid, '00000'), coalesce(t.cost_center, '000')) AS coid_uid,
          /*
          			-- allow for null COIDs in NCD data only - 00025 is NCD division code
          			case when ff.division_code='00025' then coalesce(t.Coid,'00000')||coalesce(t.cost_center,'000')
          			else t.coid||t.cost_center
          			END AS COID_UID,
          			*/
          ff.group_name,
          ff.division_name,
          ff.market_name,
          ff.coid,
          ff.coid_name,
          ff.lob_code,
          ff.sub_lob_code,
          ff.cons_facility_code,
          ff.cons_facility_name,
          fac.same_store_type_code,
          rchc.hospital_level_code,
          t.cost_center,
          pvdept.dept_name AS cost_center_desc,
          fsfd.functional_dept_desc,
          fsfd.sub_functional_dept_desc,
          fa.postal_code,
          rchc.year_num,
          rchc.quartile_rank_num,
          rchc.total_rank_num,
          fa.state_code,
          gl.county_name
        FROM
          (
            SELECT DISTINCT
                hrdr.coid,
                hrdr.cost_center
              FROM
                {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS hrdr
            UNION DISTINCT
            SELECT DISTINCT
                gls.coid,
                gls.gl_dept_num AS cost_center
              FROM
                {{ params.param_hr_views_dataset_name }}.hr_gl_summary AS gls
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON gls.coid = xwalk.coid
                 AND gls.gl_dept_num = xwalk.dept_num
                 AND DATE(xwalk.valid_to_date) = DATE('9999-12-31')
              WHERE extract(YEAR from CAST(gls.pe_date as TIMESTAMP)) >= extract(YEAR from date_add(current_date('US/Central'), interval -37 MONTH))
               AND trim(gls.fs_code) IN(
                '65050', '65100', '65070'
              )
              GROUP BY 1, 2
              HAVING sum(gls.gl_cm_actual) <> 0
          ) AS t
          INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON t.coid = ff.coid
          INNER JOIN {{ params.param_pub_views_dataset_name }}.facility AS fac ON t.coid = fac.coid
           AND (fac.eff_to_date) >= '2016-01-01'
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fsfd ON t.coid = fsfd.coid
           AND t.cost_center = fsfd.dept_num
          LEFT OUTER JOIN (
            SELECT
                ref_coid_hospital_category.coid,
                ref_coid_hospital_category.year_num,
                ref_coid_hospital_category.company_code,
                ref_coid_hospital_category.hospital_level_code,
                ref_coid_hospital_category.quartile_rank_num,
                ref_coid_hospital_category.total_rank_num
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_coid_hospital_category
              QUALIFY row_number() OVER (PARTITION BY ref_coid_hospital_category.coid ORDER BY ref_coid_hospital_category.year_num DESC) = 1
          ) AS rchc ON t.coid = rchc.coid
           AND ff.company_code = rchc.company_code
          LEFT OUTER JOIN --  qualify because the categories can change based on EBITDA year over year, so we want the highest year num / most recent year's value vs. hardcoding a year
          {{ params.param_pub_views_dataset_name }}.facility_address AS fa ON t.coid = fa.coid
           AND (fa.eff_to_date) = '9999-12-31'
           AND upper(fa.address_type_code) = 'PH'
          LEFT OUTER JOIN --  PH is physical address of the facility, not a mailing or tax-purposes address
          {{ params.param_pub_views_dataset_name }}.department AS pvdept ON t.cost_center = pvdept.dept_num
           AND t.coid = pvdept.coid
           AND upper(pvdept.source_system_code) = 'H'
           AND (pvdept.eff_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.geographic_location gl
            on fa.postal_code = gl.zip_code
    ) AS a
;
