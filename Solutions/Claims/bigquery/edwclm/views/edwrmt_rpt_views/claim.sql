CREATE OR REPLACE VIEW {{ params.param_rmt_rpt_views_dataset_name }}.claim 
AS SELECT
    c.vendor_cid AS service_center_id,
    CASE
       rtrim(c.vendor_cid, ' ')
      WHEN '07261' THEN 'MSC'
      WHEN '07262' THEN 'NASHVILLE'
      WHEN '07263' THEN 'HOUSTON'
      WHEN '07264' THEN 'DALLAS'
      WHEN '07265' THEN 'SAN ANTONIO'
      WHEN '07266' THEN 'TAMPA'
      WHEN '07267' THEN 'ORANGE PARK'
      WHEN '07268' THEN 'RICHMOND'
      WHEN '07269' THEN 'NASHVILLE WEST/ATLANTA'
      ELSE c.vendor_cid
    END AS service_center_name,
    c.unit_num AS facility_number,
    max(CASE
      WHEN fd.coid_name IS NULL THEN 'UNKNOWN'
      ELSE fd.coid_name
    END) AS facility_name,
    c.bill_dt AS claim_bill_date,
    c.npi,
    DATE(c.dw_last_update_date_time) AS claim_received_date,
    max(CASE
      WHEN fd.facilitygroup IS NULL THEN 'UNKNOWN'
      ELSE fd.facilitygroup
    END) AS facility_group,
    count(c.claim_id) AS no_of_claims,
    sum(c.total_charge_amt) AS total_charge_amount
  FROM
    {{ params.param_clm_base_views_dataset_name }}.fact_claim AS c
    LEFT OUTER JOIN (
      SELECT
          fd_0.unit_num,
          fd_0.coid_name,
          fd_0.pas_coid,
          CASE
            WHEN rtrim(fd_0.pas_coid, ' ') = '08910' THEN 'MSC'
            WHEN rtrim(fd_0.pas_coid, ' ') = '08591' THEN 'NASHVILLE WEST/ATLANTA'
            WHEN rtrim(fd_0.pas_coid, ' ') = '08648' THEN 'RICHMOND'
            WHEN rtrim(fd_0.pas_coid, ' ') = '08942' THEN 'NASHVILLE'
            WHEN rtrim(fd_0.pas_coid, ' ') IN(
              '08945', '25464'
            ) THEN 'ORANGE PARK'
            WHEN rtrim(fd_0.pas_coid, ' ') = '08947' THEN 'TAMPA'
            WHEN rtrim(fd_0.pas_coid, ' ') = '08948' THEN 'HOUSTON'
            WHEN rtrim(fd_0.pas_coid, ' ') = '08949' THEN 'SAN ANTONIO'
            WHEN rtrim(fd_0.pas_coid, ' ') = '08950' THEN 'DALLAS'
            WHEN rtrim(fd_0.pas_coid, ' ') = '27060' THEN 'HEALTH AT HOME'
            ELSE fd_0.pas_current_name
          END AS pas_coid_name,
          CASE
            WHEN upper(rtrim(fd_0.corporate_name, ' ')) = 'HCA, INC.' THEN 'HCA'
            WHEN upper(rtrim(fd_0.corporate_name, ' ')) = 'LIFEPOINT HEALTH, INC' THEN 'LIFEPOINT'
            WHEN upper(rtrim(fd_0.corporate_name, ' ')) = 'NON - HCA'
             AND upper(rtrim(fd_0.sector_name, ' ')) = 'NON - HCA' THEN fd_0.market_name
            WHEN upper(rtrim(fd_0.corporate_name, ' ')) = 'NON - HCA'
             AND upper(rtrim(fd_0.sector_name, ' ')) <> 'NON - HCA' THEN fd_0.sector_name
            ELSE fd_0.corporate_name
          END AS facilitygroup
        FROM
          {{ params.param_clm_base_views_dataset_name }}.facility_dimension AS fd_0
        WHERE rtrim(fd_0.unit_num, ' ') NOT IN(
          '00000'
        )
    ) AS fd ON rtrim(fd.unit_num, ' ') = rtrim(c.unit_num, ' ')
  WHERE DATE(c.dw_last_update_date_time) >= DATE(current_date('US/Central') - INTERVAL 1 YEAR)
   AND rtrim(c.vendor_cid, ' ') NOT IN(
    '99999'
  )
  GROUP BY 1, 2, 3, upper(CASE
    WHEN fd.coid_name IS NULL THEN 'UNKNOWN'
    ELSE fd.coid_name
  END), 5, 6, 7, upper(CASE
    WHEN fd.facilitygroup IS NULL THEN 'UNKNOWN'
    ELSE fd.facilitygroup
  END)
;
