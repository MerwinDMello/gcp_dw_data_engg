CREATE OR REPLACE VIEW {{ params.param_rmt_rpt_views_dataset_name }}.remit
 AS SELECT
    CASE
      WHEN fd.pas_coid IS NULL THEN 'UNKNOWN'
      ELSE fd.pas_coid
    END AS service_center_id,
    max(CASE
      WHEN fd.pas_coid_name IS NULL THEN 'UNKNOWN'
      ELSE fd.pas_coid_name
    END) AS service_center_name,
    CASE
      WHEN p.unit_num IS NULL THEN '00000'
      ELSE p.unit_num
    END AS facility_number,
    max(CASE
      WHEN fd.coid_name IS NULL THEN 'UNKNOWN'
      ELSE fd.coid_name
    END) AS facility_name,
    m.remit_entered_dt,
    max(pr.npi) AS npi,
    DATE(p.dw_last_update_date_time) AS remit_received_date,
    max(CASE
      WHEN fd.facilitygroup IS NULL THEN 'UNKNOWN'
      ELSE fd.facilitygroup
    END) AS facilitygroup,
    max(CASE
      WHEN upper(rtrim(p.customer_cd, ' ')) = 'ECASH' THEN 'Manual_Remits'
      WHEN upper(rtrim(p.customer_cd, ' ')) IN(
        'HCAD', 'HCA'
      ) THEN 'Payer_Remits'
      WHEN upper(rtrim(p.customer_cd, ' ')) IN(
        'PSCD', 'PSCL'
      ) THEN 'PSC_Remits'
      ELSE p.customer_cd
    END) AS remit_type,
    count(p.patient_remit_sid) AS no_of_remits,
    sum(p.tot_claim_charge_amt) AS total_amount_charged,
    sum(p.remit_payment_amt) AS total_amount_paid,
    sum(p.patient_resp_amt) AS total_patientresp_amount
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.fact_patient_remit AS p
    INNER JOIN {{ params.param_rmt_base_views_dataset_name }}.fact_master_remit AS m ON upper(rtrim(m.remit_id, ' ')) = upper(rtrim(p.remit_id, ' '))
    INNER JOIN {{ params.param_rmt_base_views_dataset_name }}.lu_remit_provider AS pr ON upper(rtrim(p.remit_provider_id, ' ')) = upper(rtrim(pr.remit_provider_id, ' '))
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
          {{ params.param_rmt_base_views_dataset_name }}.facility_dimension AS fd_0
        WHERE rtrim(fd_0.unit_num, ' ') NOT IN(
          '00000'
        )
    ) AS fd ON rtrim(fd.unit_num, ' ') = rtrim(p.unit_num, ' ')
  WHERE DATE(p.dw_last_update_date_time) >= DATE(current_date('US/Central') - INTERVAL 1 YEAR)
  GROUP BY 1, upper(CASE
    WHEN fd.pas_coid_name IS NULL THEN 'UNKNOWN'
    ELSE fd.pas_coid_name
  END), 3, upper(CASE
    WHEN fd.coid_name IS NULL THEN 'UNKNOWN'
    ELSE fd.coid_name
  END), 5, upper(pr.npi), 7, upper(CASE
    WHEN fd.facilitygroup IS NULL THEN 'UNKNOWN'
    ELSE fd.facilitygroup
  END), upper(CASE
    WHEN upper(rtrim(p.customer_cd, ' ')) = 'ECASH' THEN 'Manual_Remits'
    WHEN upper(rtrim(p.customer_cd, ' ')) IN(
      'HCAD', 'HCA'
    ) THEN 'Payer_Remits'
    WHEN upper(rtrim(p.customer_cd, ' ')) IN(
      'PSCD', 'PSCL'
    ) THEN 'PSC_Remits'
    ELSE p.customer_cd
  END)
;
