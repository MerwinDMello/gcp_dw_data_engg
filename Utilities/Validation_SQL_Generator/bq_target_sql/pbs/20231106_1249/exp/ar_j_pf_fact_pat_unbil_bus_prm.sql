-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ar_j_pf_fact_pat_unbil_bus_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    coalesce(trim(CAST(coalesce(sum(du.unbilled_charge_amt), NUMERIC '0') as STRING)), '0') AS source_string
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.discharged_unbilled AS du
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_dimension AS fd ON upper(fd.company_code) = upper(du.company_code)
     AND upper(fd.coid) = upper(du.coid)
  WHERE upper(format_date('%Y%m', du.effective_date)) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
   AND upper(du.unbilled_responsibility_ind) = 'B'
   AND (upper(du.company_code) = 'H'
   OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y')
   AND upper(du.account_process_ind) = 'S'
;
