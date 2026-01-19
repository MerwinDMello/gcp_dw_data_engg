-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_corrected_priority_payor_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*) + 1) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_remittance_corrected_priority_payor.corrected_priority_payor_sid), 0)
      FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_corrected_priority_payor
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) + row_number() OVER (
                                                                                                ORDER BY upper(f.corrected_priority_payor_qualifier_code),
                                                                                                         upper(f.corrected_priority_payor_id),
                                                                                                         upper(f.corrected_priority_payor_name)) AS corrected_priority_payor_sid, --  SID
 f.corrected_priority_payor_qualifier_code,
 f.corrected_priority_payor_id,
 f.corrected_priority_payor_name,
 'E' AS source_system_code,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT max(remittance_claim.corretd_prior_payor_qulifr_cod) AS corrected_priority_payor_qualifier_code,
             max(remittance_claim.corrected_priority_payor_num) AS corrected_priority_payor_id,
             max(remittance_claim.nm103_corretd_priority_payr_nm) AS corrected_priority_payor_name
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE DATE(remittance_claim.dw_last_update_date_time) =
          (SELECT max(DATE(remittance_claim_0.dw_last_update_date_time))
           FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim AS remittance_claim_0)
        AND upper(coalesce(remittance_claim.corretd_prior_payor_qulifr_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.corrected_priority_payor_num, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.nm103_corretd_priority_payr_nm, '')) NOT IN('')
      GROUP BY upper(remittance_claim.corretd_prior_payor_qulifr_cod),
               upper(remittance_claim.corrected_priority_payor_num),
               upper(remittance_claim.nm103_corretd_priority_payr_nm)) AS f
   WHERE (upper(f.corrected_priority_payor_qualifier_code),
          upper(f.corrected_priority_payor_id),
          upper(f.corrected_priority_payor_name)) NOT IN
       (SELECT AS STRUCT upper(ref_remittance_corrected_priority_payor.corrected_priority_payor_qualifier_code) AS corrected_priority_payor_qualifier_code,
                         upper(ref_remittance_corrected_priority_payor.corrected_priority_payor_id) AS corrected_priority_payor_id,
                         upper(ref_remittance_corrected_priority_payor.corrected_priority_payor_name) AS corrected_priority_payor_name
        FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_corrected_priority_payor
        FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) ) AS a