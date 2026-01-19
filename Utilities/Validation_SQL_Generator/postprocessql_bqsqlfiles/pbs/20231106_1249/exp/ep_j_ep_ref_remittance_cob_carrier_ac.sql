-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_cob_carrier_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*) + 1) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_remittance_cob_carrier.cob_carrier_sid), 0)
      FROM `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_cob_carrier) + row_number() OVER (
                                                                                              ORDER BY upper(f.cob_qualifier_code),
                                                                                                       upper(f.cob_carrier_num),
                                                                                                       upper(f.cob_carrier_name)) AS cob_carrier_sid, --  SID
 f.cob_qualifier_code,
 f.cob_carrier_num,
 f.cob_carrier_name,
 'E' AS source_system_code,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT max(remittance_claim.crossover_carrier_qualifr_code) AS cob_qualifier_code,
             max(remittance_claim.cordintn_of_benefit_carrier_nm) AS cob_carrier_num,
             max(remittance_claim.coordintn_of_beneft_carrier_nm) AS cob_carrier_name
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE DATE(remittance_claim.dw_last_update_date_time) =
          (SELECT max(DATE(remittance_claim_0.dw_last_update_date_time))
           FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim AS remittance_claim_0)
        AND upper(coalesce(remittance_claim.crossover_carrier_qualifr_code, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.coordintn_of_beneft_carrier_nm, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.cordintn_of_benefit_carrier_nm, '')) NOT IN('')
      GROUP BY upper(remittance_claim.crossover_carrier_qualifr_code),
               upper(remittance_claim.cordintn_of_benefit_carrier_nm),
               upper(remittance_claim.coordintn_of_beneft_carrier_nm)) AS f
   WHERE (upper(f.cob_qualifier_code),
          upper(f.cob_carrier_num),
          upper(f.cob_carrier_name)) NOT IN
       (SELECT AS STRUCT upper(ref_remittance_cob_carrier.cob_qualifier_code) AS cob_qualifier_code,
                         upper(ref_remittance_cob_carrier.cob_carrier_num) AS cob_carrier_num,
                         upper(ref_remittance_cob_carrier.cob_carrier_name) AS cob_carrier_name
        FROM `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_cob_carrier) ) AS a 