-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_remittance_payment_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT a.alternate_sn_format_ind AS alternate_sn_format_ind,
          a.apc_amt AS apc_amt,
          a.apply_er_patient_rule_ind AS apply_er_patient_rule_ind,
          a.apply_noncvrd_charges_rule_ind AS apply_noncovered_charges_rule_ind,
          a.apply_recover_rule_ind AS apply_recover_rule_ind,
          a.apply_secondary_rule_ind AS apply_secondary_rule_ind,
          a.apply_total_charges_rule_ind AS apply_total_charges_rule_ind,
          a.audit_date AS audit_date,
          a.bill_charge_amt AS bill_charge_amt,
          a.bill_process_date AS bill_process_date,
          a.bill_type_code AS bill_type_code,
          a.calculated_covered_days_ind AS calculated_covered_days_ind,
          a.capital_amt AS capital_amt,
          a.check_amt AS check_amt,
          a.check_date AS check_date,
          a.check_num AS check_num,
          a.claim_cnt AS claim_cnt,
          a.claim_payment_amt AS claim_payment_amt,
          a.coid AS coid,
          a.comment_transaction_ind AS comment_transaction_ind,
          a.company_code AS company_code,
          a.cost_report_day_cnt AS cost_report_day_cnt,
          a.covered_charge_amt AS covered_charge_amt,
          a.delete_date AS delete_date,
          a.delete_ind AS delete_ind,
          a.discharge_date_replacement_ind AS discharge_date_replacement_ind,
          a.disproportionate_share_amt AS disproportionate_share_amt,
          a.drg_amt AS drg_amt,
          a.drg_replacement_ind AS drg_replacement_ind,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
          a.ep_calc_blood_deductible_amt AS ep_calc_blood_deductible_amt,
          a.ep_calc_covered_day_cnt AS ep_calc_covered_day_cnt,
          a.ep_calc_hcpcs_charge_amt AS ep_calc_hcpcs_charge_amt,
          a.ep_calc_hcpcs_payment_amt AS ep_calc_hcpcs_payment_amt,
          a.ep_coinsurance_amt AS ep_coinsurance_amt,
          a.ep_contractual_adj_amt AS ep_contractual_adj_amt,
          a.ep_create_date AS ep_create_date,
          a.ep_deductible_amt AS ep_deductible_amt,
          a.ep_denial_amt AS ep_denial_amt,
          a.ep_discharge_cnt AS ep_discharge_cnt,
          a.ep_effective_post_date AS ep_effective_post_date,
          a.ep_lab_charge_amt AS ep_lab_charge_amt,
          a.ep_lab_payment_amt AS ep_lab_payment_amt,
          a.ep_non_covered_charge_amt AS ep_non_covered_charge_amt,
          a.ep_nonpayable_professional_fee AS ep_nonpayable_professional_fee_amt,
          a.ep_payor_batch_code AS ep_payor_batch_code,
          a.ep_plb_key_num AS ep_plb_key_num,
          a.ep_primary_insurance_payment AS ep_primary_insurance_payment_amt,
          a.ep_therapy_charge_amt AS ep_therapy_charge_amt,
          a.ep_therapy_payment_amt AS ep_therapy_payment_amt,
          a.era_create_date AS era_create_date,
          a.federal_specific_drg_amt AS federal_specific_drg_amt,
          a.indirect_teaching_amt AS indirect_teaching_amt,
          a.ins_covered_day_cnt AS ins_covered_day_cnt,
          a.interchange_receiver_code AS interchange_receiver_code,
          a.interchange_receiver_id AS interchange_receiver_id,
          a.interchange_recvr_id_qual_code AS interchange_receiver_id_qualifier_code,
          a.interchange_sender_code AS interchange_sender_code,
          a.interchange_sender_id AS interchange_sender_id,
          a.interchange_sender_qual_code AS interchange_sender_qualifier_code,
          a.interest_amt AS interest_amt,
          a.internal_denial_transctn_ind AS internal_denial_transaction_ind,
          a.lab_transaction_breakout_ind AS lab_transaction_breakout_ind,
          a.noncovered_day_cnt AS noncovered_day_cnt,
          a.outlier_amt AS outlier_amt,
          a.patient_liability_amt AS patient_liability_amt,
          a.patient_type_replacement_ind AS patient_type_replacement_ind,
          a.payment_guid AS payment_guid,
          a.payor_default_loggingtype_code AS payor_default_logging_type_code,
          a.payor_live_ind AS payor_live_ind,
          a.plb_recovery_transaction_ind AS plb_recovery_transaction_ind,
          a.pply_mother_baby_rule_ind AS apply_mother_baby_rule_ind,
          a.provider_level_adj_id AS provider_level_adj_id,
          a.rac_ind AS rac_ind,
          b.remittance_payee_sid AS remittance_payee_sid,
          c.remittance_payor_sid AS remittance_payor_sid,
          a.remittance_seq_num AS remittance_seq_num,
          'E' AS source_system_code,
          a.total_charges_replacement_ind AS total_charges_replacement_ind,
          a.transmission_receiving_code AS transmission_receiving_code,
          a.transmission_sending_code AS transmission_sending_code,
          a.unit_num AS unit_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment AS a
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_payee AS b ON upper(coalesce(a.payee_name, '')) = upper(coalesce(b.payee_name, ''))
   AND upper(coalesce(a.payee_identification_qual_code, '')) = upper(coalesce(b.payee_identification_qualifier_code, ''))
   AND upper(coalesce(a.payee_city_name, '')) = upper(coalesce(b.payee_city_name, ''))
   AND upper(coalesce(a.payee_state_code, '')) = upper(coalesce(b.payee_state_code, ''))
   AND upper(coalesce(a.payee_postal_zone_code, '')) = upper(coalesce(b.payee_postal_zone_code, ''))
   AND upper(coalesce(a.provider_tax_id_lookup_code, '')) = upper(coalesce(b.provider_tax_id_lookup_code, ''))
   AND CASE
           WHEN length(trim(format('%11d', a.provider_tax_id))) = 10 THEN trim(format('%11d', a.provider_tax_id))
           ELSE ''
       END = coalesce(b.provider_npi, '')
   AND CASE length(trim(format('%11d', a.provider_tax_id)))
           WHEN 9 THEN trim(format('%11d', a.provider_tax_id))
           WHEN 8 THEN concat('0', trim(format('%11d', a.provider_tax_id)))
           ELSE ''
       END = coalesce(b.provider_tax_id, '')
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_payor AS c ON upper(coalesce(a.payment_carrier_num, '')) = upper(coalesce(c.payment_carrier_num, ''))
   AND upper(coalesce(a.payment_agency_num, '')) = upper(coalesce(c.payment_agency_num_an, ''))
   AND upper(coalesce(a.payor_ref_id, '')) = upper(coalesce(c.payor_ref_id, ''))
   AND upper(coalesce(a.payor_name, '')) = upper(coalesce(c.payor_name, ''))
   AND upper(coalesce(a.payor_address_line_1, '')) = upper(coalesce(c.payor_address_line_1, ''))
   AND upper(coalesce(a.payor_address_line_2, '')) = upper(coalesce(c.payor_address_line_2, ''))
   AND upper(coalesce(a.payor_city_name, '')) = upper(coalesce(c.payor_city_name, ''))
   AND upper(coalesce(a.payor_state_code, '')) = upper(coalesce(c.payor_state_code, ''))
   AND upper(coalesce(a.payor_postal_zone_code, '')) = upper(coalesce(c.payor_postal_zone_code, ''))
   AND upper(coalesce(a.payor_line_of_business, '')) = upper(coalesce(c.payor_line_of_business, ''))
   AND upper(coalesce(a.payor_alternate_ref_id, '')) = upper(coalesce(c.payor_alternate_ref_id, ''))
   AND upper(coalesce(a.payor_long_name, '')) = upper(coalesce(c.payor_long_name, ''))
   AND upper(coalesce(a.payor_short_name, '')) = upper(coalesce(c.payor_short_name, ''))
   WHERE DATE(a.dw_last_update_date_time) =
       (SELECT max(DATE(remittance_payment.dw_last_update_date_time))
        FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment) ) AS a 