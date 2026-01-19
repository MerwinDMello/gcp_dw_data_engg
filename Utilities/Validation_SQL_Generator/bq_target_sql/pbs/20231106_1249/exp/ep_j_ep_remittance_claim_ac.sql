-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_remittance_claim_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          a.claim_guid AS claim_guid,
          ROUND(CASE
             coalesce(format('%#20.0f', fp.patient_dw_id), '999999')
            WHEN '' THEN NUMERIC '0'
            ELSE CAST(coalesce(format('%#20.0f', fp.patient_dw_id), '999999') as NUMERIC)
          END, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
          a.payment_guid AS payment_guid,
          a.audit_date AS audit_date,
          a.delete_ind AS delete_ind,
          coalesce(a.delete_date, DATE '1999-01-01') AS delete_date,
          a.provider_split_coid AS coid,
          a.provider_split_unit_num AS unit_num,
          a.company_code AS company_code,
          a.payor_patient_id AS payor_patient_id,
          CASE
            WHEN a.remit_account_number > 9.999999999E9 THEN NUMERIC '9999999999'
            ELSE a.remit_account_number
          END AS remit_account_number,
          a.patient_last_name AS patient_last_name,
          a.patient_first_name AS patient_first_name,
          a.patient_middle_initial AS patient_middle_initial,
          a.mpi_ind AS mpi_ind,
          a.iplan_id AS iplan_id,
          a.iplan_id AS ep_calc_iplan_id,
          a.insurance_order_num AS iplan_insurance_order_num,
          a.medical_record_num AS medical_record_num,
          a.payor_icn AS payer_claim_control_number,
          a.service_from_date AS stmt_cover_from_date,
          a.service_thru_date AS stmt_cover_to_date,
          parse_date('%Y/%m/%d', CASE
            WHEN syslib.length(a.received_date) > 0 THEN CASE
              WHEN syslib.length(bqutil.fn.cw_td_strtok(a.received_date, '/', 1)) = 1
               AND syslib.length(bqutil.fn.cw_td_strtok(a.received_date, '/', 2)) = 1 THEN concat(bqutil.fn.cw_td_strtok(a.received_date, '/', 3), '/0', bqutil.fn.cw_td_strtok(a.received_date, '/', 1), '/0', bqutil.fn.cw_td_strtok(a.received_date, '/', 2))
              WHEN syslib.length(bqutil.fn.cw_td_strtok(a.received_date, '/', 1)) = 1 THEN concat(bqutil.fn.cw_td_strtok(a.received_date, '/', 3), '/0', bqutil.fn.cw_td_strtok(a.received_date, '/', 1), '/', bqutil.fn.cw_td_strtok(a.received_date, '/', 2))
              WHEN syslib.length(bqutil.fn.cw_td_strtok(a.received_date, '/', 2)) = 1 THEN concat(bqutil.fn.cw_td_strtok(a.received_date, '/', 3), '/', bqutil.fn.cw_td_strtok(a.received_date, '/', 1), '/0', bqutil.fn.cw_td_strtok(a.received_date, '/', 2))
              ELSE concat(bqutil.fn.cw_td_strtok(a.received_date, '/', 3), '/', bqutil.fn.cw_td_strtok(a.received_date, '/', 1), '/', bqutil.fn.cw_td_strtok(a.received_date, '/', 2))
            END
            ELSE CAST(NULL as STRING)
          END) AS received_date,
          a.mpi_corrected_discharge_date AS mpi_corrected_discharge_date,
          a.mpi_calc_type_ind AS patient_type_code_pos1,
          a.financial_class_code AS financial_class_code,
          a.charge_amt AS total_charge_amt,
          a.mpi_calc_charge_amt AS mpi_calc_charge_amt,
          a.payment_amt AS payment_amt,
          a.ep_calc_denial_amount AS ep_calc_denial_amount,
          a.ep_calc_contractual_adj_amt AS ep_calc_contractual_adj_amt,
          a.mpi_contractual_adjust_amount AS mpi_contractual_adj_amt,
          a.mpi_calc_contractual_adj_amt AS corrected_contractual_adj_amt,
          a.net_benefit_amount AS net_benefit_amt,
          a.covered_charge_amt AS covered_charge_amt,
          a.mpi_calc_payor_prev_paymnt_amt AS mpi_calc_payor_previous_payment_amt,
          a.ep_calc_noncovered_charge_amt AS ep_calc_noncovered_charge_amt,
          a.mpi_calc_non_coverd_charge_amt AS mpi_calc_non_covered_charge_amt,
          a.ep_calc_deductible_amt AS ep_calc_deductible_amt,
          a.claim_mpi_deductible_amount AS mpi_calc_deductible_amt,
          a.ep_calc_blood_deductible_amt AS ep_calc_blood_deductible_amt,
          a.ep_calc_coinsurance_amt AS ep_calc_coinsurance_amt,
          a.claim_mpi_coinsurance_amount AS mpi_calc_coinsurance_amt,
          a.ep_calc_prim_payor_payment_amt AS ep_calc_primary_payor_payment_amt,
          a.patient_liability_amount AS patient_liability_amount,
          a.capital_amt AS capital_amt,
          a.ep_calc_discount_amt AS ep_calc_discount_amt,
          a.disproportionate_share_amt AS disproportionate_share_amt,
          a.drg_disproportionate_share_amt AS drg_disproportionate_share_amt,
          a.drg_amt AS drg_amt,
          a.drg_code AS drg_code,
          a.federal_specific_drg_amt AS federal_specific_drg_amt,
          a.hcpcs_charge_amt AS hcpcs_charge_amt,
          a.hcpcs_payment_amt AS hcpcs_payment_amt,
          a.indirect_medical_education_amt AS indirect_medical_education_amt,
          a.indirect_teaching_amt AS indirect_teaching_amt,
          a.interest_amt AS interest_amt,
          a.ep_calc_lab_charge_amt AS ep_calc_lab_charge_amt,
          a.ep_calc_lab_payment_amt AS ep_calc_lab_payment_amt,
          a.hospital_specific_drg_amt AS hospital_specific_drg_amt,
          a.mia_pps_oprtng_fed_spc_drg_amt AS pps_opr_federal_specific_drg_amt,
          a.mpi_calc_non_billed_charge_amt AS mpi_calc_non_billed_charge_amt,
          a.mpi_calc_net_benefit_amt AS mpi_calc_net_benefit_amt,
          a.ep_cal_non_payble_prof_fee_amt AS ep_calc_non_payable_professional_fee_amt,
          a.old_capital_amt AS old_capital_amt,
          a.operating_outlier_amt AS operating_outlier_amt,
          a.outlier_amt AS outlier_amt,
          a.outpatient_remibursemnt_rt_amt AS outpatient_reimibursement_rate_amt,
          a.ep_calc_therapy_charge_amt AS ep_calc_therapy_charge_amt,
          a.ep_calc_therapy_payment_amt AS ep_calc_therapy_payment_amt,
          a.pps_capital_outlier_amt AS pps_capital_outlier_amt,
          a.ins_covered_day_cnt AS ins_covered_day_cnt,
          a.ep_calc_covered_day_cnt AS ep_calc_covered_day_cnt,
          a.noncovered_day_cnt AS noncovered_day_cnt,
          a.cost_report_day_cnt AS cost_report_day_cnt,
          a.drg_weight AS drg_weight_amt,
          a.mpi_calc_drg_code AS mpi_calc_drg_code,
          a.mpi_calc_drg_grouper_code AS mpi_calc_drg_grouper_code,
          a.discharge_fraction AS discharge_fraction_pct,
          a.mia_lifetime_psych_day_cnt AS lifetime_psychiatric_day_cnt,
          a.prodr_code_contractual_adj_amt AS contractual_adj_amt_procedure_code,
          a.procedure_code_payment_amt AS payment_amt_procedure_code,
          a.logging_flag AS logging_flag,
          a.logging_type AS logging_type_code,
          a.mpi_calc_interim_bill_ind AS mpi_calc_interim_bill_ind,
          a.claim_status_code AS claim_status_code,
          a.ep_calc_status_code AS ep_calc_status_code,
          a.ep_calc_denial_code_ind AS ep_calc_denial_code_ind,
          a.mpi_calc_denial_code_ind AS mpi_calc_denial_code_ind,
          a.claim_filing_indicator_code AS type_of_claim_code,
          a.bill_type_code AS type_of_bill_code,
          a.ep_calc_balanced_ind AS ep_calc_balanced_ind,
          a.discount_applied_ind AS discount_applied_ind,
          a.ep_cal_plb_ovpym_recov_trx_ind AS ep_calc_plb_opay_rcvy_trx_ind,
          a.ep_calc_iz_denial_code AS ep_calc_internal_denial_code,
          a.concuity_acct_ind AS concuity_acct_ind,
          a.mia_switch_ind AS mia_switch_ind,
          a.moa_switch_ind AS moa_switch_ind,
          a.mia_pps_capital_exception_amt AS mia_pps_capital_exception_amt,
          a.casemix_ind AS casemix_ind,
          a.ep_calc_denial_code1 AS ep_calc_denial_code_1,
          a.ep_calc_denial_code2 AS ep_calc_denial_code_2,
          a.ep_calc_denial_code3 AS ep_calc_denial_code_3,
          a.ep_calc_denial_code4 AS ep_calc_denial_code_4,
          a.ep_calc_denial_code5 AS ep_calc_denial_code_5,
          a.ep_calc_denial_code6 AS ep_calc_denial_code_6,
          a.ep_calc_denial_code7 AS ep_calc_denial_code_7,
          a.ep_calc_denial_code8 AS ep_calc_denial_code_8,
          a.ep_calc_denial_code9 AS ep_calc_denial_code_9,
          a.ep_calc_denial_code10 AS ep_calc_denial_code_10,
          a.ep_calc_handling_ind AS ep_calc_handling_ind,
          a.crossover_payor_name AS crossover_payor_name,
          a.ep_calc_payor_category_code AS ep_calc_payor_category_code,
          a.secondary_payor_ind AS secondary_payor_flag,
          a.claim_mpi_primary_iplan_payer AS ep_calc_prim_itnl_pyr_code,
          a.mpi_secondary_iplan_payer AS ep_calc_secn_itnl_pyr_code,
          a.claim_mpi_tertiary_iplan_payer AS ep_calc_tert_itnl_pyr_code,
          f.corrected_priority_payor_sid AS corrected_priority_payor_sid,
          e.cob_carrier_sid AS cob_carrier_sid,
          a.corrected_subscriber_last_name AS corrected_subscriber_last_name,
          a.corrected_subscriber_frst_name AS corrected_subscriber_first_name,
          a.corrected_subscriber_mid_initl AS corrected_subscriber_middle_name,
          a.correctd_sbscbr_health_ins_num AS corrected_subscriber_health_ins_num,
          b.remittance_subscriber_sid AS remittance_subscriber_sid,
          c.remittance_oth_subscriber_sid AS remittance_oth_subscriber_sid,
          d.remittance_rendering_provider_sid AS remittance_rendering_provider_sid,
          a.grp_2100_qualifier1_code AS supplemental_amt_qlfr_code_1,
          a.grp_2100_supplementl_info1_amt AS supplemental_amt_1,
          a.grp_2100_qualifier2_code AS supplemental_amt_qlfr_code_2,
          a.grp_2100_supplementl_info2_amt AS supplemental_amt_2,
          a.grp_2100_qualifier3_code AS supplemental_amt_qlfr_code_3,
          a.grp_2100_supplementl_info3_amt AS supplemental_amt_3,
          a.grp_2100_qualifier4_code AS supplemental_amt_qlfr_code_4,
          a.grp_2100_supplementl_info4_amt AS supplemental_amt_4,
          a.grp_2100_qualifier5_code AS supplemental_amt_qlfr_code_5,
          a.grp_2100_supplementl_info5_amt AS supplemental_amt_5,
          a.grp_2100_qualifier6_code AS supplemental_amt_qlfr_code_6,
          a.grp_2100_supplementl_info6_amt AS supplemental_amt_6,
          a.grp_2100_qualifier7_code AS supplemental_amt_qlfr_code_7,
          a.grp_2100_supplementl_info7_amt AS supplemental_amt_7,
          a.grp_2100_qualifier8_code AS supplemental_amt_qlfr_code_8,
          a.grp_2100_supplementl_info8_amt AS supplemental_amt_8,
          a.grp_2100_qualifier9_code AS supplemental_amt_qlfr_code_9,
          a.grp_2100_supplementl_info9_amt AS supplemental_amt_9,
          a.grp_2100_qualifier10_code AS supplemental_amt_qlfr_code_10,
          a.grp_2100_supplemntl_info10_amt AS supplemental_amt_10,
          a.grp_2100_qualifier11_code AS supplemental_amt_qlfr_code_11,
          a.grp_2100_supplemntl_info11_amt AS supplemental_amt_11,
          a.grp_2100_qualifier12_code AS supplemental_amt_qlfr_code_12,
          a.grp_2100_supplemntl_info12_amt AS supplemental_amt_12,
          a.grp_2100_qualifier13_code AS supplemental_amt_qlfr_code_13,
          a.grp_2100_supplemntl_info13_amt AS supplemental_amt_13,
          a.quantity_qualifier1_code AS supplemental_qty_qlfr_code_1,
          a.supplemental_infrmtn1_quantity AS supplemental_qty_1,
          a.quantity_qualifier2_code AS supplemental_qty_qlfr_code_2,
          a.supplemental_infrmtn2_quantity AS supplemental_qty_2,
          a.quantity_qualifier3_code AS supplemental_qty_qlfr_code_3,
          a.supplemental_infrmtn3_quantity AS supplemental_qty_3,
          a.quantity_qualifier4_code AS supplemental_qty_qlfr_code_4,
          a.supplemental_infrmtn4_quantity AS supplemental_qty_4,
          a.quantity_qualifier5_code AS supplemental_qty_qlfr_code_5,
          a.supplemental_infrmtn5_quantity AS supplemental_qty_5,
          a.quantity_qualifier6_code AS supplemental_qty_qlfr_code_6,
          a.supplemental_infrmtn6_quantity AS supplemental_qty_6,
          a.quantity_qualifier7_code AS supplemental_qty_qlfr_code_7,
          a.supplemental_infrmtn7_quantity AS supplemental_qty_7,
          a.quantity_qualifier8_code AS supplemental_qty_qlfr_code_8,
          a.supplemental_infrmtn8_quantity AS supplemental_qty_8,
          a.quantity_qualifier9_code AS supplemental_qty_qlfr_code_9,
          a.supplemental_infrmtn9_quantity AS supplemental_qty_9,
          a.quantity_qualifier10_code AS supplemental_qty_qlfr_code_10,
          a.supplementl_infrmtn10_quantity AS supplemental_qty_10,
          a.quantity_qualifier11_code AS supplemental_qty_qlfr_code_11,
          a.supplementl_infrmtn11_quantity AS supplemental_qty_11,
          a.quantity_qualifier12_code AS supplemental_qty_qlfr_code_12,
          a.supplementl_infrmtn12_quantity AS supplemental_qty_12,
          a.quantity_qualifier13_code AS supplemental_qty_qlfr_code_13,
          a.supplementl_infrmtn13_quantity AS supplemental_qty_13,
          a.quantity_qualifier14_code AS supplemental_qty_qlfr_code_14,
          a.supplementl_infrmtn14_quantity AS supplemental_qty_14,
          a.rarc_code1 AS rarc_code1,
          a.rarc_code2 AS rarc_code2,
          a.rarc_code3 AS rarc_code3,
          a.rarc_code4 AS rarc_code4,
          a.rarc_code5 AS rarc_code5,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim AS a
          LEFT OUTER JOIN -- left join Edwpf_staging.Clinical_AcctKeys CA
          -- ON CA.pat_acct_num = A.Remit_Account_Number
          -- AND A.Provider_Split_COID=CA.coid
          -- and A.Provider_Split_Unit_num =CA.Unit_num
          (
            SELECT
                fact_patient.*
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient
              WHERE upper(fact_patient.source_system_code) = 'P'
          ) AS fp ON fp.pat_acct_num = a.remit_account_number
           AND upper(a.provider_split_coid) = upper(CASE
            WHEN upper(fp.coid) = '08158'
             AND CASE
               fp.sub_unit_num
              WHEN '' THEN 0.0
              ELSE CAST(fp.sub_unit_num as FLOAT64)
            END = 5 THEN '8165'
            WHEN upper(fp.coid) = '34224'
             AND CASE
               fp.sub_unit_num
              WHEN '' THEN 0.0
              ELSE CAST(fp.sub_unit_num as FLOAT64)
            END = 2 THEN '34241'
            ELSE fp.coid
          END)
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_subscriber_info AS b ON upper(coalesce(b.patient_health_ins_num, '')) = upper(coalesce(a.ins_subscriber_id, ''))
           AND upper(coalesce(b.insured_identification_qualifier_code, '')) = upper(coalesce(a.insurd_identifictn_qualifr_cod, ''))
           AND upper(coalesce(b.subscriber_id, '')) = upper(coalesce(a.subscriber_id, ''))
           AND upper(coalesce(b.insured_entity_type_qualifier_code, '')) = upper(coalesce(a.insured_entity_typ_qualifr_cod, ''))
           AND upper(coalesce(b.subscriber_last_name, '')) = upper(coalesce(a.subscriber_last_name, ''))
           AND upper(coalesce(b.subscriber_first_name, '')) = upper(coalesce(a.subscriber_first_name, ''))
           AND upper(coalesce(b.subscriber_middle_name, '')) = upper(coalesce(a.subscriber_middle_name, ''))
           AND upper(coalesce(b.subscriber_name_suffix, '')) = upper(coalesce(a.subscriber_name_suffix, ''))
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_oth_subscriber_info AS c ON upper(coalesce(a.othr_subcbr_enty_typ_qulfr_cod, '')) = upper(coalesce(c.oth_subscriber_enty_type_qualifier_code, ''))
           AND upper(coalesce(a.other_subscriber_last_name, '')) = upper(coalesce(c.oth_subscriber_last_name, ''))
           AND upper(coalesce(a.other_subscriber_first_name, '')) = upper(coalesce(c.oth_subscriber_first_name, ''))
           AND upper(coalesce(a.other_subscriber_middle_name, '')) = upper(coalesce(c.oth_subscriber_middle_name, ''))
           AND upper(coalesce(a.other_subscriber_name_suffix, '')) = upper(coalesce(c.oth_subscriber_name_suffix, ''))
           AND upper(coalesce(a.othr_subcbr_idntfctn_qulfr_cod, '')) = upper(coalesce(c.oth_subscriber_id_qualifier_code, ''))
           AND upper(coalesce(a.other_subscriber_id, '')) = upper(coalesce(c.oth_subscriber_id, ''))
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_rendering_provider AS d ON upper(coalesce(d.serv_provider_enty_type_qualifier_code, '')) = upper(coalesce(a.srvce_prvdr_enty_typ_qulfr_cod, ''))
           AND upper(coalesce(d.rendering_provider_last_org_name, '')) = upper(coalesce(a.rendering_provider_last_org_nm, ''))
           AND upper(coalesce(d.rendering_provider_first_name, '')) = upper(coalesce(a.rendering_provider_first_name, ''))
           AND upper(coalesce(d.rendering_provider_middle_name, '')) = upper(coalesce(a.rendering_provider_middle_name, ''))
           AND upper(coalesce(d.rendering_provider_name_suffix, '')) = upper(coalesce(a.rendering_provider_name_suffix, ''))
           AND upper(coalesce(d.serv_provider_id_qualifier_code, '')) = upper(coalesce(a.srvce_prvdr_idntfctn_qulfr_cod, ''))
           AND upper(coalesce(d.rendering_provider_id, '')) = upper(coalesce(a.rendering_provider_id, ''))
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_cob_carrier AS e ON upper(coalesce(e.cob_qualifier_code, '')) = upper(coalesce(a.crossover_carrier_qualifr_code, ''))
           AND upper(coalesce(e.cob_carrier_num, '')) = upper(coalesce(a.cordintn_of_benefit_carrier_nm, ''))
           AND upper(coalesce(e.cob_carrier_name, '')) = upper(coalesce(a.coordintn_of_beneft_carrier_nm, ''))
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_corrected_priority_payor AS f ON upper(coalesce(f.corrected_priority_payor_qualifier_code, '')) = upper(coalesce(a.corretd_prior_payor_qulifr_cod, ''))
           AND upper(coalesce(f.corrected_priority_payor_id, '')) = upper(coalesce(a.corrected_priority_payor_num, ''))
           AND upper(coalesce(f.corrected_priority_payor_name, '')) = upper(coalesce(a.nm103_corretd_priority_payr_nm, ''))
        WHERE DATE(a.dw_last_update_date_time) = (
          SELECT
              max(DATE(remittance_claim.dw_last_update_date_time))
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
        )
    ) AS a
;
