DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_amount_stg_other_service.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Ashan
       Date: 02/03/2014
       Name: EOR_Amount_Stg_Other_Service.sql
	Mod1:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod2: Modified logic for populating columns Calc_IRF_Teaching_Adj,Calc_IRF_Wage_Adj,Calc_IRF_Lip_Adj,refer PBI 18374 and PBI 20158. 1/4/2019 PT
	Mod3:Modified logic to have on Mapcecl.Ce_Exclusion_Id  = Cexl.Id instead of   on Mapcecl.Ce_Service_Id = Cexl.Id 1/23/2019 AM
	Mod4: Added columns Srv_Hcd_Exp_Payment,Srv_Imp_Exp_Payment,Srv_Lab_Exp_Payment,Srv_Lab_Service_Charges,Srv_Ect_Exp_Payment,
	Srv_Mri_Ct_Amb_Exp_Payment and inorder to populate them added logic.Modified logic to use in Ce_Exclusion_Id instead of Ce_Service_Id for Excl part as per PBI19954 on 1/26/2019 PT
	MOd5: Added more strings for like any where ever we use CE_Exclusion table and for ECT usage AM 2/8/2019
*****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA147_Other_Service;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- diagnostic nohashjoin on for session;
 BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS mt USING
  (SELECT DISTINCT clmax.mon_account_payer_id AS mon_account_payer_id,
                   clmax.schema_id AS schema_id,
                   CAST(ROUND(coalesce(fs.calc_fs_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_fs_exp_payment,
                   CAST(ROUND(coalesce(coinfs.calc_coinfs_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_coinfs_exp_payment,
                   CAST(ROUND(coalesce(coinfs.calc_coinfs_coins_amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_coinfs_coins_amount,
                   CAST(ROUND(coalesce(excl.calc_excl_amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_excl_amount,
                   CAST(ROUND(coalesce(calc_irf.calc_irf_teaching_adj, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_irf_teaching_adj,
                   CAST(ROUND(coalesce(calc_irf.calc_irf_wage_adj, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_irf_wage_adj,
                   CAST(ROUND(coalesce(calc_irf.calc_irf_lip_adj, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_irf_lip_adj,
                   CAST(ROUND(coalesce(calc_irf.calc_irf_outlier_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_irf_outlier_payment,
                   CAST(ROUND(coalesce(tri_drg.tri_drg_payment, CAST(0 AS BIGNUMERIC)), 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_drg_payment,
                   CAST(ROUND(coalesce(tri_drg.tri_drg_idme_payment, CAST(0 AS BIGNUMERIC)), 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_drg_idme_payment,
                   CAST(ROUND(coalesce(tri_drg.tri_drg_short_stay_outlier_amt, CAST(0 AS BIGNUMERIC)), 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_drg_short_stay_outlier_amt,
                   CAST(ROUND(coalesce(tri_drg.tri_drg_transfer_rate, CAST(0 AS BIGNUMERIC)), 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_drg_transfer_rate,
                   CAST(ROUND(coalesce(tri_drg.tri_drg_stnd_xfr_cost_outlier, CAST(0 AS BIGNUMERIC)) + coalesce(tri_drg.tri_drg_spec_xfr_cost_outlier, CAST(0 AS BIGNUMERIC)) + coalesce(tri_drg.tri_drg_stnd_cost_outlier, CAST(0 AS BIGNUMERIC)) + coalesce(tri_drg.tri_drg_neonate_cost_outlier, CAST(0 AS BIGNUMERIC)) + coalesce(tri_drg.tri_drg_short_stay_outlier_amt, CAST(0 AS BIGNUMERIC)), 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_drg_outlier_amt,
                   CAST(ROUND(coalesce(tri_er.tri_er_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_er_exp_payment,
                   CAST(ROUND(coalesce(tri_apc.tri_apc_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_apc_exp_payment,
                   CAST(ROUND(coalesce(tri_apc.tri_apc_outlier_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_apc_outlier_payment,
                   CAST(ROUND(coalesce(tri_comp.tri_comp_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS tri_comp_exp_payment,
                   CAST(ROUND(coalesce(hcd_exp.srv_hcd_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_hcd_exp_payment,
                   CAST(ROUND(coalesce(imp_exp.srv_imp_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_imp_exp_payment,
                   CAST(ROUND(coalesce(lab_exp.srv_lab_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_lab_exp_payment,
                   CAST(ROUND(coalesce(lab_svc.srv_lab_service_charges, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_lab_service_charges,
                   CAST(ROUND(coalesce(ect_exp.srv_ect_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_ect_exp_payment,
                   CAST(ROUND(coalesce(mri_ct_amb_exp.srv_mri_ct_amb_exp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_mri_ct_amb_exp_payment,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             max(mapcl.service_date_begin) AS service_date_begin
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
      GROUP BY 1,
               2) AS clmax
   LEFT OUTER JOIN --  Calc FS

     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcfs.expected_payment, CAST(0 AS NUMERIC))) AS calc_fs_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_fs AS mapcfs
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcfs.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcfs.schema_id = mapcl.schema_id
      GROUP BY 1,
               2) AS fs ON fs.mon_account_payer_id = clmax.mon_account_payer_id
   AND fs.schema_id = clmax.schema_id
   LEFT OUTER JOIN --  Coin FS

     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcoinfs.expected_payment, CAST(0 AS NUMERIC))) AS calc_coinfs_exp_payment,
             sum(coalesce(mapcoinfs.coins_amount, CAST(0 AS NUMERIC))) AS calc_coinfs_coins_amount
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_coin_fs AS mapcoinfs
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcoinfs.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcoinfs.schema_id = mapcl.schema_id
      GROUP BY 1,
               2) AS coinfs ON coinfs.mon_account_payer_id = clmax.mon_account_payer_id
   AND coinfs.schema_id = clmax.schema_id
   LEFT OUTER JOIN --  Excl

     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS calc_excl_amount
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'MRI%'
        OR upper(cexl.description) LIKE 'CT SCAN%'
        OR upper(cexl.description) LIKE '%CAT %'
        OR upper(cexl.description) LIKE 'CT %'
        OR upper(cexl.description) LIKE 'CT/%'
        OR upper(cexl.description) LIKE ' CT %'
        OR upper(cexl.description) LIKE '%/CT %'
        OR upper(cexl.description) LIKE '% CT'
        OR upper(cexl.description) LIKE '% CT %'
        OR upper(cexl.description) LIKE 'AMB%'
        OR upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE '%HCD'
        OR upper(cexl.description) LIKE '%HCD %'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE '%HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS'
        OR upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE '%IMPLANT'
        OR upper(cexl.description) LIKE '%IMPLANT%'
        OR upper(cexl.description) LIKE 'IMPLANTS%'
      GROUP BY 1,
               2) AS excl ON excl.mon_account_payer_id = clmax.mon_account_payer_id
   AND excl.schema_id = clmax.schema_id
   LEFT OUTER JOIN --  IRF

     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcirf.outlier_payment, CAST(0 AS NUMERIC))) AS calc_irf_outlier_payment,
             CASE
                 WHEN sum(coalesce(mapcirf.is_irf_transfer_calc, CAST(0 AS NUMERIC))) <> 0 THEN sum(coalesce(mapcirf.irf_xfer_teaching_status_adj, CAST(0 AS NUMERIC)))
                 ELSE sum(coalesce(mapcirf.irf_teaching_status_adjustment, CAST(0 AS NUMERIC)))
             END AS calc_irf_teaching_adj,
             CASE
                 WHEN sum(coalesce(mapcirf.is_irf_transfer_calc, NUMERIC '0')) <> 0 THEN sum(coalesce(mapcirf.irf_transfer_shortstay_amount, NUMERIC '0')) - sum(coalesce(mapcirf.irf_xfer_teaching_status_adj, NUMERIC '0')) - sum(coalesce(mapcirf.wage_xfer_rural_lip_adj_rate, NUMERIC '0')) + sum(coalesce(mapcl.acct_subterm_amt, NUMERIC '0'))
                 ELSE CASE
                          WHEN sum(coalesce(mapcirf.wage_rural_adj_rate, NUMERIC '0')) <> 0 THEN sum(coalesce(mapcirf.wage_rural_adj_rate, NUMERIC '0')) + sum(coalesce(mapcl.acct_subterm_amt, NUMERIC '0'))
                          ELSE sum(coalesce(mapcirf.wage_adj_rate, NUMERIC '0')) + sum(coalesce(mapcl.acct_subterm_amt, NUMERIC '0'))
                      END
             END AS calc_irf_wage_adj,
             CASE
                 WHEN sum(coalesce(mapcirf.is_irf_transfer_calc, CAST(0 AS NUMERIC))) <> 0 THEN sum(coalesce(mapcirf.wage_xfer_rural_lip_adj_rate, CAST(0 AS NUMERIC)))
                 ELSE sum(coalesce(mapcirf.wage_rural_lip_adj_rate, CAST(0 AS NUMERIC)))
             END AS calc_irf_lip_adj
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_irf AS mapcirf
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcirf.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcirf.schema_id = mapcl.schema_id
      GROUP BY 1,
               2) AS calc_irf ON calc_irf.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_irf.schema_id = clmax.schema_id
   LEFT OUTER JOIN --  Tricare

     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcdrg.drg_payment, CAST(0 AS NUMERIC))) AS tri_drg_payment,
             sum(coalesce(mapcdrg.idme_addon_payment, CAST(0 AS NUMERIC))) AS tri_drg_idme_payment,
             sum(coalesce(mapcdrg.short_stay_outlier_amount, CAST(0 AS NUMERIC))) AS tri_drg_short_stay_outlier_amt,
             sum(coalesce(mapcdrg.transfer_rate, CAST(0 AS NUMERIC))) AS tri_drg_transfer_rate,
             sum(coalesce(mapcdrg.standard_xfr_cost_outlier_pay, CAST(0 AS NUMERIC))) AS tri_drg_stnd_xfr_cost_outlier,
             sum(coalesce(mapcdrg.special_xfr_cost_outlier_pay, CAST(0 AS NUMERIC))) AS tri_drg_spec_xfr_cost_outlier,
             sum(coalesce(mapcdrg.standard_cost_outlier_pay, CAST(0 AS NUMERIC))) AS tri_drg_stnd_cost_outlier,
             sum(coalesce(mapcdrg.neonate_cost_outlier_pay, CAST(0 AS NUMERIC))) AS tri_drg_neonate_cost_outlier
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_drg AS mapcdrg
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcdrg.mon_acct_payer_summary_id = mapcl.id
      AND mapcdrg.schema_id = mapcl.schema_id
      GROUP BY 1,
               2) AS tri_drg ON tri_drg.mon_account_payer_id = clmax.mon_account_payer_id
   AND tri_drg.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcer.expected_payment, CAST(0 AS NUMERIC))) AS tri_er_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_er AS mapcer
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcer.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcer.schema_id = mapcl.schema_id
      GROUP BY 1,
               2) AS tri_er ON tri_er.mon_account_payer_id = clmax.mon_account_payer_id
   AND tri_er.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(triapc.expected_payment, CAST(0 AS NUMERIC))) AS tri_apc_exp_payment,
             sum(coalesce(triapc.tricare_apc_outlier_payment, CAST(0 AS NUMERIC))) AS tri_apc_outlier_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_apc AS triapc
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON triapc.mon_acct_payer_calc_summary_id = mapcl.id
      AND triapc.schema_id = mapcl.schema_id
      GROUP BY 1,
               2) AS tri_apc ON tri_apc.mon_account_payer_id = clmax.mon_account_payer_id
   AND tri_apc.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(apcomp.expected_payment, CAST(0 AS NUMERIC))) AS tri_comp_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tricomp AS apcomp
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON apcomp.mon_acct_payer_calc_summary_id = mapcl.id
      AND apcomp.schema_id = mapcl.schema_id
      GROUP BY 1,
               2) AS tri_comp ON tri_comp.mon_account_payer_id = clmax.mon_account_payer_id
   AND tri_comp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_hcd_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE 'HIGH COST PHARMACEUTICALS%'
        OR upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE '%HCD'
        OR upper(cexl.description) LIKE '%HCD %'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE '%HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS'
      GROUP BY 1,
               2) AS hcd_exp ON hcd_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND hcd_exp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_imp_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE '%IMPLANT'
        OR upper(cexl.description) LIKE '%IMPLANT%'
      GROUP BY 1,
               2) AS imp_exp ON imp_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND imp_exp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_lab_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE '%LABORATORY%'
        OR upper(cexl.description) LIKE 'LAB %'
        OR upper(cexl.description) LIKE '%LAB %'
        OR upper(cexl.description) LIKE 'LAB %'
        OR upper(cexl.description) LIKE '%LAB'
        OR upper(cexl.description) LIKE 'LABORATORY%'
        OR upper(cexl.description) LIKE '%LABORATORY%'
        OR upper(cexl.description) LIKE '%LABS%'
        OR upper(cexl.description) LIKE '%LAB/%'
        OR upper(cexl.description) LIKE 'LABS%'
      GROUP BY 1,
               2) AS lab_exp ON lab_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND lab_exp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcecl.charge_amount, CAST(0 AS NUMERIC))) AS srv_lab_service_charges
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE '%LABORATORY%'
        OR upper(cexl.description) LIKE 'LAB %'
        OR upper(cexl.description) LIKE '%LAB %'
        OR upper(cexl.description) LIKE 'LAB %'
        OR upper(cexl.description) LIKE '%LAB'
        OR upper(cexl.description) LIKE 'LABORATORY%'
        OR upper(cexl.description) LIKE '%LABORATORY%'
        OR upper(cexl.description) LIKE '%LABS%'
        OR upper(cexl.description) LIKE '%LAB/%'
        OR upper(cexl.description) LIKE 'LABS%'
      GROUP BY 1,
               2) AS lab_svc ON lab_svc.mon_account_payer_id = clmax.mon_account_payer_id
   AND lab_svc.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_ect_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'ELECTROCONVULSIVE%'
        OR upper(cexl.description) LIKE 'ECT%'
        OR upper(cexl.description) LIKE '%(ECT)%'
      GROUP BY 1,
               2) AS ect_exp ON ect_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND ect_exp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_mri_ct_amb_exp_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'AMB%'
        OR upper(cexl.description) LIKE 'MRI%'
        OR upper(cexl.description) LIKE 'CAT%'
        OR upper(cexl.description) LIKE 'CT%'
        OR upper(cexl.description) LIKE '%CAT %'
        OR upper(cexl.description) LIKE 'CT %'
        OR upper(cexl.description) LIKE 'CT/%'
        OR upper(cexl.description) LIKE ' CT %'
        OR upper(cexl.description) LIKE '%/CT %'
        OR upper(cexl.description) LIKE '% CT'
        OR upper(cexl.description) LIKE '% CT %'
      GROUP BY 1,
               2) AS mri_ct_amb_exp ON mri_ct_amb_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND mri_ct_amb_exp.schema_id = clmax.schema_id
   WHERE clmax.service_date_begin < current_date('US/Central') ) AS ms ON coalesce(mt.mon_account_payer_id, NUMERIC '0') = coalesce(ms.mon_account_payer_id, NUMERIC '0')
AND coalesce(mt.mon_account_payer_id, NUMERIC '1') = coalesce(ms.mon_account_payer_id, NUMERIC '1')
AND (coalesce(mt.schema_id, NUMERIC '0') = coalesce(ms.schema_id, NUMERIC '0')
     AND coalesce(mt.schema_id, NUMERIC '1') = coalesce(ms.schema_id, NUMERIC '1'))
AND (coalesce(mt.calc_fs_exp_payment, NUMERIC '0') = coalesce(ms.calc_fs_exp_payment, NUMERIC '0')
     AND coalesce(mt.calc_fs_exp_payment, NUMERIC '1') = coalesce(ms.calc_fs_exp_payment, NUMERIC '1'))
AND (coalesce(mt.calc_coinfs_exp_payment, NUMERIC '0') = coalesce(ms.calc_coinfs_exp_payment, NUMERIC '0')
     AND coalesce(mt.calc_coinfs_exp_payment, NUMERIC '1') = coalesce(ms.calc_coinfs_exp_payment, NUMERIC '1'))
AND (coalesce(mt.calc_coinfs_coins_amount, NUMERIC '0') = coalesce(ms.calc_coinfs_coins_amount, NUMERIC '0')
     AND coalesce(mt.calc_coinfs_coins_amount, NUMERIC '1') = coalesce(ms.calc_coinfs_coins_amount, NUMERIC '1'))
AND (coalesce(mt.calc_excl_amount, NUMERIC '0') = coalesce(ms.calc_excl_amount, NUMERIC '0')
     AND coalesce(mt.calc_excl_amount, NUMERIC '1') = coalesce(ms.calc_excl_amount, NUMERIC '1'))
AND (coalesce(mt.calc_irf_teaching_adj, NUMERIC '0') = coalesce(ms.calc_irf_teaching_adj, NUMERIC '0')
     AND coalesce(mt.calc_irf_teaching_adj, NUMERIC '1') = coalesce(ms.calc_irf_teaching_adj, NUMERIC '1'))
AND (coalesce(mt.calc_irf_wage_adj, NUMERIC '0') = coalesce(ms.calc_irf_wage_adj, NUMERIC '0')
     AND coalesce(mt.calc_irf_wage_adj, NUMERIC '1') = coalesce(ms.calc_irf_wage_adj, NUMERIC '1'))
AND (coalesce(mt.calc_irf_lip_adj, NUMERIC '0') = coalesce(ms.calc_irf_lip_adj, NUMERIC '0')
     AND coalesce(mt.calc_irf_lip_adj, NUMERIC '1') = coalesce(ms.calc_irf_lip_adj, NUMERIC '1'))
AND (coalesce(mt.calc_irf_outlier_payment, NUMERIC '0') = coalesce(ms.calc_irf_outlier_payment, NUMERIC '0')
     AND coalesce(mt.calc_irf_outlier_payment, NUMERIC '1') = coalesce(ms.calc_irf_outlier_payment, NUMERIC '1'))
AND (coalesce(mt.tri_drg_payment, NUMERIC '0') = coalesce(ms.tri_drg_payment, NUMERIC '0')
     AND coalesce(mt.tri_drg_payment, NUMERIC '1') = coalesce(ms.tri_drg_payment, NUMERIC '1'))
AND (coalesce(mt.tri_drg_idme_payment, NUMERIC '0') = coalesce(ms.tri_drg_idme_payment, NUMERIC '0')
     AND coalesce(mt.tri_drg_idme_payment, NUMERIC '1') = coalesce(ms.tri_drg_idme_payment, NUMERIC '1'))
AND (coalesce(mt.tri_drg_short_stay_outlier_amt, NUMERIC '0') = coalesce(ms.tri_drg_short_stay_outlier_amt, NUMERIC '0')
     AND coalesce(mt.tri_drg_short_stay_outlier_amt, NUMERIC '1') = coalesce(ms.tri_drg_short_stay_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.tri_drg_transfer_rate, NUMERIC '0') = coalesce(ms.tri_drg_transfer_rate, NUMERIC '0')
     AND coalesce(mt.tri_drg_transfer_rate, NUMERIC '1') = coalesce(ms.tri_drg_transfer_rate, NUMERIC '1'))
AND (coalesce(mt.tri_drg_outlier_amt, NUMERIC '0') = coalesce(ms.tri_drg_outlier_amt, NUMERIC '0')
     AND coalesce(mt.tri_drg_outlier_amt, NUMERIC '1') = coalesce(ms.tri_drg_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.tri_er_exp_payment, NUMERIC '0') = coalesce(ms.tri_er_exp_payment, NUMERIC '0')
     AND coalesce(mt.tri_er_exp_payment, NUMERIC '1') = coalesce(ms.tri_er_exp_payment, NUMERIC '1'))
AND (coalesce(mt.tri_apc_exp_payment, NUMERIC '0') = coalesce(ms.tri_apc_exp_payment, NUMERIC '0')
     AND coalesce(mt.tri_apc_exp_payment, NUMERIC '1') = coalesce(ms.tri_apc_exp_payment, NUMERIC '1'))
AND (coalesce(mt.tri_apc_outlier_payment, NUMERIC '0') = coalesce(ms.tri_apc_outlier_payment, NUMERIC '0')
     AND coalesce(mt.tri_apc_outlier_payment, NUMERIC '1') = coalesce(ms.tri_apc_outlier_payment, NUMERIC '1'))
AND (coalesce(mt.tri_comp_exp_payment, NUMERIC '0') = coalesce(ms.tri_comp_exp_payment, NUMERIC '0')
     AND coalesce(mt.tri_comp_exp_payment, NUMERIC '1') = coalesce(ms.tri_comp_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_hcd_exp_payment, NUMERIC '0') = coalesce(ms.srv_hcd_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_hcd_exp_payment, NUMERIC '1') = coalesce(ms.srv_hcd_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_imp_exp_payment, NUMERIC '0') = coalesce(ms.srv_imp_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_imp_exp_payment, NUMERIC '1') = coalesce(ms.srv_imp_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_lab_exp_payment, NUMERIC '0') = coalesce(ms.srv_lab_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_lab_exp_payment, NUMERIC '1') = coalesce(ms.srv_lab_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_lab_service_charges, NUMERIC '0') = coalesce(ms.srv_lab_service_charges, NUMERIC '0')
     AND coalesce(mt.srv_lab_service_charges, NUMERIC '1') = coalesce(ms.srv_lab_service_charges, NUMERIC '1'))
AND (coalesce(mt.srv_ect_exp_payment, NUMERIC '0') = coalesce(ms.srv_ect_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_ect_exp_payment, NUMERIC '1') = coalesce(ms.srv_ect_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_mri_ct_amb_exp_payment, NUMERIC '0') = coalesce(ms.srv_mri_ct_amb_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_mri_ct_amb_exp_payment, NUMERIC '1') = coalesce(ms.srv_mri_ct_amb_exp_payment, NUMERIC '1'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (mon_account_payer_id,
        schema_id,
        calc_fs_exp_payment,
        calc_coinfs_exp_payment,
        calc_coinfs_coins_amount,
        calc_excl_amount,
        calc_irf_teaching_adj,
        calc_irf_wage_adj,
        calc_irf_lip_adj,
        calc_irf_outlier_payment,
        tri_drg_payment,
        tri_drg_idme_payment,
        tri_drg_short_stay_outlier_amt,
        tri_drg_transfer_rate,
        tri_drg_outlier_amt,
        tri_er_exp_payment,
        tri_apc_exp_payment,
        tri_apc_outlier_payment,
        tri_comp_exp_payment,
        srv_hcd_exp_payment,
        srv_imp_exp_payment,
        srv_lab_exp_payment,
        srv_lab_service_charges,
        srv_ect_exp_payment,
        srv_mri_ct_amb_exp_payment,
        dw_last_update_date_time)
VALUES (ms.mon_account_payer_id, ms.schema_id, ms.calc_fs_exp_payment, ms.calc_coinfs_exp_payment, ms.calc_coinfs_coins_amount, ms.calc_excl_amount, ms.calc_irf_teaching_adj, ms.calc_irf_wage_adj, ms.calc_irf_lip_adj, ms.calc_irf_outlier_payment, ms.tri_drg_payment, ms.tri_drg_idme_payment, ms.tri_drg_short_stay_outlier_amt, ms.tri_drg_transfer_rate, ms.tri_drg_outlier_amt, ms.tri_er_exp_payment, ms.tri_apc_exp_payment, ms.tri_apc_outlier_payment, ms.tri_comp_exp_payment, ms.srv_hcd_exp_payment, ms.srv_imp_exp_payment, ms.srv_lab_exp_payment, ms.srv_lab_service_charges, ms.srv_ect_exp_payment, ms.srv_mri_ct_amb_exp_payment, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT mon_account_payer_id,
             schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service
      GROUP BY mon_account_payer_id,
               schema_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.eor_amount_stg_other_service');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('ra_edwra_STAGING','Eor_Amount_Stg_Other_Service');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;