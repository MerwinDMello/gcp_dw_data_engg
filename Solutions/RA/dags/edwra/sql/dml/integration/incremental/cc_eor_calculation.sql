DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_calculation.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: CC_EOR_Calculation - BTEQ Script.
      Mod1: Creation of script on 10/5/2011. SW.
      Mod2: Used Sum or Max for measures that might have multiple clac as survivors. 2/14/2014. AP.
      Mod3: Removed CAST on Patient Account number on 1/14/2015 AS.
      Mod4: Reformatted SQL and added Ref_CC_Org_Structure for optimization on 3/23/2015 SW.
      Mod5: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	  Mod6: CR152 - ICD10 - Adding new column ICD_Version_Desc -  09/30/2015  jac
	  Mod7: Changed delete to only consider active coids on 1/30/2018 SW.
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod9:  -  PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA139;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Locking EDWPF_STAGING.Payor_Organization For Access
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation AS x USING
  (SELECT company_cd AS company_cd,
          coido AS coido,
          a.patient_dw_id,
          po.payor_dw_id,
          clmax.payer_rank AS iplan_insurance_order_num,
          clmax.calc_date AS eor_log_date,
          concat('INS', trim(format('%11d', CAST(clmax.payer_rank AS INT64)))) AS log_id,
          row_number() OVER (PARTITION BY upper(company_cd),
                                          upper(coido),
                                          a.patient_dw_id,
                                          po.payor_dw_id,
                                          clmax.payer_rank,
                                          clmax.calc_date,
                                          upper(po.log_id),
                                          po.eff_from_date
                             ORDER BY clmax.calc_id) AS log_sequence_num,
                            CASE
                                WHEN clmax.calc_date IS NULL THEN DATE '1800-01-01'
                                ELSE DATE(clmax.calc_date)
                            END AS eff_from_date,
                            og.short_name AS unit_num,
                            clmax.account_no AS pat_acct_nbr,
                            CASE
                                WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                                ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                            END AS iplan,
                            mapyr.interest_calc_date,
                            mapyr.interest_rate,
                            mpcl.interest_days AS interest_days_num,
                            mapyr.interest_amt,
                            mapyr.interest_stop_date,
                            mapyr.first_denial_date,
                            clmax.length_of_stay AS length_of_stay_days_num,
                            clmax.length_of_service AS length_of_service_days_num,
                            clmax.billing_status AS billing_status_code,
                            CASE
                                WHEN mapyr.calc_lock = 1 THEN 'Y'
                                ELSE 'N'
                            END AS calc_lock_ind,
                            CASE
                                WHEN mapyr.calc_result = 1 THEN 'Y'
                                ELSE 'N'
                            END AS calc_success_ind,
                            CASE
                                WHEN mapyr.allow_contract_code_changes = 1 THEN 'Y'
                                ELSE 'N'
                            END AS allow_contract_code_change_ind,
                            CASE
                                WHEN mapyr.is_eligible = 1 THEN 'Y'
                                ELSE 'N'
                            END AS payer_eligible_ind,
                            CASE
                                WHEN mapyr.is_owner_overridable = 1 THEN 'Y'
                                ELSE 'N'
                            END AS owner_override_ind,
                            CASE
                                WHEN mapyr.is_reason_overridable = 1 THEN 'Y'
                                ELSE 'N'
                            END AS reason_override_ind,
                            CASE
                                WHEN mapyr.is_status_overridable = 1 THEN 'Y'
                                ELSE 'N'
                            END AS status_override_ind,
                            CASE
                                WHEN mpcl.is_deleted = 1 THEN 'N'
                                ELSE 'Y'
                            END AS active_ind,
                            CASE
                                WHEN mpcl.claim_id = -1 THEN 2666344
                                ELSE 2666345
                            END AS calc_base_id,
                            mapyr.cob_method_id,
                            clmax.cers_term_id,
                            mapyr.mon_status_id AS account_payer_status_id,
                            clmax.calc_id AS calc_id,
                            mapyr.apl_appeal_id AS appeal_id,
                            pvicd.display_text AS icd_version_desc
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr
   INNER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             mapcl.payer_rank,
             max(mapcl.account_no) AS account_no,
             max(mapcl.id) AS calc_id,
             sum(mapcl.length_of_stay) AS length_of_stay,
             sum(mapcl.length_of_service) AS length_of_service,
             max(mapcl.billing_status) AS billing_status,
             max(mapcl.cers_term_id) AS cers_term_id,
             max(mapcl.calculation_date) AS calc_date
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
      WHERE mapcl.is_survivor = 1
      GROUP BY 1,
               2,
               3,
               upper(mapcl.account_no)) AS clmax ON mapyr.id = clmax.mon_account_payer_id
   AND mapyr.schema_id = clmax.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mpcl ON clmax.calc_id = mpcl.id
   AND clmax.schema_id = mpcl.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pvicd ON pvicd.id = mpcl.icd_vrsn_id
   AND pvicd.schema_id = mpcl.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapyr.schema_id = mpyr.schema_id
   AND mapyr.mon_payer_id = mpyr.id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS ctrm ON mpcl.schema_id = ctrm.schema_id
   AND mpcl.cers_term_id = ctrm.id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapyr.schema_id = ma.schema_id
   AND mapyr.mon_account_id = ma.id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.schema_id = og.schema_id
   AND ma.org_id_provider = og.org_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.org_id = og.org_id
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(trim(po.coid)) = upper(trim(rccos.coid))
   AND po.iplan_id = CASE
                         WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                     END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(trim(a.coid)) = upper(trim(rccos.coid))
   AND upper(trim(a.company_code)) = upper(trim(rccos.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(clmax.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN upper(trim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                ELSE og.client_id
                            END ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ po.company_code ]) AS company_cd) AS z ON upper(trim(x.company_code)) = upper(trim(z.company_cd))
AND upper(trim(x.coid)) = upper(trim(z.coido))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = z.iplan_insurance_order_num
AND x.eor_log_date = DATE(z.eor_log_date)
AND trim(x.log_id) = trim(z.log_id)
AND x.log_sequence_num = z.log_sequence_num
AND x.eff_from_date = z.eff_from_date WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num, 1, 5),
    pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.pat_acct_nbr) AS NUMERIC), 0, 'ROUND_HALF_EVEN'),
    iplan_id = z.iplan,
    interest_calc_date = z.interest_calc_date,
    interest_rate = ROUND(z.interest_rate, 4, 'ROUND_HALF_EVEN'),
    interest_days_num = CAST(z.interest_days_num AS INT64),
    interest_amt = ROUND(z.interest_amt, 3, 'ROUND_HALF_EVEN'),
    interest_stop_date = z.interest_stop_date,
    first_denial_date = z.first_denial_date,
    length_of_stay_days_num = CAST(z.length_of_stay_days_num AS INT64),
    length_of_service_days_num = CAST(z.length_of_service_days_num AS INT64),
    billing_status_code = substr(z.billing_status_code, 1, 1),
    calc_lock_ind = substr(z.calc_lock_ind, 1, 1),
    calc_success_ind = substr(z.calc_success_ind, 1, 1),
    allow_contract_code_change_ind = substr(z.allow_contract_code_change_ind, 1, 1),
    payer_eligible_ind = substr(z.payer_eligible_ind, 1, 1),
    owner_override_ind = substr(z.owner_override_ind, 1, 1),
    reason_override_ind = substr(z.reason_override_ind, 1, 1),
    status_override_ind = substr(z.status_override_ind, 1, 1),
    active_ind = substr(z.active_ind, 1, 1),
    calc_base_id = CAST(z.calc_base_id AS NUMERIC),
    cob_method_id = z.cob_method_id,
    cers_term_id = CAST(ROUND(z.cers_term_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC),
    account_payer_status_id = z.account_payer_status_id,
    calc_id = ROUND(z.calc_id, 0, 'ROUND_HALF_EVEN'),
    appeal_id = ROUND(z.appeal_id, 0, 'ROUND_HALF_EVEN'),
    icd_version_desc = substr(z.icd_version_desc, 1, 15),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        eor_log_date,
        log_id,
        log_sequence_num,
        eff_from_date,
        unit_num,
        pat_acct_num,
        iplan_id,
        interest_calc_date,
        interest_rate,
        interest_days_num,
        interest_amt,
        interest_stop_date,
        first_denial_date,
        length_of_stay_days_num,
        length_of_service_days_num,
        billing_status_code,
        calc_lock_ind,
        calc_success_ind,
        allow_contract_code_change_ind,
        payer_eligible_ind,
        owner_override_ind,
        reason_override_ind,
        status_override_ind,
        active_ind,
        calc_base_id,
        cob_method_id,
        cers_term_id,
        account_payer_status_id,
        calc_id,
        appeal_id,
        icd_version_desc,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_cd, substr(z.coido, 1, 5), z.patient_dw_id, z.payor_dw_id, CAST(z.iplan_insurance_order_num AS INT64), DATE(z.eor_log_date), substr(z.log_id, 1, 4), z.log_sequence_num, z.eff_from_date, substr(z.unit_num, 1, 5), ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.pat_acct_nbr) AS NUMERIC), 0, 'ROUND_HALF_EVEN'), z.iplan, z.interest_calc_date, ROUND(z.interest_rate, 4, 'ROUND_HALF_EVEN'), CAST(z.interest_days_num AS INT64), ROUND(z.interest_amt, 3, 'ROUND_HALF_EVEN'), z.interest_stop_date, z.first_denial_date, CAST(z.length_of_stay_days_num AS INT64), CAST(z.length_of_service_days_num AS INT64), substr(z.billing_status_code, 1, 1), substr(z.calc_lock_ind, 1, 1), substr(z.calc_success_ind, 1, 1), substr(z.allow_contract_code_change_ind, 1, 1), substr(z.payer_eligible_ind, 1, 1), substr(z.owner_override_ind, 1, 1), substr(z.reason_override_ind, 1, 1), substr(z.status_override_ind, 1, 1), substr(z.active_ind, 1, 1), CAST(z.calc_base_id AS NUMERIC), z.cob_method_id, CAST(ROUND(z.cers_term_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC), z.account_payer_status_id, ROUND(z.calc_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.appeal_id, 0, 'ROUND_HALF_EVEN'), substr(z.icd_version_desc, 1, 15), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             eor_log_date,
             log_id,
             log_sequence_num,
             eff_from_date
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num,
               eff_from_date
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation
WHERE upper(cc_eor_calculation.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(trim(r.org_status)) = 'ACTIVE' )
  AND cc_eor_calculation.dw_last_update_date_time <>
    (SELECT max(cc_eor_calculation_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation AS cc_eor_calculation_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_EOR_Calculation');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;