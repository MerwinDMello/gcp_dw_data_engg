DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_clinicalacctkeys.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*******************************************************************************************
Neme........: CC_ClinicalAcctkeys
Developer...: Sean Wilson
Date Created: 04/15/2015
Description.: Populates a staging stable for patient_dw_ids that only pertain to Concuity.
              Used for optimization.
		Mod1: Removed where clause to get only active Coids. 2/6/2018 SW.
        Mod2: Added new section for Payor_DW_Id on 4/2/2018 SW.
        Mod3: Added filter for Payor_DW_Id PO.Source_System_Code='P' on 7/30/2018.
Mod4:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
MOd:5: Delete data from Calc _Old table. Keep only 60 days data 02232021
********************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA270;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS mt USING
  (SELECT DISTINCT cak.coid AS coid,
                   cak.company_code AS company_code,
                   cak.unit_num AS unit_num,
                   cak.patient_dw_id,
                   cak.pat_acct_num
   FROM {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS cak
   WHERE EXISTS
       (SELECT 1
        FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rcos
        WHERE upper(rtrim(cak.coid)) = upper(rtrim(rcos.coid)) ) ) AS ms ON upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1'))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1')) WHEN NOT MATCHED BY TARGET THEN
INSERT (coid,
        company_code,
        unit_num,
        patient_dw_id,
        pat_acct_num)
VALUES (ms.coid, ms.company_code, ms.unit_num, ms.patient_dw_id, ms.pat_acct_num);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys
      GROUP BY patient_dw_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('ra_edwra_staging','Clinical_Acctkeys');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.payor_organization;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.payor_organization AS mt USING
  (SELECT DISTINCT po.coid AS coid,
                   po.company_code AS company_code,
                   po.iplan_id,
                   po.payor_dw_id,
                   po.payor_organization_type_code AS payor_organization_type_code,
                   po.source_system_code AS source_system_code
   FROM  {{ params.param_auth_base_views_dataset_name }}.payor_organization AS po
   WHERE EXISTS
       (SELECT 1
        FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rcos
        WHERE upper(rtrim(po.coid)) = upper(rtrim(rcos.coid)) )
     AND upper(rtrim(po.source_system_code)) = 'P' ) AS ms ON upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1'))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (upper(coalesce(mt.payor_organization_type_code, '0')) = upper(coalesce(ms.payor_organization_type_code, '0'))
     AND upper(coalesce(mt.payor_organization_type_code, '1')) = upper(coalesce(ms.payor_organization_type_code, '1')))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (coid,
        company_code,
        iplan_id,
        payor_dw_id,
        payor_organization_type_code,
        source_system_code)
VALUES (ms.coid, ms.company_code, ms.iplan_id, ms.payor_dw_id, ms.payor_organization_type_code, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT payor_dw_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.payor_organization
      GROUP BY payor_dw_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.payor_organization');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('ra_edwra_staging','PAYOR_ORGANIZATION');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_apc_old
WHERE DATE(mon_account_payer_calc_apc_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_atp_old
WHERE DATE(mon_account_payer_calc_atp_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_fs_old
WHERE DATE(mon_account_payer_calc_fs_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_irf_old
WHERE DATE(mon_account_payer_calc_irf_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_old
WHERE DATE(mon_account_payer_calc_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service_old
WHERE DATE(mon_account_payer_calc_service_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_snf_old
WHERE DATE(mon_account_payer_calc_snf_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_sum_old
WHERE mon_account_payer_calc_sum_old.dw_last_update_date < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_svc_old
WHERE DATE(mon_account_payer_calc_svc_old.dw_last_update_date) < date_sub(current_date('US/Central'), interval 60 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;