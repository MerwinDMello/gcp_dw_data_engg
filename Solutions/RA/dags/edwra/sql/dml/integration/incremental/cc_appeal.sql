DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_appeal.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/********************************************************************************
   Developer: Sean Wilson
        Date: 2/16/2011
        Name: CC_Appeal.sql
        Mod1: Initial creation of BTEQ script on 2/1/2011.
        Mod2: Payer_Code data type changed from char to integer
              due to EDW rules. Script was changed on 5/6/2011.
        Mod3: Updated script for new Appeals model on 9/26/2011. SW.
        Mod4: Changed script to use LEFT OUTER JOIN to RACP on 11/18/2011 SW.
        Mod5: Added logic to only get Mon_Appeal_Sequence rows with sequence = 1
              on 12/8/2011 SW.
        Mod6: Added secondary join to Mon_Appeal due to duplicates on 12/28/2011 SW.
        Mod7: New join to derived table for omitting possible duplicate
              appeal rows noticed in production on 1/17/2013. SW.
        Mod8: Moved aggregation functions to sub queries on 9/18/2013 SW.
        Mod9: Removed CAST on Patient Account Number on 1/13/2015. AS.
        Mod10:More optimization added by using ref_cc_org_strucutre on 2/25/2015 SW.
        Mod11:Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
        Mod12:Added Appeal reopen columns to extract on 10/4/2016 SW.
		Mod13:Changed delete to only consider active coids on 1/30/2018 SW.
	Mod14:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
      Mod15:  -  PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
**********************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA229;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Locking EDWPF_STAGING.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal AS x USING
  (SELECT po.company_code AS company_cd,
          CASE
              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
              ELSE og.client_id
          END AS coido,
          a.patient_dw_id,
          po.payor_dw_id,
          mapl.payer_rank AS iplan_insurance_order_num,
          mapl.appeal_no AS appeal_num,
          og.short_name AS unit_num,
          ma.account_no AS pat_acct_nbr,
          CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64) AS iplan,
          maps.appealed_amt AS appeal_amt,
          CASE
              WHEN mapyr.first_denial_date IS NOT NULL
                   AND mplc.lifecycledate IS NOT NULL
                   AND mapyr.first_denial_date < mplc.lifecycledate THEN mapyr.first_denial_date
              ELSE coalesce(mapyr.first_denial_date, coalesce(mplc.lifecycledate, mapl.date_created))
          END AS appeal_origination_date,
          mapl.appeal_close_date,
          mapl.id AS appeal_id,
          usr.login_id AS appeal_create_user_id,
          mapl.date_created AS appeal_create_date_time,
          usr2.login_id AS appeal_update_user_id,
          mapl.date_modified AS appeal_update_date_time,
          CAST(coalesce(clmax.lenofstay, CAST(0 AS BIGNUMERIC)) AS INT64) - CAST(coalesce(racp.actcovdays, CAST(0 AS BIGNUMERIC)) AS INT64) AS denied_days_num,
          mapl.reopen_date AS appeal_reopen_date_time,
          mapl.reopen_user AS appeal_reopen_user_id
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal AS mapl
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence AS maps ON mapl.id = maps.mon_appeal_id
   AND mapl.schema_id = maps.schema_id
   AND maps.sequence_no = 1
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapl.mon_account_id = ma.id
   AND mapl.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON mapl.mon_account_id = mapyr.mon_account_id
   AND mapl.schema_id = mapyr.schema_id
   AND mapl.mon_payer_id = mapyr.mon_payer_id
   AND mapl.payer_rank = mapyr.payer_rank
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapyr.mon_payer_id = mpyr.id
   AND mapyr.schema_id = mpyr.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcl.length_of_stay, CAST(0 AS NUMERIC))) AS lenofstay,
             mapcl.payer_rank AS payer_rank,
             mapcl.mon_account_id,
             mapcl.schema_id,
             mapcl.mon_account_payer_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
      WHERE mapcl.is_survivor = 1
        AND mapcl.is_deleted = 0
      GROUP BY 2,
               3,
               4,
               5) AS clmax ON mapyr.id = clmax.mon_account_payer_id
   AND mapyr.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT min(maplc.lifecycle_date) AS lifecycledate,
             maplc.mon_account_payer_id,
             maplc.schema_id,
             maplc.lifecycle_event
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_lifecycle AS maplc
      WHERE maplc.lifecycle_event = 2666583
      GROUP BY 2,
               3,
               4) AS mplc ON clmax.mon_account_payer_id = mplc.mon_account_payer_id
   AND clmax.schema_id = mplc.schema_id
   LEFT OUTER JOIN
     (SELECT sum(rcp.actual_covered_days) AS actcovdays,
             rcp.mon_account_id,
             rcp.schema_id,
             rcp.mon_account_payer_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS rcp
      WHERE rcp.is_deleted = 0
      GROUP BY 2,
               3,
               4) AS racp ON racp.mon_account_id = clmax.mon_account_id
   AND racp.mon_account_payer_id = clmax.mon_account_payer_id
   AND racp.schema_id = clmax.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON mapl.schema_id = usr.schema_id
   AND mapl.user_id_created_by = usr.user_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON mapl.schema_id = usr2.schema_id
   AND mapl.user_id_modified_by = usr2.user_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(po.company_code)) = upper(rtrim(rccos.company_code))
   AND po.iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64)
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = ma.account_no
   WHERE upper(rtrim(mpyr.code)) NOT IN('NO INS',
                                        '000-00') ) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_cd))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coido))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = z.iplan_insurance_order_num
AND x.appeal_num = z.appeal_num WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num, 1, 5),
    pat_acct_num = z.pat_acct_nbr,
    iplan_id = z.iplan,
    appeal_amt = ROUND(z.appeal_amt, 3, 'ROUND_HALF_EVEN'),
    appeal_origination_date = z.appeal_origination_date,
    appeal_close_date = z.appeal_close_date,
    denied_days_num = z.denied_days_num,
    appeal_id = z.appeal_id,
    appeal_create_user_id = substr(z.appeal_create_user_id, 1, 20),
    appeal_create_date_time = CAST(z.appeal_create_date_time AS DATETIME),
    appeal_update_user_id = substr(z.appeal_update_user_id, 1, 20),
    appeal_update_date_time = CAST(z.appeal_update_date_time AS DATETIME),
    appeal_reopen_date_time = z.appeal_reopen_date_time,
    appeal_reopen_user_id = substr(z.appeal_reopen_user_id, 1, 20),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        appeal_num,
        unit_num,
        pat_acct_num,
        iplan_id,
        appeal_amt,
        appeal_origination_date,
        appeal_close_date,
        denied_days_num,
        appeal_id,
        appeal_create_user_id,
        appeal_create_date_time,
        appeal_update_user_id,
        appeal_update_date_time,
        appeal_reopen_date_time,
        appeal_reopen_user_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_cd, substr(z.coido, 1, 5), z.patient_dw_id, z.payor_dw_id, CAST(z.iplan_insurance_order_num AS INT64), ROUND(z.appeal_num, 0, 'ROUND_HALF_EVEN'), substr(z.unit_num, 1, 5), z.pat_acct_nbr, z.iplan, ROUND(z.appeal_amt, 3, 'ROUND_HALF_EVEN'), z.appeal_origination_date, z.appeal_close_date, z.denied_days_num, z.appeal_id, substr(z.appeal_create_user_id, 1, 20), CAST(z.appeal_create_date_time AS DATETIME), substr(z.appeal_update_user_id, 1, 20), CAST(z.appeal_update_date_time AS DATETIME), z.appeal_reopen_date_time, substr(z.appeal_reopen_user_id, 1, 20), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             appeal_num
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               appeal_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Appeal');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal AS a
WHERE (upper(a.company_code),
       upper(a.coid),
       a.patient_dw_id,
       a.appeal_id) IN
    (SELECT AS STRUCT upper(max(b.company_code)) AS company_code,
                      upper(max(b.coid)) AS coid,
                      b.patient_dw_id,
                      b.appeal_id
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal AS b
     INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r ON upper(rtrim(b.coid)) = upper(rtrim(r.coid))
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE'
     GROUP BY upper(b.company_code),
              upper(b.coid),
              3,
              4
     HAVING count(*) > 1)
  AND upper(rtrim(CAST(a.dw_last_update_date_time AS STRING))) <> upper(rtrim(
                                                                                (SELECT CAST(max(c.dw_last_update_date_time) AS STRING)
                                                                                 FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal AS c
                                                                                 INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS x ON upper(rtrim(c.coid)) = upper(rtrim(x.coid))
                                                                                 WHERE upper(rtrim(x.org_status)) = 'ACTIVE'
                                                                                   AND a.appeal_id = c.appeal_id )));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Appeal');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;