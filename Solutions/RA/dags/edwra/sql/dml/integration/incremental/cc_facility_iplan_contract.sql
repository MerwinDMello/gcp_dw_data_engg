DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_facility_iplan_contract.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: CC_Facility_Iplan_Contract BTEQ Script.
      Mod1: Creation of script on 7/15/2016. SW.
	  Mod2: Changed delete to only consider active coids on 1/31/2018 SW.
	Mod3:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod4:  -  PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA289;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Locking EDWPF_STAGING.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan_contract AS x USING
  (SELECT po.company_code,
          substr(o.client_id, 7, 5) AS coid,
          po.payor_dw_id, --  part of the unique primary key.
 CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mp.code), 1, 3), substr(trim(mp.code), 5, 2))) AS INT64) AS iplan_id,
 mpcet.cers_profile_id,
 mpcet.effective_date_start AS contract_eff_start_date,
 mpcet.ip_op_cd AS patient_type_code,
 mpcet.effective_date_end AS contract_eff_end_date,
 mpcet.date_updated AS last_updated_date,
 mpcet.last_updated_by AS last_updated_by_uid
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mp
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer_contract_eff_term AS mpcet ON mp.id = mpcet.mon_payer_id
   AND mp.schema_id = mpcet.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS provider ON mp.org_id = provider.org_id
   AND mp.schema_id = provider.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON mpcet.last_updated_by = usr.user_id
   AND mpcet.schema_id = usr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment_org AS seo ON seo.org_id = provider.org_id
   AND seo.schema_id = provider.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment AS se ON se.id = seo.establishment_id
   AND se.schema_id = seo.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS o ON o.org_id = seo.org_id
   AND o.schema_id = seo.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(substr(o.client_id, 7, 5)))
   AND po.iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mp.code, 1, 3), substr(mp.code, 5, 2))) AS INT64)
   WHERE se.level_no = 9
     AND upper(rtrim(mp.code)) NOT IN('NO INS',
                                      '000-00') ) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.payor_dw_id = z.payor_dw_id
AND x.cers_profile_id = z.cers_profile_id
AND x.contract_effective_start_date = z.contract_eff_start_date
AND upper(rtrim(x.patient_type_code)) = upper(rtrim(z.patient_type_code)) WHEN MATCHED THEN
UPDATE
SET contract_effective_end_date = z.contract_eff_end_date,
    last_updated_date = z.last_updated_date,
    last_updated_by_uid = ROUND(z.last_updated_by_uid, 0, 'ROUND_HALF_EVEN'),
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        payor_dw_id,
        cers_profile_id,
        contract_effective_start_date,
        patient_type_code,
        contract_effective_end_date,
        last_updated_date,
        last_updated_by_uid,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, substr(z.coid, 1, 5), z.payor_dw_id, ROUND(z.cers_profile_id, 0, 'ROUND_HALF_EVEN'), z.contract_eff_start_date, substr(z.patient_type_code, 1, 1), z.contract_eff_end_date, z.last_updated_date, ROUND(z.last_updated_by_uid, 0, 'ROUND_HALF_EVEN'), 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             payor_dw_id,
             cers_profile_id,
             contract_effective_start_date,
             patient_type_code
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan_contract
      GROUP BY company_code,
               coid,
               payor_dw_id,
               cers_profile_id,
               contract_effective_start_date,
               patient_type_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan_contract');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan_contract
WHERE upper(cc_facility_iplan_contract.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_facility_iplan_contract.dw_last_update_date_time <>
    (SELECT max(cc_facility_iplan_contract_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan_contract AS cc_facility_iplan_contract_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Facility_Iplan_Contract');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;