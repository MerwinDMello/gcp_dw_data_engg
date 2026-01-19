DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_facility_iplan.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: CC_Facility_Iplan BTEQ Script.
      Mod1: Creation of script on 8/2/2011. SW.
      Mod2: CR152 - ICD10 - Add new column ICD10_Conversion_Date -  09/30/2015  jac
      Mod3: Added new columns in support for payer calc mapping exception on
            5/19/2016. SW
	  Mod4: Changed delete to only consider active coids on 1/30/2018 SW.
	Mod5:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod9:  -  PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA220;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- SET QUERY_BAND = 'App=RA_ETL;' FOR SESSION;
 -- Locking EDWPF_STAGING.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan AS x USING
  (SELECT po.payor_dw_id,
          po.company_code,
          CASE
              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
              ELSE og.client_id
          END AS coido,
          og.short_name AS unit_num,
          mp.id AS payer_id,
          mp.name AS payer_name,
          mp.fin_class_id AS financial_class_code,
          CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mp.code, 1, 3), substr(mp.code, 5, 2))) AS INT64) AS iplan,
          mp.misc_yn01 AS payer_part_b_ind,
          mp.model_covered_population AS model_covered_population_text,
          mp.model_product_class AS model_product_class_text,
          CAST(mp.date_created AS DATETIME) AS create_date_time,
          CAST(mp.date_updated AS DATETIME) AS update_date_time,
          mp.icd10_conversion_date,
          mp.inpatient_provider_no AS ip_provider_num,
          mp.outpatient_provider_no AS op_provider_num
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mp
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS rcsm ON mp.schema_id = rcsm.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON mp.org_id = og.org_id
   AND mp.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(CASE
                                                                                                                          WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                          ELSE og.client_id
                                                                                                                      END))
   AND po.iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mp.code, 1, 3), substr(mp.code, 5, 2))) AS INT64)
   WHERE upper(rtrim(mp.code)) NOT IN('NO INS',
                                      '000-00') ) AS z ON x.payor_dw_id = z.payor_dw_id WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    coid = substr(z.coido, 1, 5),
    unit_num = substr(z.unit_num, 1, 5),
    payer_id = CAST(ROUND(z.payer_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC),
    payer_name = z.payer_name,
    financial_class_code = ROUND(z.financial_class_code, 0, 'ROUND_HALF_EVEN'),
    iplan_id = z.iplan,
    payer_part_b_ind = substr(z.payer_part_b_ind, 1, 1),
    model_covered_population_text = z.model_covered_population_text,
    model_product_class_text = z.model_product_class_text,
    create_date_time = z.create_date_time,
    update_date_time = z.update_date_time,
    icd10_conversion_date = z.icd10_conversion_date,
    ip_provider_num = z.ip_provider_num,
    op_provider_num = z.op_provider_num,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (payor_dw_id,
        company_code,
        coid,
        unit_num,
        payer_id,
        payer_name,
        financial_class_code,
        iplan_id,
        payer_part_b_ind,
        model_covered_population_text,
        model_product_class_text,
        create_date_time,
        update_date_time,
        icd10_conversion_date,
        ip_provider_num,
        op_provider_num,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.payor_dw_id, z.company_code, substr(z.coido, 1, 5), substr(z.unit_num, 1, 5), CAST(ROUND(z.payer_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC), z.payer_name, ROUND(z.financial_class_code, 0, 'ROUND_HALF_EVEN'), z.iplan, substr(z.payer_part_b_ind, 1, 1), z.model_covered_population_text, z.model_product_class_text, z.create_date_time, z.update_date_time, z.icd10_conversion_date, z.ip_provider_num, z.op_provider_num, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT payor_dw_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan
      GROUP BY payor_dw_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan
WHERE upper(cc_facility_iplan.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_facility_iplan.dw_last_update_date_time <>
    (SELECT max(cc_facility_iplan_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan AS cc_facility_iplan_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Facility_Iplan');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;