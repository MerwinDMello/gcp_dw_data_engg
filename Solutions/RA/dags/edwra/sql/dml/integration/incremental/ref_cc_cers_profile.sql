DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_cers_profile.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Sean Wilson
          Date: 10/14/2014
          Name: Ref_CC_CERS_Profile.sql
	Mod1:  Modified select to join with new {{ params.param_parallon_ra_stage_dataset_name }}.cers_profile_establishment table
	      to explode out to coids that are authentically junctioned with profiles for Contract
		  Modeling reports  -  JAC -  08/20/2015
		saving old join here
		--FROM EDWRA_STAGING.Cers_Profile CERP
		--JOIN EDWRA.Ref_CC_Org_Structure RCOS ON
		--RCOS.Schema_Id = CERP.Schema_Id;
	Mod2: Add purge statement at the end to clean-up deleted rows in source.  -  09/17/2015  jac
	Mod3: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod4: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
*************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA257;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile AS x USING
  (SELECT DISTINCT o.company_code,
                   o.coid,
                   cerp.id AS cers_profile_id,
                   trim(cerp.name) AS cers_profile_name,
                   cerp.date_created AS cers_profile_create_date,
                   cerp.date_updated AS cers_profile_update_date,
                   cerp.updated_by AS cers_profile_update_user_nm,
                   CASE
                       WHEN cerp.is_model = 1 THEN 'Y'
                       ELSE 'N'
                   END AS cers_model_ind,
                   cerp.ce_rs_category_id AS cers_category_id,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cers_profile AS cerp
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_profile_establishment AS cpe ON cpe.cers_profile_id = cerp.id
   AND cpe.schema_id = cerp.schema_id
   INNER JOIN
     (SELECT seo.schema_id,
             seo.establishment_id,
             se.level_no,
             seo.org_id,
             se.id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment_org AS seo
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment AS se ON se.id = seo.establishment_id
      AND seo.schema_id = se.schema_id
      AND (se.level_no = 9
           OR se.level_no = 10)) AS estab ON estab.establishment_id = cpe.sec_establishment_id
   AND estab.schema_id = cpe.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS o ON estab.org_id = o.org_id
   AND estab.schema_id = o.schema_id) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.cers_profile_id = z.cers_profile_id WHEN MATCHED THEN
UPDATE
SET cers_profile_name = substr(z.cers_profile_name, 1, 100),
    cers_profile_create_date = z.cers_profile_create_date,
    cers_profile_update_date = z.cers_profile_update_date,
    cers_profile_update_user_nm = substr(z.cers_profile_update_user_nm, 1, 100),
    cers_category_id = ROUND(z.cers_category_id, 0, 'ROUND_HALF_EVEN'),
    cers_model_ind = substr(z.cers_model_ind, 1, 1),
    source_system_code = substr(z.source_system_code, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cers_profile_id,
        cers_profile_name,
        cers_profile_create_date,
        cers_profile_update_date,
        cers_profile_update_user_nm,
        cers_category_id,
        cers_model_ind,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, z.cers_profile_id, substr(z.cers_profile_name, 1, 100), z.cers_profile_create_date, z.cers_profile_update_date, substr(z.cers_profile_update_user_nm, 1, 100), ROUND(z.cers_category_id, 0, 'ROUND_HALF_EVEN'), substr(z.cers_model_ind, 1, 1), substr(z.source_system_code, 1, 1), z.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cers_profile_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile
      GROUP BY company_code,
               coid,
               cers_profile_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile
WHERE ref_cc_cers_profile.dw_last_update_date_time <>
    (SELECT max(ref_cc_cers_profile_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile AS ref_cc_cers_profile_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_CERS_Profile');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;