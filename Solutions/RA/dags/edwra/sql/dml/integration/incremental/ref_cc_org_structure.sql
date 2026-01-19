DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_org_structure.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*******************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Org_Structure_INSERT - BTEQ Script.
      Mod1: Creation of script on 7/22/2011. SW.
      Mod2: Added rename logic on 7/27/2011. SW.
      Mod3: Enhancement to add SSC information on 7/17/2014 SW.
      Mod4: Added logic to handle error condition on 11/4/2014 SW.
      Mod5: Enhancement to SQL for matching the current active secuity
            establishment record (SSC) on 11/18/2014 SW.
	  Mod6: Changed logic because we are selecting all 'Active' orgs
    	    so the Active_Ind must be Y. The last step looks at old
            rows that have not been updated indicating an inactive
            record which updates them to Org_Id = 0,Active_Ind = 'N',
			Org_Status = 'deleted'. 1/24/2018 SW.
Mod6:  -  Added Job name to the query band statement 04062021
*******************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA215;');
 BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS x USING
  (SELECT ff.company_code,
          trim(substr(og.client_id, 7, 5)) AS coid,
          og.short_name AS unit_num,
          rcsm.schema_id AS schema_id,
          og.org_id AS org_id,
          og.customer_org_id AS customer_id,
          'Y' AS active_ind,
          rcsm.schema_name AS SCHEMA_NAME,
          cust.name AS customer_name,
          se.establishment_name AS ssc_name,
          trim(substr(og.name, 8, 100)) AS facility_name,
          og.org_status AS org_status,
          rcsm.create_date_time AS create_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment AS se
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment_org AS seo ON seo.establishment_id = se.id
   AND seo.schema_id = se.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON seo.org_id = og.org_id
   AND seo.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.customer AS cust ON cust.schema_id = og.schema_id
   INNER JOIN  {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ff ON upper(rtrim(ff.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS rcsm ON rcsm.schema_id = og.schema_id
   WHERE se.level_no = 9
     AND seo.eff_dt_begin <= current_date('US/Central')
     AND seo.eff_dt_end >= current_date('US/Central') ) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid)) WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num, 1, 5),
    schema_id = CAST(z.schema_id AS INT64),
    org_id = z.org_id,
    active_ind = substr(z.active_ind, 1, 1),
    customer_name = substr(z.customer_name, 1, 50),
    ssc_name = substr(z.ssc_name, 1, 50),
    facility_name = substr(z.facility_name, 1, 50),
    org_status = z.org_status,
    create_date_time = z.create_date_time,
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        unit_num,
        schema_id,
        org_id,
        customer_id,
        active_ind,
        SCHEMA_NAME,
        customer_name,
        ssc_name,
        facility_name,
        org_status,
        create_date_time,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, substr(z.coid, 1, 5), substr(z.unit_num, 1, 5), CAST(z.schema_id AS INT64), z.org_id, ROUND(z.customer_id, 0, 'ROUND_HALF_EVEN'), substr(z.active_ind, 1, 1), z.schema_name, substr(z.customer_name, 1, 50), substr(z.ssc_name, 1, 50), substr(z.facility_name, 1, 50), z.org_status, z.create_date_time, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure
      GROUP BY company_code,
               coid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure');

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


UPDATE {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure
SET active_ind = 'N',
    org_status = 'deleted'
WHERE DATE(ref_cc_org_structure.dw_last_update_date_time) <> current_date('US/Central');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;