DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_vendor.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/******************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Vendor BTEQ Script.
      Mod1: Creation of script on 7/29/2016. SW.
      Mod2: Changed script to be an insert instead of merge on 10/31/2016 SW.
      Mod3: Added Company_Code and Coid columns plus new join on 11/7/2016 SW.
	Mod4:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
********************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA291;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_vendor AS x USING
  (SELECT max(ros.company_code) AS company_code,
          max(ros.coid) AS coid,
          max(ven.code) AS vendor_cd,
          max(ven.name) AS vendor_name,
          max(ven.description) AS vendor_desc,
          ven.eff_date AS eff_from_date,
          ven.expr_date AS eff_to_date,
          ven.creation_date AS vendor_creation_date_time,
          max(ven.creation_user) AS vendor_creation_user_id,
          ven.modification_date AS vendor_modification_date_time,
          max(ven.modification_user) AS vendor_modification_user_id,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.vendor AS ven
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON ven.schema_id = ros.schema_id
   WHERE ven.code IS NOT NULL
     AND ven.eff_date IS NOT NULL
     AND (DATE(ven.expr_date) > current_date('US/Central')
          OR ven.expr_date IS NULL)
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            upper(ven.code),
            upper(ven.name),
            upper(ven.description),
            6,
            7,
            8,
            upper(ven.creation_user),
            10,
            upper(ven.modification_user)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND upper(rtrim(x.vendor_cd)) = upper(rtrim(z.vendor_cd))
AND x.eff_from_date = DATE(z.eff_from_date) WHEN MATCHED THEN
UPDATE
SET vendor_name = z.vendor_name,
    vendor_desc = z.vendor_desc,
    eff_to_date = DATE(z.eff_to_date),
    vendor_creation_user_id = substr(z.vendor_creation_user_id, 1, 20),
    vendor_creation_date_time = z.vendor_creation_date_time,
    vendor_modification_user_id = substr(z.vendor_modification_user_id, 1, 20),
    vendor_modification_date_time = z.vendor_modification_date_time,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        vendor_cd,
        eff_from_date,
        vendor_name,
        vendor_desc,
        eff_to_date,
        vendor_creation_user_id,
        vendor_creation_date_time,
        vendor_modification_user_id,
        vendor_modification_date_time,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.vendor_cd, DATE(z.eff_from_date), z.vendor_name, z.vendor_desc, DATE(z.eff_to_date), substr(z.vendor_creation_user_id, 1, 20), z.vendor_creation_date_time, substr(z.vendor_modification_user_id, 1, 20), z.vendor_modification_date_time, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             vendor_cd,
             eff_from_date
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_vendor
      GROUP BY company_code,
               coid,
               vendor_cd,
               eff_from_date
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_vendor');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Vendor');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;