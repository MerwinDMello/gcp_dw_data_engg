DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_patient_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Patient_Type - BTEQ Script.
      Mod1: Creation of script on 8/8/2011. SW.
      Mod2:Changed script for new DDL on 9/22/2011. SW.
	Mod3:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod4: Removed _MV tables along with Insert/Select. Replaced with Merge
****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA222;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_patient_type AS x USING
  (SELECT ff.company_code,
          CASE
              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
              ELSE og.client_id
          END AS coido,
          mpt.id AS patient_type_id,
          og.short_name AS unit_num,
          mpt.code AS cc_patient_type_code,
          mpt.ext_patient_type AS pa_patient_type_code,
          mpt.description AS patient_type_desc,
          mpt.ip_op_ind,
          mpt.ce_patient_type_id AS ce_patient_type_id,
          CAST(mpt.date_created AS DATETIME) AS create_date_time,
          CAST(mpt.date_updated AS DATETIME) AS update_date_time,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON mpt.org_id = og.org_id
   AND mpt.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON og.schema_id = sm.schema_id
   AND mpt.schema_id = sm.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ff ON upper(rtrim(ff.coid)) = upper(rtrim(CASE
                                                                                                                         WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                         ELSE og.client_id
                                                                                                                     END))) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coido))
AND x.patient_type_id = z.patient_type_id WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num, 1, 5),
    cc_patient_type_code = z.cc_patient_type_code,
    pa_patient_type_code = z.pa_patient_type_code,
    patient_type_desc = z.patient_type_desc,
    ip_op_ind = substr(z.ip_op_ind, 1, 1),
    ce_patient_type_id = z.ce_patient_type_id,
    create_date_time = z.create_date_time,
    update_date_time = z.update_date_time,
    dw_last_update_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_type_id,
        unit_num,
        cc_patient_type_code,
        pa_patient_type_code,
        patient_type_desc,
        ip_op_ind,
        ce_patient_type_id,
        create_date_time,
        update_date_time,
        dw_last_update_time,
        source_system_code)
VALUES (z.company_code, substr(z.coido, 1, 5), z.patient_type_id, substr(z.unit_num, 1, 5), z.cc_patient_type_code, z.pa_patient_type_code, z.patient_type_desc, substr(z.ip_op_ind, 1, 1), z.ce_patient_type_id, z.create_date_time, z.update_date_time, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_type_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_patient_type
      GROUP BY company_code,
               coid,
               patient_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_patient_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Patient_Type');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;