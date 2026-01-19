DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_lookup.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RAD_ONC_LOOKUP                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		  		   : EDWCR_STAGING.STG_DIMLOOKUP     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_LOOKUP;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DIMLOOKUP');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_lookup;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_rad_onc_lookup AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY ds.dimsiteid,
                                               CASE
                                                   WHEN trim(format('%20d', ds.dimlookupid)) = '' THEN CAST(NULL AS STRING)
                                                   ELSE trim(format('%20d', ds.dimlookupid))
                                               END) AS lookup_sk,
                                     rr.site_sk AS site_sk,
                                     CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(CASE
                                                                                                                   WHEN trim(format('%20d', ds.dimlookupid)) = '' THEN CAST(NULL AS STRING)
                                                                                                                   ELSE trim(format('%20d', ds.dimlookupid))
                                                                                                               END) AS INT64) AS source_lookup_id,
                                     substr(CASE
                                                WHEN upper(trim(ds.tablename)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.tablename)
                                            END, 1, 50) AS TABLE_NAME,
                                     substr(CASE
                                                WHEN upper(trim(ds.lookupcode)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.lookupcode)
                                            END, 1, 65) AS lookup_code_text,
                                     substr(CASE
                                                WHEN upper(trim(ds.lookupdescriptionenu)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.lookupdescriptionenu)
                                            END, 1, 255) AS lookup_desc,
                                     ds.logid AS log_id,
                                     ds.runid AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimlookup AS ds
   CROSS JOIN {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site AS rr
   WHERE rr.source_site_id = ds.dimsiteid ) AS ms ON mt.lookup_sk = ms.lookup_sk
AND mt.site_sk = ms.site_sk
AND mt.source_lookup_id = ms.source_lookup_id
AND (upper(coalesce(mt.table_name, '0')) = upper(coalesce(ms.table_name, '0'))
     AND upper(coalesce(mt.table_name, '1')) = upper(coalesce(ms.table_name, '1')))
AND (upper(coalesce(mt.lookup_code_text, '0')) = upper(coalesce(ms.lookup_code_text, '0'))
     AND upper(coalesce(mt.lookup_code_text, '1')) = upper(coalesce(ms.lookup_code_text, '1')))
AND (upper(coalesce(mt.lookup_desc, '0')) = upper(coalesce(ms.lookup_desc, '0'))
     AND upper(coalesce(mt.lookup_desc, '1')) = upper(coalesce(ms.lookup_desc, '1')))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (lookup_sk,
        site_sk,
        source_lookup_id,
        TABLE_NAME,
        lookup_code_text,
        lookup_desc,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.lookup_sk, ms.site_sk, ms.source_lookup_id, ms.table_name, ms.lookup_code_text, ms.lookup_desc, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT lookup_sk
      FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_lookup
      GROUP BY lookup_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_rad_onc_lookup');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RAD_ONC_LOOKUP');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF