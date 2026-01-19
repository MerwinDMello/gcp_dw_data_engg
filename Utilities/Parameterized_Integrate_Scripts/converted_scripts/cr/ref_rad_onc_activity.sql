-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_activity.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RAD_ONC_ACTIVITY                ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_STAGING.STG_DimActivity 			##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_RO_RAD_ONC_ACTVT_PRIORITY;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DimActivity');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Data
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_activity;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_activity AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY upper(dhd.dimsiteid), upper(dhd.dimactivityid)) AS activity_sk,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimlookupid_activityobjectstat) as INT64) AS activity_object_status_id,
        rr.site_sk AS site_sk,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimactivityid) as INT64) AS source_activity_id,
        substr(CASE
          WHEN upper(trim(dhd.activitycategorycode)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.activitycategorycode)
        END, 1, 30) AS activity_category_code_text,
        substr(CASE
          WHEN upper(trim(dhd.activitycategoryenu)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.activitycategoryenu)
        END, 1, 50) AS activity_category_desc,
        substr(CASE
          WHEN upper(trim(dhd.activitycode)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.activitycode)
        END, 1, 65) AS activity_code_text,
        substr(CASE
          WHEN upper(trim(dhd.activitynameenu)) = '' THEN CAST(NULL as STRING)
          ELSE trim(dhd.activitynameenu)
        END, 1, 100) AS activity_desc,
        CAST(trim(substr(dhd.lastmodifiedon, 1, 19)) as DATETIME) AS source_last_modified_date_time,
        CAST(trim(substr(dhd.effectivestartdate, 1, 26)) as DATETIME) AS effective_start_date_time,
        CAST(trim(substr(dhd.effectiveenddate, 1, 26)) as DATETIME) AS effective_end_date_time,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.logid) as INT64) AS log_id,
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.runid) as INT64) AS run_id,
        'R' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimactivity AS dhd
        INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimsiteid) as INT64)
  ) AS ms
  ON mt.activity_sk = ms.activity_sk
   AND (coalesce(mt.activity_object_status_id, 0) = coalesce(ms.activity_object_status_id, 0)
   AND coalesce(mt.activity_object_status_id, 1) = coalesce(ms.activity_object_status_id, 1))
   AND mt.site_sk = ms.site_sk
   AND mt.source_activity_id = ms.source_activity_id
   AND (upper(coalesce(mt.activity_category_code_text, '0')) = upper(coalesce(ms.activity_category_code_text, '0'))
   AND upper(coalesce(mt.activity_category_code_text, '1')) = upper(coalesce(ms.activity_category_code_text, '1')))
   AND (upper(coalesce(mt.activity_category_desc, '0')) = upper(coalesce(ms.activity_category_desc, '0'))
   AND upper(coalesce(mt.activity_category_desc, '1')) = upper(coalesce(ms.activity_category_desc, '1')))
   AND (upper(coalesce(mt.activity_code_text, '0')) = upper(coalesce(ms.activity_code_text, '0'))
   AND upper(coalesce(mt.activity_code_text, '1')) = upper(coalesce(ms.activity_code_text, '1')))
   AND (upper(coalesce(mt.activity_desc, '0')) = upper(coalesce(ms.activity_desc, '0'))
   AND upper(coalesce(mt.activity_desc, '1')) = upper(coalesce(ms.activity_desc, '1')))
   AND (coalesce(mt.source_last_modified_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.source_last_modified_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.source_last_modified_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.source_last_modified_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.effective_start_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.effective_start_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.effective_start_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.effective_start_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.effective_end_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.effective_end_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.effective_end_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.effective_end_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
   AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
   AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
   AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (activity_sk, activity_object_status_id, site_sk, source_activity_id, activity_category_code_text, activity_category_desc, activity_code_text, activity_desc, source_last_modified_date_time, effective_start_date_time, effective_end_date_time, log_id, run_id, source_system_code, dw_last_update_date_time)
      VALUES (ms.activity_sk, ms.activity_object_status_id, ms.site_sk, ms.source_activity_id, ms.activity_category_code_text, ms.activity_category_desc, ms.activity_code_text, ms.activity_desc, ms.source_last_modified_date_time, ms.effective_start_date_time, ms.effective_end_date_time, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RAD_ONC_ACTIVITY');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
