DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_location.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RAD_ONC_LOCATION                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		  		   : EDWCR_STAGING.STG_DIMLOCATION     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_LOCATION;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DIMLOCATION');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_location;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_location AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY rr.site_sk,
                                               CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(ds.dimlocationid) AS INT64)) AS location_sk,
                                     rr.site_sk AS site_sk,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(ds.dimlocationid) AS INT64) AS source_location_id,
                                     substr(CASE
                                                WHEN upper(trim(ds.country)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.country)
                                            END, 1, 50) AS country_name,
                                     substr(CASE
                                                WHEN upper(trim(ds.state)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.state)
                                            END, 1, 50) AS state_name,
                                     substr(CASE
                                                WHEN upper(trim(ds.city)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.city)
                                            END, 1, 50) AS city_name,
                                     substr(CASE
                                                WHEN upper(trim(ds.county)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.county)
                                            END, 1, 50) AS county_name,
                                     CASE
                                         WHEN upper(trim(ds.postalcode)) = '' THEN CAST(NULL AS STRING)
                                         ELSE trim(ds.postalcode)
                                     END AS zip_code,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(ds.logid) AS INT64) AS log_id,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(ds.runid) AS INT64) AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimlocation AS ds
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(ds.dimsiteid) AS INT64)) AS ms ON mt.location_sk = ms.location_sk
AND mt.site_sk = ms.site_sk
AND mt.source_location_id = ms.source_location_id
AND (upper(coalesce(mt.country_name, '0')) = upper(coalesce(ms.country_name, '0'))
     AND upper(coalesce(mt.country_name, '1')) = upper(coalesce(ms.country_name, '1')))
AND (upper(coalesce(mt.state_name, '0')) = upper(coalesce(ms.state_name, '0'))
     AND upper(coalesce(mt.state_name, '1')) = upper(coalesce(ms.state_name, '1')))
AND (upper(coalesce(mt.city_name, '0')) = upper(coalesce(ms.city_name, '0'))
     AND upper(coalesce(mt.city_name, '1')) = upper(coalesce(ms.city_name, '1')))
AND (upper(coalesce(mt.county_name, '0')) = upper(coalesce(ms.county_name, '0'))
     AND upper(coalesce(mt.county_name, '1')) = upper(coalesce(ms.county_name, '1')))
AND (upper(coalesce(mt.zip_code, '0')) = upper(coalesce(ms.zip_code, '0'))
     AND upper(coalesce(mt.zip_code, '1')) = upper(coalesce(ms.zip_code, '1')))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (location_sk,
        site_sk,
        source_location_id,
        country_name,
        state_name,
        city_name,
        county_name,
        zip_code,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.location_sk, ms.site_sk, ms.source_location_id, ms.country_name, ms.state_name, ms.city_name, ms.county_name, ms.zip_code, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT location_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_location
      GROUP BY location_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_location');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RAD_ONC_LOCATION');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF