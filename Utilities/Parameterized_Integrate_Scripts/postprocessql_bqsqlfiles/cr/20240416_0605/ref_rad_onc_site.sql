DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_site.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RAD_ONC_SITE                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		  		   : EDWCR_STAGING.CR_RO_DIMSITE_STG     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_SITE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CR_RO_DIMSITE_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY ds.dimsiteid) AS site_sk,
                                     ds.dimsiteid AS source_site_id,
                                     substr(CASE
                                                WHEN trim(ds.siteguid) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.siteguid)
                                            END, 1, 40) AS source_site_guid_text,
                                     substr(CASE
                                                WHEN trim(ds.sitecode) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.sitecode)
                                            END, 1, 20) AS site_code_text,
                                     substr(CASE
                                                WHEN trim(ds.sitename) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.sitename)
                                            END, 1, 32) AS site_name,
                                     substr(CASE
                                                WHEN trim(ds.servername) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.servername)
                                            END, 1, 20) AS SERVER_NAME,
                                     substr(CASE
                                                WHEN trim(ds.sitedescription) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.sitedescription)
                                            END, 1, 100) AS site_desc,
                                     substr(CASE
                                                WHEN trim(ds.serveripaddress) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.serveripaddress)
                                            END, 1, 20) AS server_ip_address_text,
                                     substr(CASE
                                                WHEN trim(ds.auraversion) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.auraversion)
                                            END, 1, 20) AS aura_version_text,
                                     CAST(trim(substr(trim(ds.auralastinstalleddate), 1, 19)) AS DATETIME) AS aura_last_installed_date_time,
                                     CAST(trim(substr(trim(ds.registrationdate), 1, 19)) AS DATETIME) AS registration_date_time,
                                     substr(CASE
                                                WHEN trim(ds.hstryusername) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.hstryusername)
                                            END, 1, 30) AS history_user_name,
                                     CAST(trim(substr(trim(ds.hstrydatetime), 1, 19)) AS DATETIME) AS history_date_time,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_ro_dimsite_stg AS ds) AS ms ON mt.site_sk = ms.site_sk
AND mt.source_site_id = ms.source_site_id
AND mt.source_site_guid_text = ms.source_site_guid_text
AND mt.site_code_text = ms.site_code_text
AND mt.site_name = ms.site_name
AND (upper(coalesce(mt.server_name, '0')) = upper(coalesce(ms.server_name, '0'))
     AND upper(coalesce(mt.server_name, '1')) = upper(coalesce(ms.server_name, '1')))
AND (upper(coalesce(mt.site_desc, '0')) = upper(coalesce(ms.site_desc, '0'))
     AND upper(coalesce(mt.site_desc, '1')) = upper(coalesce(ms.site_desc, '1')))
AND (upper(coalesce(mt.server_ip_address_text, '0')) = upper(coalesce(ms.server_ip_address_text, '0'))
     AND upper(coalesce(mt.server_ip_address_text, '1')) = upper(coalesce(ms.server_ip_address_text, '1')))
AND (upper(coalesce(mt.aura_version_text, '0')) = upper(coalesce(ms.aura_version_text, '0'))
     AND upper(coalesce(mt.aura_version_text, '1')) = upper(coalesce(ms.aura_version_text, '1')))
AND (coalesce(mt.aura_last_installed_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.aura_last_installed_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.aura_last_installed_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.aura_last_installed_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.registration_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.registration_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.registration_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.registration_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.history_user_name, '0')) = upper(coalesce(ms.history_user_name, '0'))
     AND upper(coalesce(mt.history_user_name, '1')) = upper(coalesce(ms.history_user_name, '1')))
AND (coalesce(mt.history_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.history_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.history_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.history_date_time, DATETIME '1970-01-01 00:00:01'))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (site_sk,
        source_site_id,
        source_site_guid_text,
        site_code_text,
        site_name,
        SERVER_NAME,
        site_desc,
        server_ip_address_text,
        aura_version_text,
        aura_last_installed_date_time,
        registration_date_time,
        history_user_name,
        history_date_time,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.site_sk, ms.source_site_id, ms.source_site_guid_text, ms.site_code_text, ms.site_name, ms.server_name, ms.site_desc, ms.server_ip_address_text, ms.aura_version_text, ms.aura_last_installed_date_time, ms.registration_date_time, ms.history_user_name, ms.history_date_time, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site
      GROUP BY site_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RAD_ONC_SITE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF