DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_physician.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Physician                          	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_STAGING.STG_DIMDOCTOR						##
-- ##							  													##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_Rad_Onc_Physician;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DIMDOCTOR');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_physician;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_physician AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimsiteid) AS INT64),
                                               CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimdoctorid) AS INT64)) AS physician_sk,
                                     rr.site_sk AS site_sk,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimdoctorid) AS INT64) AS source_physician_id,
                                     ra.location_sk AS location_sk,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimlookupid_resourcetype) AS INT64) AS resource_type_id,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctorfirstname)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctorfirstname)
                                            END, 1, 70) AS physician_first_name,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctorlastname)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctorlastname)
                                            END, 1, 70) AS physician_last_name,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctornamesuffix)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctornamesuffix)
                                            END, 1, 20) AS physician_suffix_name,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctoraliasname)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctoraliasname)
                                            END, 1, 70) AS physician_alias_name,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctorhonorific)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctorhonorific)
                                            END, 1, 20) AS physician_title_name,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctoremailaddress)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctoremailaddress)
                                            END, 1, 100) AS physician_email_address_text,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctorspecialty)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctorspecialty)
                                            END, 1, 65) AS physician_specialty_text,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctorid)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctorid)
                                            END, 1, 20) AS physician_id_text,
                                     CASE upper(trim(dhd.resourceobjectstatus))
                                         WHEN 'ACTIVE' THEN 'Y'
                                         WHEN 'DELETED' THEN 'N'
                                         ELSE 'U'
                                     END AS resource_active_ind,
                                     CASE upper(trim(dhd.schedulable))
                                         WHEN 'YES' THEN 'Y'
                                         WHEN 'NO' THEN 'N'
                                         ELSE 'U'
                                     END AS appointment_schedule_ind,
                                     PARSE_DATETIME("%b %e %Y %l:%M%p", dhd.doctororiginationdate) AS physician_start_date_time,
                                     PARSE_DATETIME("%b %e %Y %l:%M%p", dhd.doctorterminationdate) AS physician_termination_date_time,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctorinstitution)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctorinstitution)
                                            END, 1, 100) AS physician_institution_text,
                                     substr(CASE
                                                WHEN upper(trim(dhd.doctorcomment)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.doctorcomment)
                                            END, 1, 100) AS physician_comment_text,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.ctrresourceser) AS INT64) AS resource_id,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.logid) AS INT64) AS log_id,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.runid) AS INT64) AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimdoctor AS dhd
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site) AS rr ON rr.source_site_id = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimsiteid) AS FLOAT64)
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_location AS ra ON CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimlocationid) AS INT64) = ra.source_location_id
   AND rr.site_sk = ra.site_sk) AS ms ON mt.physician_sk = ms.physician_sk
AND mt.site_sk = ms.site_sk
AND mt.source_physician_id = ms.source_physician_id
AND (coalesce(mt.location_sk, 0) = coalesce(ms.location_sk, 0)
     AND coalesce(mt.location_sk, 1) = coalesce(ms.location_sk, 1))
AND (coalesce(mt.resource_type_id, 0) = coalesce(ms.resource_type_id, 0)
     AND coalesce(mt.resource_type_id, 1) = coalesce(ms.resource_type_id, 1))
AND (upper(coalesce(mt.physician_first_name, '0')) = upper(coalesce(ms.physician_first_name, '0'))
     AND upper(coalesce(mt.physician_first_name, '1')) = upper(coalesce(ms.physician_first_name, '1')))
AND (upper(coalesce(mt.physician_last_name, '0')) = upper(coalesce(ms.physician_last_name, '0'))
     AND upper(coalesce(mt.physician_last_name, '1')) = upper(coalesce(ms.physician_last_name, '1')))
AND (upper(coalesce(mt.physician_suffix_name, '0')) = upper(coalesce(ms.physician_suffix_name, '0'))
     AND upper(coalesce(mt.physician_suffix_name, '1')) = upper(coalesce(ms.physician_suffix_name, '1')))
AND (upper(coalesce(mt.physician_alias_name, '0')) = upper(coalesce(ms.physician_alias_name, '0'))
     AND upper(coalesce(mt.physician_alias_name, '1')) = upper(coalesce(ms.physician_alias_name, '1')))
AND (upper(coalesce(mt.physician_title_name, '0')) = upper(coalesce(ms.physician_title_name, '0'))
     AND upper(coalesce(mt.physician_title_name, '1')) = upper(coalesce(ms.physician_title_name, '1')))
AND (upper(coalesce(mt.physician_email_address_text, '0')) = upper(coalesce(ms.physician_email_address_text, '0'))
     AND upper(coalesce(mt.physician_email_address_text, '1')) = upper(coalesce(ms.physician_email_address_text, '1')))
AND (upper(coalesce(mt.physician_specialty_text, '0')) = upper(coalesce(ms.physician_specialty_text, '0'))
     AND upper(coalesce(mt.physician_specialty_text, '1')) = upper(coalesce(ms.physician_specialty_text, '1')))
AND (upper(coalesce(mt.physician_id_text, '0')) = upper(coalesce(ms.physician_id_text, '0'))
     AND upper(coalesce(mt.physician_id_text, '1')) = upper(coalesce(ms.physician_id_text, '1')))
AND (upper(coalesce(mt.resource_active_ind, '0')) = upper(coalesce(ms.resource_active_ind, '0'))
     AND upper(coalesce(mt.resource_active_ind, '1')) = upper(coalesce(ms.resource_active_ind, '1')))
AND (upper(coalesce(mt.appointment_schedule_ind, '0')) = upper(coalesce(ms.appointment_schedule_ind, '0'))
     AND upper(coalesce(mt.appointment_schedule_ind, '1')) = upper(coalesce(ms.appointment_schedule_ind, '1')))
AND (coalesce(mt.physician_start_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.physician_start_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.physician_start_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.physician_start_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.physician_termination_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.physician_termination_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.physician_termination_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.physician_termination_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.physician_institution_text, '0')) = upper(coalesce(ms.physician_institution_text, '0'))
     AND upper(coalesce(mt.physician_institution_text, '1')) = upper(coalesce(ms.physician_institution_text, '1')))
AND (upper(coalesce(mt.physician_comment_text, '0')) = upper(coalesce(ms.physician_comment_text, '0'))
     AND upper(coalesce(mt.physician_comment_text, '1')) = upper(coalesce(ms.physician_comment_text, '1')))
AND (coalesce(mt.resource_id, 0) = coalesce(ms.resource_id, 0)
     AND coalesce(mt.resource_id, 1) = coalesce(ms.resource_id, 1))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (physician_sk,
        site_sk,
        source_physician_id,
        location_sk,
        resource_type_id,
        physician_first_name,
        physician_last_name,
        physician_suffix_name,
        physician_alias_name,
        physician_title_name,
        physician_email_address_text,
        physician_specialty_text,
        physician_id_text,
        resource_active_ind,
        appointment_schedule_ind,
        physician_start_date_time,
        physician_termination_date_time,
        physician_institution_text,
        physician_comment_text,
        resource_id,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.physician_sk, ms.site_sk, ms.source_physician_id, ms.location_sk, ms.resource_type_id, ms.physician_first_name, ms.physician_last_name, ms.physician_suffix_name, ms.physician_alias_name, ms.physician_title_name, ms.physician_email_address_text, ms.physician_specialty_text, ms.physician_id_text, ms.resource_active_ind, ms.appointment_schedule_ind, ms.physician_start_date_time, ms.physician_termination_date_time, ms.physician_institution_text, ms.physician_comment_text, ms.resource_id, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT physician_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.rad_onc_physician
      GROUP BY physician_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.rad_onc_physician');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Physician');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;