-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/mt_ref_national_svc_center.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.REF_NATIONAL_SERV_CENTER	             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.Ref_National_Svc_Center_Stg		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                          		#
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_MT_REF_NAT_SERV_CENTER;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_National_Svc_Center_Stg');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_national_service_center;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Insert the new records into the Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_national_service_center AS mt USING (
    SELECT DISTINCT
        ref_national_svc_center_stg.id,
        ref_national_svc_center_stg.code AS nsc_code,
        ref_national_svc_center_stg.subcode AS nsc_sub_code,
        ref_national_svc_center_stg.description,
        ref_national_svc_center_stg.category,
        ref_national_svc_center_stg.subcategory,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.ref_national_svc_center_stg
  ) AS ms
  ON mt.nsc_id = ms.id
   AND mt.nsc_code = ms.nsc_code
   AND (upper(coalesce(mt.nsc_sub_code, '0')) = upper(coalesce(ms.nsc_sub_code, '0'))
   AND upper(coalesce(mt.nsc_sub_code, '1')) = upper(coalesce(ms.nsc_sub_code, '1')))
   AND (upper(coalesce(mt.nsc_desc, '0')) = upper(coalesce(ms.description, '0'))
   AND upper(coalesce(mt.nsc_desc, '1')) = upper(coalesce(ms.description, '1')))
   AND (upper(coalesce(mt.nsc_category_text, '0')) = upper(coalesce(ms.category, '0'))
   AND upper(coalesce(mt.nsc_category_text, '1')) = upper(coalesce(ms.category, '1')))
   AND (upper(coalesce(mt.nsc_sub_category_text, '0')) = upper(coalesce(ms.subcategory, '0'))
   AND upper(coalesce(mt.nsc_sub_category_text, '1')) = upper(coalesce(ms.subcategory, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (nsc_id, nsc_code, nsc_sub_code, nsc_desc, nsc_category_text, nsc_sub_category_text, source_system_code, dw_last_update_date_time)
      VALUES (ms.id, ms.nsc_code, ms.nsc_sub_code, ms.description, ms.category, ms.subcategory, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Ref_National_Service_Center');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
