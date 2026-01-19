-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_junc_physician_phone.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Junc_Physician_Phone                   ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DIMPATIENT 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_RO_Rad_Onc_Junc_Pat_Phone;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DimDoctor');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Data
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_junc_physician_phone;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_junc_physician_phone AS mt USING (
    SELECT DISTINCT
        pt.physician_sk AS physician_sk,
        dim_sub.phone_num_type_code AS phone_num_type_code,
        rp.phone_num_sk,
        dim_sub.source_system_code AS source_system_code,
        dim_sub.dw_last_update_date_time
      FROM
        (
          SELECT
              CAST(NULL as INT64) AS physician_sk,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimsiteid) as INT64) AS dimsiteid,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimdoctorid) as INT64) AS dimdoctorid,
              'O' AS phone_num_type_code,
              stg_dimdoctor.doctorprimaryphonenumber AS phone,
              'R' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimdoctor
          UNION DISTINCT
          SELECT
              CAST(NULL as INT64) AS physician_sk,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimsiteid) as INT64) AS dimsiteid,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimdoctorid) as INT64) AS dimdoctorid,
              'S' AS phone_num_type_code,
              stg_dimdoctor.doctorsecondaryphonenumber AS phone,
              'R' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimdoctor
          UNION DISTINCT
          SELECT
              CAST(NULL as INT64) AS physician_sk,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimsiteid) as INT64) AS dimsiteid,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimdoctorid) as INT64) AS dimdoctorid,
              'P' AS phone_num_type_code,
              stg_dimdoctor.doctorpagernumber AS phone,
              'R' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimdoctor
          UNION DISTINCT
          SELECT
              CAST(NULL as INT64) AS physician_sk,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimsiteid) as INT64) AS dimsiteid,
              CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimdoctor.dimdoctorid) as INT64) AS dimdoctorid,
              'F' AS phone_num_type_code,
              stg_dimdoctor.doctorfaxnumber AS phone,
              'R' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimdoctor
        ) AS dim_sub
        INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_phone AS rp ON upper(trim(dim_sub.phone)) = upper(trim(rp.phone_num_text))
        INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = dim_sub.dimsiteid
        INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_physician AS pt ON rr.site_sk = pt.site_sk
         AND dim_sub.dimdoctorid = pt.source_physician_id
  ) AS ms
  ON mt.physician_sk = ms.physician_sk
   AND mt.phone_num_type_code = ms.phone_num_type_code
   AND (coalesce(mt.phone_num_sk, 0) = coalesce(ms.phone_num_sk, 0)
   AND coalesce(mt.phone_num_sk, 1) = coalesce(ms.phone_num_sk, 1))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (physician_sk, phone_num_type_code, phone_num_sk, source_system_code, dw_last_update_date_time)
      VALUES (ms.physician_sk, ms.phone_num_type_code, ms.phone_num_sk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Junc_Physician_Phone');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
