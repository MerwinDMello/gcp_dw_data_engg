DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_junc_physician_address.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Junc_Physician_Address                   ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.STG_DimDoctor 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Rad_Onc_Junc_Physician_Address;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DimDoctor');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_junc_physician_address;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_junc_physician_address AS mt USING
  (SELECT DISTINCT stg.physician_sk,
                   stg.address_type_code AS address_type_code,
                   stg.address_sk,
                   stg.primary_address_ind AS primary_address_ind,
                   'R' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT pt.physician_sk AS physician_sk,
             CASE
                 WHEN upper(trim(dhd.doctoraddresstype)) = 'OFFICE' THEN 'O'
                 ELSE 'U'
             END AS address_type_code,
             ra.address_sk AS address_sk,
             CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimdoctorid) AS INT64) AS dimdoctorid,
             CASE CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.isprimarydoctoraddress) AS INT64)
                 WHEN 1 THEN 'Y'
                 WHEN 0 THEN 'N'
                 ELSE CAST(NULL AS STRING)
             END AS primary_address_ind
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimdoctor AS dhd
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_address AS ra ON upper(trim(coalesce(dhd.doctorcompleteaddress, '##'))) = upper(trim(coalesce(ra.full_address_text, '##')))
      AND upper(trim(coalesce(dhd.doctoraddresscomment, '##'))) = upper(trim(coalesce(ra.address_comment_text, '##')))
      AND trim(ra.address_line_1_text) IS NULL
      AND trim(ra.address_line_2_text) IS NULL
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimsiteid) AS INT64)
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_physician AS pt ON rr.site_sk = pt.site_sk
      AND CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimdoctorid) AS INT64) = pt.source_physician_id) AS stg) AS ms ON mt.physician_sk = ms.physician_sk
AND mt.address_type_code = ms.address_type_code
AND (coalesce(mt.address_sk, 0) = coalesce(ms.address_sk, 0)
     AND coalesce(mt.address_sk, 1) = coalesce(ms.address_sk, 1))
AND (upper(coalesce(mt.primary_address_ind, '0')) = upper(coalesce(ms.primary_address_ind, '0'))
     AND upper(coalesce(mt.primary_address_ind, '1')) = upper(coalesce(ms.primary_address_ind, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (physician_sk,
        address_type_code,
        address_sk,
        primary_address_ind,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.physician_sk, ms.address_type_code, ms.address_sk, ms.primary_address_ind, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT physician_sk,
             address_type_code
      FROM `hca-hin-dev-cur-ops`.edwcr.rad_onc_junc_physician_address
      GROUP BY physician_sk,
               address_type_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.rad_onc_junc_physician_address');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Junc_Physician_Address');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;