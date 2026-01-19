DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_patient_contact.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.RAD_ONC_PATIENT_CONTACT                     	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DIMPATIENT 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_PATIENT;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DIMPATIENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_patient_contact;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.rad_onc_patient_contact AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY stg.patient_sk,
                                               stg.contact_address_sk,
                                               upper(stg.contact_full_name),
                                               upper(stg.contact_relation_text),
                                               upper(stg.contact_entrusted_ind),
                                               upper(stg.contact_comment_text)) AS patient_contact_sk,
                                     stg.patient_sk,
                                     stg.contact_address_sk,
                                     substr(stg.contact_full_name, 1, 50) AS contact_full_name,
                                     substr(stg.contact_relation_text, 1, 50) AS contact_relation_text,
                                     stg.contact_entrusted_ind AS contact_entrusted_ind,
                                     substr(stg.contact_comment_text, 1, 100) AS contact_comment_text,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT NULL AS patient_contact_sk,
             pt.patient_sk AS patient_sk,
             rr.site_sk AS site_sk,
             ra.address_sk AS contact_address_sk,
             CASE
                 WHEN upper(trim(dhd.patemergencycontactfullname)) = '' THEN CAST(NULL AS STRING)
                 ELSE trim(dhd.patemergencycontactfullname)
             END AS contact_full_name,
             CASE
                 WHEN upper(trim(dhd.patemergencycontactrelationshp)) = '' THEN CAST(NULL AS STRING)
                 ELSE trim(dhd.patemergencycontactrelationshp)
             END AS contact_relation_text,
             CASE dhd.patemergencycontactentrusted
                 WHEN 1 THEN 'Y'
                 WHEN 0 THEN 'N'
                 ELSE 'U'
             END AS contact_entrusted_ind,
             CASE
                 WHEN upper(trim(dhd.patemergencycontactcomment)) = '' THEN CAST(NULL AS STRING)
                 ELSE trim(dhd.patemergencycontactcomment)
             END AS contact_comment_text
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient AS dhd
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.rad_onc_address AS ra ON upper(trim(dhd.patemergencycontactaddress)) = upper(trim(ra.full_address_text))
      AND trim(ra.address_line_1_text) IS NULL
      AND trim(ra.address_line_2_text) IS NULL
      AND trim(ra.address_comment_text) IS NULL
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site AS rr ON rr.source_site_id = dhd.dimsiteid
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient AS pt ON rr.site_sk = pt.site_sk
      AND dhd.dimpatientid = pt.source_patient_id) AS stg
   WHERE stg.contact_address_sk IS NOT NULL
     OR stg.contact_full_name IS NOT NULL
     OR stg.contact_relation_text IS NOT NULL
     OR stg.contact_entrusted_ind IS NOT NULL
     OR stg.contact_comment_text IS NOT NULL ) AS ms ON mt.patient_contact_sk = ms.patient_contact_sk
AND mt.patient_sk = ms.patient_sk
AND (coalesce(mt.contact_address_sk, 0) = coalesce(ms.contact_address_sk, 0)
     AND coalesce(mt.contact_address_sk, 1) = coalesce(ms.contact_address_sk, 1))
AND (upper(coalesce(mt.contact_full_name, '0')) = upper(coalesce(ms.contact_full_name, '0'))
     AND upper(coalesce(mt.contact_full_name, '1')) = upper(coalesce(ms.contact_full_name, '1')))
AND (upper(coalesce(mt.contact_relation_text, '0')) = upper(coalesce(ms.contact_relation_text, '0'))
     AND upper(coalesce(mt.contact_relation_text, '1')) = upper(coalesce(ms.contact_relation_text, '1')))
AND (upper(coalesce(mt.contact_entrusted_ind, '0')) = upper(coalesce(ms.contact_entrusted_ind, '0'))
     AND upper(coalesce(mt.contact_entrusted_ind, '1')) = upper(coalesce(ms.contact_entrusted_ind, '1')))
AND (upper(coalesce(mt.contact_comment_text, '0')) = upper(coalesce(ms.contact_comment_text, '0'))
     AND upper(coalesce(mt.contact_comment_text, '1')) = upper(coalesce(ms.contact_comment_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_contact_sk,
        patient_sk,
        contact_address_sk,
        contact_full_name,
        contact_relation_text,
        contact_entrusted_ind,
        contact_comment_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_contact_sk, ms.patient_sk, ms.contact_address_sk, ms.contact_full_name, ms.contact_relation_text, ms.contact_entrusted_ind, ms.contact_comment_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_contact_sk
      FROM {{ params.param_cr_core_dataset_name }}.rad_onc_patient_contact
      GROUP BY patient_contact_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.rad_onc_patient_contact');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','RAD_ONC_PATIENT_CONTACT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF