DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_contact_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CR_Patient_Contact_WRK             	    #
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 				    #
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_CONTACT_STG		    #
-- #	                                                                            #
-- #  INITIAL RELEASE	   	: 						    #
-- #  PROJECT             	: 	 		    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                              	    #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_CONTACT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_CONTACT_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_contact_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate WRK Table */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_contact_wrk (patient_contact_id, cr_patient_id, contact_relation_id, contact_type_id, contact_num_code, contact_first_name, contact_last_name, contact_middle_name, preferred_contact_method_text, source_system_code, dw_last_update_date_time)
SELECT src.patient_contact_id,
       coalesce(src.cr_patient_id, -99) AS cr_patient_id,
       coalesce(rlc.master_lookup_sid,
                  (SELECT rcl.master_lookup_sid
                   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rcl
                   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON rcl.lookup_id = nm.lookup_sid
                   WHERE upper(rtrim(nm.lookup_name)) = 'RELATION'
                     AND upper(rtrim(rcl.lookup_code)) = '-99' )) AS contact_relation_id,
       coalesce(rlc1.master_lookup_sid,
                  (SELECT rcl1.master_lookup_sid
                   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rcl1
                   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON rcl1.lookup_id = nm.lookup_sid
                   WHERE upper(rtrim(nm.lookup_name)) = 'CONTACT TYPE'
                     AND upper(rtrim(rcl1.lookup_code)) = '-99' )) AS contact_type_id,
       src.contact_num_code,
       src.contact_first_name,
       src.contact_last_name,
       src.contact_middle_name,
       src.preferred_contact_method_text,
       src.source_system_code,
       src.dw_last_update_date_time
FROM
  (SELECT stg.patient_contact_id AS patient_contact_id,
          stg.cr_patient_id AS cr_patient_id,
          trim(stg.contact_relation_id) AS contact_relation_id,
          trim(stg.contact_type_id) AS contact_type_id,
          stg.contact_num_code AS contact_num_code,
          stg.contact_first_name AS contact_first_name,
          stg.contact_last_name AS contact_last_name,
          stg.contact_middle_name AS contact_middle_name,
          stg.preferred_contact_method_text AS preferred_contact_method_text,
          stg.source_system_code AS source_system_code,
          stg.dw_last_update_date_time AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_contact_stg AS stg) AS src
LEFT OUTER JOIN
  (SELECT lkp.lookup_code,
          lkp.master_lookup_sid
   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS lkp
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON lkp.lookup_id = nm.lookup_sid
   WHERE upper(rtrim(nm.lookup_name)) = 'RELATION' ) AS rlc ON upper(rtrim(src.contact_relation_id)) = upper(rtrim(rlc.lookup_code))
LEFT OUTER JOIN
  (SELECT lkp.lookup_code,
          lkp.master_lookup_sid
   FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS lkp
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON lkp.lookup_id = nm.lookup_sid
   WHERE upper(rtrim(nm.lookup_name)) = 'CONTACT TYPE' ) AS rlc1 ON upper(rtrim(src.contact_type_id)) = upper(rtrim(rlc1.lookup_code));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_Contact_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF