-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_address_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CR_Patient_ADDRESS_WRK             	    #
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 				    #
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_ADDRESS_STG		    #
-- #	                                                                            #
-- #  INITIAL RELEASE	   	: Patient address details upload in EDW	            #
-- #  PROJECT             	: 	 		    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                              	    #
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_PATIENT_ADDRESS;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_ADDRESS_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Truncate WRK Table */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_address_wrk;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Populate WRK Table */
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_address_wrk (cr_patient_id, state_id, address_line_1_text, address_line_2_text, city_name, zip_code, source_system_code, dw_last_update_date_time)
    SELECT
        src.cr_patient_id,
        coalesce(rlc.master_lookup_sid, (
          SELECT
              rlc_0.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc_0
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON rlc_0.lookup_id = nm.lookup_sid
            WHERE upper(rtrim(nm.lookup_name)) = 'STATE'
             AND upper(rtrim(rlc_0.lookup_code)) = '-99'
        )) AS state_id,
        substr(src.address_line_1_text, 1, 60) AS address_line_1_text,
        substr(src.address_line_2_text, 1, 60) AS address_line_2_text,
        substr(src.city_name, 1, 50) AS city_name,
        src.zip_code,
        src.source_system_code,
        src.dw_last_update_date_time
      FROM
        (
          SELECT
              stg.cr_patient_id AS cr_patient_id,
              stg.state_id AS state_id,
              trim(stg.address_line_1_text) AS address_line_1_text,
              trim(stg.address_line_2_text) AS address_line_2_text,
              trim(stg.city_name) AS city_name,
              stg.zip_code AS zip_code,
              stg.source_system_code AS source_system_code,
              stg.dw_last_update_date_time AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_address_stg AS stg
        ) AS src
        LEFT OUTER JOIN (
          SELECT
              lkp.lookup_code,
              lkp.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS lkp
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON lkp.lookup_id = nm.lookup_sid
            WHERE upper(rtrim(nm.lookup_name)) = 'STATE'
        ) AS rlc ON upper(rtrim(src.state_id)) = upper(rtrim(rlc.lookup_code))
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_Patient_ADDRESS_WRK');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
