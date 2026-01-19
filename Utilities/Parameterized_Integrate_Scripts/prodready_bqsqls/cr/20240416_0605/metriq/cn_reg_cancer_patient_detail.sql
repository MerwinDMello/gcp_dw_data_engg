DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_reg_cancer_patient_detail.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CANCER_PATIENT_DETAIL            		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #
-- #	                                                                        	#
-- #
-- #
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_REG_CANCER_PATIENT_DETAIL;;
 --' FOR SESSION;;
 /* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_detail;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cancer_patient_detail AS mt USING
  (SELECT DISTINCT CAST(row_number() OVER (
                                           ORDER BY upper(met.patient_market_urn_text), met.network_mnemonic_cs) AS NUMERIC) AS cancer_patient_detail_sk,
                   coalesce(cp.cancer_patient_driver_sk, cpp.cancer_patient_driver_sk, cpn.cancer_patient_driver_sk) AS cancer_patient_driver_sk,
                   met.cr_patient_id AS cr_patient_id,
                   inav.nav_patient_id AS cn_patient_id,
                   cpid.patient_dw_id AS cp_patient_id,
                   coalesce(met.coid, cpid.coid, inav.coid) AS coid,
                   coalesce(met.company_code, cpid.company_code, inav.company_code) AS company_code,
                   coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs) AS network_mnemonic_cs,
                   substr(coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text), 1, 30) AS patient_market_urn_text,
                   coalesce(met.medical_record_num, cpid.medical_record_num, inav.medical_record_num) AS medical_record_num,
                   substr(met.relation_name, 1, 50) AS relationship_name,
                   coalesce(met.address_line_1_text, cpid.address_line_1_text, inav.address_line_1_text) AS address_line_1_text,
                   coalesce(met.address_line_2_text, cpid.address_line_2_text, inav.address_line_2_text) AS address_line_2_text,
                   coalesce(met.city_name, cpid.city_name, inav.city_name) AS city_name,
                   inav.death_date,
                   coalesce(met.patient_birth_date, cpid.patient_birth_date, inav.patient_birth_date) AS patient_birth_date,
                   coalesce(met.patient_email_address_text, inav.patient_email_address_text) AS patient_email_address_text,
                   coalesce(met.gender_code, cpid.gender_code, inav.gender_code) AS gender_code,
                   coalesce(met.state_code, cpid.state_code, inav.state_code) AS state_code,
                   coalesce(met.zip_code, cpid.zip_code, inav.zip_code) AS zip_code,
                   coalesce(met.phone_num_type_code, inav.phone_num_type_code) AS phone_num_type_code,
                   coalesce(met.phone_num, inav.phone_num) AS phone_num,
                   inav.preferred_language_text, -- -INAV.PREFERRED_NAME,
 substr(met.insurance_type_name, 1, 50) AS insurance_type_name,
 met.preferred_contact_method_text,
 substr(met.insurance_company_name, 1, 50) AS insurance_company_name,
 substr(met.race_name, 1, 50) AS race_name,
 met.patient_system_id,
 substr(met.vital_status_name, 1, 20) AS vital_status_name,
 coalesce(met.source_system_code, cpid.source_system_code, inav.source_system_code) AS source_system_code,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT cp_0.patient_market_urn_text,
                      cp_0.cr_patient_id,
                      cp_0.coid,
                      cp_0.company_code,
                      cp_0.medical_record_num,
                      rel.lookup_desc AS relation_name,
                      df.network_mnemonic_cs,
                      df.facility_mnemonic_cs,
                      cppn.phone_num_type_code AS phone_num_type_code,
                      cppn.phone_num AS phone_num,
                      cpa.address_line_1_text AS address_line_1_text,
                      cpa.address_line_2_text AS address_line_2_text,
                      cpa.city_name AS city_name,
                      cp_0.patient_birth_date AS patient_birth_date,
                      cp_0.patient_email_address_text AS patient_email_address_text,
                      cp_0.patient_first_name AS patient_first_name,
                      CASE upper(rtrim(gn.lookup_desc))
                          WHEN 'MALE' THEN 'M'
                          WHEN 'FEMALE' THEN 'F'
                          ELSE 'U'
                      END AS gender_code,
                      cp_0.patient_last_name AS patient_last_name,
                      cp_0.patient_middle_name AS patient_middle_name,
                      st.lookup_code AS state_code,
                      cpa.zip_code AS zip_code,
                      ici.lookup_desc AS insurance_type_name,
                      cpc.preferred_contact_method_text AS preferred_contact_method_text,
                      iti.lookup_desc AS insurance_company_name,
                      rc.lookup_desc AS race_name,
                      cp_0.patient_system_id AS patient_system_id,
                      vs.lookup_desc AS vital_status_name,
                      cp_0.source_system_code
      FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient AS cp_0
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM {{ params.param_cr_auth_base_views_dataset_name }}.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                                              ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df ON upper(rtrim(cp_0.coid)) = upper(rtrim(df.coid))
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_address AS cpa ON cp_0.cr_patient_id = cpa.cr_patient_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_contact AS cpc ON cp_0.cr_patient_id = cpc.cr_patient_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_phone_num AS cppn ON cp_0.cr_patient_id = cppn.cr_patient_id
      AND cpc.patient_contact_id = cppn.patient_contact_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_insurance AS cpi ON cp_0.cr_patient_id = cpi.cr_patient_id
      LEFT OUTER JOIN
        (SELECT ref_lookup_code.master_lookup_sid,
                ref_lookup_code.lookup_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
         WHERE ref_lookup_code.lookup_id = 1 ) AS vs ON cp_0.vital_status_id = vs.master_lookup_sid
      LEFT OUTER JOIN
        (SELECT ref_lookup_code.master_lookup_sid,
                ref_lookup_code.lookup_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
         WHERE ref_lookup_code.lookup_id = 2 ) AS rc ON cp_0.patient_race_id = rc.master_lookup_sid
      LEFT OUTER JOIN
        (SELECT ref_lookup_code.master_lookup_sid,
                ref_lookup_code.lookup_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
         WHERE ref_lookup_code.lookup_id = 3 ) AS gn ON cp_0.patient_gender_id = gn.master_lookup_sid
      LEFT OUTER JOIN
        (SELECT ref_lookup_code.master_lookup_sid,
                ref_lookup_code.lookup_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
         WHERE ref_lookup_code.lookup_id = 4 ) AS rel ON cpc.contact_relation_id = rel.master_lookup_sid
      LEFT OUTER JOIN
        (SELECT ref_lookup_code.master_lookup_sid,
                ref_lookup_code.lookup_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
         WHERE ref_lookup_code.lookup_id = 13 ) AS iti ON cpi.insurance_type_id = iti.master_lookup_sid
      LEFT OUTER JOIN
        (SELECT ref_lookup_code.master_lookup_sid,
                ref_lookup_code.lookup_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
         WHERE ref_lookup_code.lookup_id = 14 ) AS ici ON cpi.insurance_company_id = ici.master_lookup_sid
      LEFT OUTER JOIN
        (SELECT ref_lookup_code.master_lookup_sid,
                ref_lookup_code.lookup_code,
                ref_lookup_code.lookup_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
         WHERE ref_lookup_code.lookup_id = 23 ) AS st ON cpa.state_id = st.master_lookup_sid) AS met
   FULL OUTER JOIN
     (SELECT crio.patient_market_urn,
             crio.network_mnemonic_cs,
             crio.patient_dw_id,
             crio.coid,
             crio.medical_record_num,
             crio.address_line_1_text,
             df1.facility_mnemonic_cs,
             crio.company_code,
             crio.address_line_2_text,
             crio.city_name,
             crio.birth_date AS patient_birth_date,
             crio.first_name AS patient_first_name,
             crio.gender_code,
             crio.last_name AS patient_last_name,
             crio.state_code,
             crio.zip_code,
             crio.source_system_code
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS crio
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM {{ params.param_cr_auth_base_views_dataset_name }}.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                                              ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df1 ON upper(rtrim(crio.coid)) = upper(rtrim(df1.coid)) QUALIFY row_number() OVER (PARTITION BY coalesce(crio.patient_market_urn, concat(crio.medical_record_num, df1.facility_mnemonic_cs), format('%#20.0f', crio.patient_dw_id))
                                                                                                                                                                                                                                                                     ORDER BY crio.report_assigned_date_time DESC) = 1) AS cpid ON rtrim(coalesce(met.patient_market_urn_text, concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(cpid.patient_market_urn, concat(cpid.medical_record_num, cpid.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(cpid.network_mnemonic_cs)
   FULL OUTER JOIN
     (SELECT cp_0.nav_patient_id,
             cpt.patient_market_urn,
             df2.network_mnemonic_cs,
             cpt.coid,
             df2.facility_mnemonic_cs,
             cpt.company_code,
             cpt.empi_text,
             cpt.medical_record_num,
             cpa.address_line_1_text,
             cpa.address_line_2_text,
             cpa.city_name,
             cp_0.death_date,
             cp_0.birth_date AS patient_birth_date,
             cp_0.patient_email_text AS patient_email_address_text,
             cp_0.first_name AS patient_first_name,
             cp_0.gender_code,
             cp_0.last_name AS patient_last_name,
             cp_0.middle_name AS patient_middle_name,
             cpp_0.phone_num,
             cpp_0.phone_num_type_code,
             cp_0.preferred_language_text,
             cp_0.preferred_name,
             cpa.state_code,
             cpa.zip_code,
             cp_0.source_system_code
      FROM {{ params.param_cr_base_views_dataset_name }}.cn_person AS cp_0
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient AS cpt ON cp_0.nav_patient_id = cpt.nav_patient_id
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM {{ params.param_cr_auth_base_views_dataset_name }}.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                                              ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df2 ON upper(rtrim(cpt.coid)) = upper(rtrim(df2.coid))
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_address AS cpa ON cp_0.nav_patient_id = cpa.nav_patient_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_phone_num AS cpp_0 ON cp_0.nav_patient_id = cpp_0.nav_patient_id QUALIFY row_number() OVER (PARTITION BY coalesce(cpt.patient_market_urn, concat(cpt.medical_record_num, df2.facility_mnemonic_cs), format('%#20.0f', cp_0.nav_patient_id))
                                                                                                                                                                           ORDER BY cp_0.nav_patient_id DESC) = 1) AS inav ON rtrim(coalesce(concat(trim(met.network_mnemonic_cs), met.patient_market_urn_text), concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(inav.patient_market_urn, concat(inav.medical_record_num, inav.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(inav.network_mnemonic_cs)
   LEFT OUTER JOIN
     (SELECT cancer_patient_driver.cr_patient_id,
             cancer_patient_driver.cancer_patient_driver_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cr_patient_id
                                                                                                          ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cp ON met.cr_patient_id = cp.cr_patient_id
   LEFT OUTER JOIN
     (SELECT cancer_patient_driver.cn_patient_id,
             cancer_patient_driver.cancer_patient_driver_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cn_patient_id
                                                                                                          ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpn ON inav.nav_patient_id = cpn.cn_patient_id
   LEFT OUTER JOIN
     (SELECT cancer_patient_driver.cp_patient_id,
             cancer_patient_driver.cancer_patient_driver_sk
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cp_patient_id
                                                                                                          ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpp ON cpid.patient_dw_id = cpp.cp_patient_id) AS ms ON mt.cancer_patient_detail_sk = ms.cancer_patient_detail_sk
AND (coalesce(mt.cancer_patient_driver_sk, NUMERIC '0') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '0')
     AND coalesce(mt.cancer_patient_driver_sk, NUMERIC '1') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '1'))
AND (coalesce(mt.cr_patient_id, 0) = coalesce(ms.cr_patient_id, 0)
     AND coalesce(mt.cr_patient_id, 1) = coalesce(ms.cr_patient_id, 1))
AND (coalesce(mt.cn_patient_id, NUMERIC '0') = coalesce(ms.cn_patient_id, NUMERIC '0')
     AND coalesce(mt.cn_patient_id, NUMERIC '1') = coalesce(ms.cn_patient_id, NUMERIC '1'))
AND (coalesce(mt.cp_patient_id, NUMERIC '0') = coalesce(ms.cp_patient_id, NUMERIC '0')
     AND coalesce(mt.cp_patient_id, NUMERIC '1') = coalesce(ms.cp_patient_id, NUMERIC '1'))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.network_mnemonic_cs, '0') = coalesce(ms.network_mnemonic_cs, '0')
     AND coalesce(mt.network_mnemonic_cs, '1') = coalesce(ms.network_mnemonic_cs, '1'))
AND (upper(coalesce(mt.patient_market_urn_text, '0')) = upper(coalesce(ms.patient_market_urn_text, '0'))
     AND upper(coalesce(mt.patient_market_urn_text, '1')) = upper(coalesce(ms.patient_market_urn_text, '1')))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND (upper(coalesce(mt.relationship_name, '0')) = upper(coalesce(ms.relationship_name, '0'))
     AND upper(coalesce(mt.relationship_name, '1')) = upper(coalesce(ms.relationship_name, '1')))
AND (upper(coalesce(mt.address_line_1_text, '0')) = upper(coalesce(ms.address_line_1_text, '0'))
     AND upper(coalesce(mt.address_line_1_text, '1')) = upper(coalesce(ms.address_line_1_text, '1')))
AND (upper(coalesce(mt.address_line_2_text, '0')) = upper(coalesce(ms.address_line_2_text, '0'))
     AND upper(coalesce(mt.address_line_2_text, '1')) = upper(coalesce(ms.address_line_2_text, '1')))
AND (upper(coalesce(mt.city_name, '0')) = upper(coalesce(ms.city_name, '0'))
     AND upper(coalesce(mt.city_name, '1')) = upper(coalesce(ms.city_name, '1')))
AND (coalesce(mt.death_date, DATE '1970-01-01') = coalesce(ms.death_date, DATE '1970-01-01')
     AND coalesce(mt.death_date, DATE '1970-01-02') = coalesce(ms.death_date, DATE '1970-01-02'))
AND (coalesce(mt.patient_birth_date, DATE '1970-01-01') = coalesce(ms.patient_birth_date, DATE '1970-01-01')
     AND coalesce(mt.patient_birth_date, DATE '1970-01-02') = coalesce(ms.patient_birth_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.patient_email_address_text, '0')) = upper(coalesce(ms.patient_email_address_text, '0'))
     AND upper(coalesce(mt.patient_email_address_text, '1')) = upper(coalesce(ms.patient_email_address_text, '1')))
AND (upper(coalesce(mt.patient_gender_code, '0')) = upper(coalesce(ms.gender_code, '0'))
     AND upper(coalesce(mt.patient_gender_code, '1')) = upper(coalesce(ms.gender_code, '1')))
AND (upper(coalesce(mt.patient_state_code, '0')) = upper(coalesce(ms.state_code, '0'))
     AND upper(coalesce(mt.patient_state_code, '1')) = upper(coalesce(ms.state_code, '1')))
AND (upper(coalesce(mt.zip_code, '0')) = upper(coalesce(ms.zip_code, '0'))
     AND upper(coalesce(mt.zip_code, '1')) = upper(coalesce(ms.zip_code, '1')))
AND (upper(coalesce(mt.phone_num_type_code, '0')) = upper(coalesce(ms.phone_num_type_code, '0'))
     AND upper(coalesce(mt.phone_num_type_code, '1')) = upper(coalesce(ms.phone_num_type_code, '1')))
AND (upper(coalesce(mt.phone_num, '0')) = upper(coalesce(ms.phone_num, '0'))
     AND upper(coalesce(mt.phone_num, '1')) = upper(coalesce(ms.phone_num, '1')))
AND (upper(coalesce(mt.preferred_language_text, '0')) = upper(coalesce(ms.preferred_language_text, '0'))
     AND upper(coalesce(mt.preferred_language_text, '1')) = upper(coalesce(ms.preferred_language_text, '1')))
AND (upper(coalesce(mt.insurance_type_name, '0')) = upper(coalesce(ms.insurance_type_name, '0'))
     AND upper(coalesce(mt.insurance_type_name, '1')) = upper(coalesce(ms.insurance_type_name, '1')))
AND (upper(coalesce(mt.preferred_contact_method_text, '0')) = upper(coalesce(ms.preferred_contact_method_text, '0'))
     AND upper(coalesce(mt.preferred_contact_method_text, '1')) = upper(coalesce(ms.preferred_contact_method_text, '1')))
AND (upper(coalesce(mt.insurance_company_name, '0')) = upper(coalesce(ms.insurance_company_name, '0'))
     AND upper(coalesce(mt.insurance_company_name, '1')) = upper(coalesce(ms.insurance_company_name, '1')))
AND (upper(coalesce(mt.race_name, '0')) = upper(coalesce(ms.race_name, '0'))
     AND upper(coalesce(mt.race_name, '1')) = upper(coalesce(ms.race_name, '1')))
AND (coalesce(mt.patient_system_id, 0) = coalesce(ms.patient_system_id, 0)
     AND coalesce(mt.patient_system_id, 1) = coalesce(ms.patient_system_id, 1))
AND (upper(coalesce(mt.vital_status_name, '0')) = upper(coalesce(ms.vital_status_name, '0'))
     AND upper(coalesce(mt.vital_status_name, '1')) = upper(coalesce(ms.vital_status_name, '1')))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_patient_detail_sk,
        cancer_patient_driver_sk,
        cr_patient_id,
        cn_patient_id,
        cp_patient_id,
        coid,
        company_code,
        network_mnemonic_cs,
        patient_market_urn_text,
        medical_record_num,
        relationship_name,
        address_line_1_text,
        address_line_2_text,
        city_name,
        death_date,
        patient_birth_date,
        patient_email_address_text,
        patient_gender_code,
        patient_state_code,
        zip_code,
        phone_num_type_code,
        phone_num,
        preferred_language_text,
        insurance_type_name,
        preferred_contact_method_text,
        insurance_company_name,
        race_name,
        patient_system_id,
        vital_status_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cancer_patient_detail_sk, ms.cancer_patient_driver_sk, ms.cr_patient_id, ms.cn_patient_id, ms.cp_patient_id, ms.coid, ms.company_code, ms.network_mnemonic_cs, ms.patient_market_urn_text, ms.medical_record_num, ms.relationship_name, ms.address_line_1_text, ms.address_line_2_text, ms.city_name, ms.death_date, ms.patient_birth_date, ms.patient_email_address_text, ms.gender_code, ms.state_code, ms.zip_code, ms.phone_num_type_code, ms.phone_num, ms.preferred_language_text, ms.insurance_type_name, ms.preferred_contact_method_text, ms.insurance_company_name, ms.race_name, ms.patient_system_id, ms.vital_status_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_patient_detail_sk
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_detail
      GROUP BY cancer_patient_detail_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cancer_patient_detail');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_cr_core_dataset_name }}.cancer_patient_detail AS cd1
SET cancer_patient_driver_sk = cp.cancer_patient_driver_sk
FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cp
WHERE upper(rtrim(cd1.medical_record_num)) = upper(rtrim(cp.medical_record_num))
  AND upper(rtrim(cd1.patient_market_urn_text)) = upper(rtrim(cp.patient_market_urn_text))
  AND cd1.cancer_patient_driver_sk IS NULL
  AND cp.cr_patient_id IS NULL
  AND cp.cn_patient_id IS NULL
  AND cp.cp_patient_id IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_cr_core_dataset_name }}.cancer_patient_detail AS cd1
SET cancer_patient_driver_sk = cp.cancer_patient_driver_sk
FROM
  (SELECT cpd.cp_patient_id,
          cp1.cancer_patient_driver_sk,
          cp1.medical_record_num,
          cp1.patient_market_urn_text
   FROM
     (SELECT cancer_patient_driver.cp_patient_id,
             count(*) AS cnt
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver
      WHERE cancer_patient_driver.cn_patient_id IS NULL
        AND cancer_patient_driver.cr_patient_id IS NULL
      GROUP BY 1
      HAVING count(*) > 1) AS cpd
   INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cp1 ON cpd.cp_patient_id = cp1.cp_patient_id) AS cp
WHERE upper(rtrim(cd1.medical_record_num)) = upper(rtrim(cp.medical_record_num))
  AND upper(rtrim(cd1.patient_market_urn_text)) = upper(rtrim(cp.patient_market_urn_text));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CANCER_PATIENT_DETAIL');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF