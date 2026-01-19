DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_reg_cancer_patient_driver.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_DIAGNOSIS_DETAIL             		#
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
 --Job=J_CN_REG_CANCER_PATIENT_DRIVER;;
 --' FOR SESSION;;
 /* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS mt USING
  (SELECT DISTINCT CAST(row_number() OVER (
                                           ORDER BY met.cr_patient_id, inav.nav_patient_id, cpid.patient_dw_id) AS NUMERIC) AS cancer_patient_driver_sk,
                   met.cr_patient_id AS cr_patient_id,
                   inav.nav_patient_id AS cn_patient_id,
                   cpid.patient_dw_id AS cp_patient_id,
                   coid1 AS coid1,
                   coalesce(met.company_code, cpid.company_code, inav.company_code) AS company_code,
                   coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs) AS network_mnemonic_cs,
                   substr(coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text), 1, 30) AS patient_market_urn_text,
                   coalesce(met.medical_record_num, cpid.medical_record_num, inav.medical_record_num) AS medical_record_num,
                   empi.empi_text,
                   coalesce(met.patient_first_name, cpid.patient_first_name, inav.patient_first_name) AS patient_first_name,
                   coalesce(met.patient_middle_name, inav.patient_middle_name) AS patient_middle_name,
                   coalesce(met.patient_last_name, cpid.patient_last_name, inav.patient_last_name) AS patient_last_name,
                   inav.preferred_name,
                   coalesce(met.source_system_code, cpid.source_system_code, inav.source_system_code) AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT cp.patient_market_urn_text,
             cp.cr_patient_id,
             cp.coid,
             cp.company_code,
             cp.medical_record_num,
             df.facility_mnemonic_cs,
             df.network_mnemonic_cs,
             cp.patient_first_name AS patient_first_name,
             cp.patient_last_name AS patient_last_name,
             cp.patient_middle_name AS patient_middle_name,
             cp.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient AS cp
      LEFT OUTER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df ON upper(rtrim(cp.coid)) = upper(rtrim(df.coid))) AS met
   FULL OUTER JOIN
     (SELECT crio.patient_market_urn,
             crio.network_mnemonic_cs,
             crio.medical_record_num,
             crio.patient_dw_id, /* Handled null coid in CRIO */ coalesce(crio.coid, df2.coid) AS coid,
                                                                 crio.company_code,
                                                                 crio.first_name AS patient_first_name,
                                                                 crio.last_name AS patient_last_name,
                                                                 coalesce(df1.facility_mnemonic_cs, crio.facility_menmonic_cs) AS facility_mnemonic_cs,
                                                                 crio.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS crio
      LEFT OUTER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df1 ON upper(rtrim(crio.coid)) = upper(rtrim(df1.coid))
      LEFT OUTER JOIN /* Handled null coid in CRIO by using below join of Df2*/
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edwcr_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY clinical_facility.facility_mnemonic_cs
                                                                                             ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df2 ON rtrim(crio.facility_menmonic_cs) = rtrim(df2.facility_mnemonic_cs) QUALIFY row_number() OVER (PARTITION BY coalesce(concat(crio.patient_market_urn, crio.network_mnemonic_cs), concat(crio.medical_record_num, facility_mnemonic_cs), format('%#20.0f', crio.patient_dw_id))
                                                                                                                                                                                                                                                                      ORDER BY crio.report_assigned_date_time DESC) = 1) AS cpid ON rtrim(coalesce(met.patient_market_urn_text, concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(cpid.patient_market_urn, concat(cpid.medical_record_num, cpid.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(cpid.network_mnemonic_cs)
   FULL OUTER JOIN
     (SELECT cp.nav_patient_id,
             cpt.patient_market_urn,
             cpt.network_mnemonic_cs,
             cpt.medical_record_num,
             cpt.company_code,
             cpt.empi_text,
             cpt.coid,
             df2.facility_mnemonic_cs,
             cp.first_name AS patient_first_name,
             cp.last_name AS patient_last_name,
             cp.middle_name AS patient_middle_name,
             cp.preferred_name,
             cp.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_person AS cp
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient AS cpt ON cp.nav_patient_id = cpt.nav_patient_id
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df2 ON upper(rtrim(cpt.coid)) = upper(rtrim(df2.coid))
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_address AS cpa ON cp.nav_patient_id = cpa.nav_patient_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_phone_num AS cpp ON cp.nav_patient_id = cpp.nav_patient_id QUALIFY row_number() OVER (PARTITION BY coalesce(cpt.patient_market_urn, concat(cpt.medical_record_num, df2.facility_mnemonic_cs), format('%#20.0f', cp.nav_patient_id))
                                                                                                                                                              ORDER BY cp.nav_patient_id DESC) = 1) AS inav ON rtrim(coalesce(concat(trim(met.network_mnemonic_cs), met.patient_market_urn_text), concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(inav.patient_market_urn, concat(inav.medical_record_num, inav.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(inav.network_mnemonic_cs)
   LEFT OUTER JOIN
     (SELECT a.patient_market_urn,
             a.network_mnemonic_cs,
             a.empi_text
      FROM
        (SELECT cr.patient_market_urn,
                cr.network_mnemonic_cs,
                e.empi_text
         FROM `hca-hin-dev-cur-ops`.edwcr_views.clinical_registration AS cr
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_views.empi_encounter_id_xwalk AS e ON cr.patient_dw_id = e.encounter_id
         WHERE upper(rtrim(e.encounter_id_type_name)) = 'PATIENT_DW_ID'
           AND CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(e.empi_text) AS FLOAT64) > 0 ) AS a QUALIFY row_number() OVER (PARTITION BY upper(a.patient_market_urn),
                                                                                                                                                        upper(a.network_mnemonic_cs)
                                                                                                                                           ORDER BY upper(a.empi_text)) = 1) AS empi ON rtrim(coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs)) = rtrim(empi.network_mnemonic_cs)
   AND upper(rtrim(coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text))) = upper(rtrim(empi.patient_market_urn))
   CROSS JOIN UNNEST(ARRAY[ coalesce(met.coid, cpid.coid, inav.coid) ]) AS coid1
   WHERE coid1 IS NOT NULL ) AS ms ON mt.cancer_patient_driver_sk = ms.cancer_patient_driver_sk
AND (coalesce(mt.cr_patient_id, 0) = coalesce(ms.cr_patient_id, 0)
     AND coalesce(mt.cr_patient_id, 1) = coalesce(ms.cr_patient_id, 1))
AND (coalesce(mt.cn_patient_id, NUMERIC '0') = coalesce(ms.cn_patient_id, NUMERIC '0')
     AND coalesce(mt.cn_patient_id, NUMERIC '1') = coalesce(ms.cn_patient_id, NUMERIC '1'))
AND (coalesce(mt.cp_patient_id, NUMERIC '0') = coalesce(ms.cp_patient_id, NUMERIC '0')
     AND coalesce(mt.cp_patient_id, NUMERIC '1') = coalesce(ms.cp_patient_id, NUMERIC '1'))
AND mt.coid = ms.coid1
AND mt.company_code = ms.company_code
AND (coalesce(mt.network_mnemonic_cs, '0') = coalesce(ms.network_mnemonic_cs, '0')
     AND coalesce(mt.network_mnemonic_cs, '1') = coalesce(ms.network_mnemonic_cs, '1'))
AND (upper(coalesce(mt.patient_market_urn_text, '0')) = upper(coalesce(ms.patient_market_urn_text, '0'))
     AND upper(coalesce(mt.patient_market_urn_text, '1')) = upper(coalesce(ms.patient_market_urn_text, '1')))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND (upper(coalesce(mt.empi_text, '0')) = upper(coalesce(ms.empi_text, '0'))
     AND upper(coalesce(mt.empi_text, '1')) = upper(coalesce(ms.empi_text, '1')))
AND (upper(coalesce(mt.patient_first_name, '0')) = upper(coalesce(ms.patient_first_name, '0'))
     AND upper(coalesce(mt.patient_first_name, '1')) = upper(coalesce(ms.patient_first_name, '1')))
AND (upper(coalesce(mt.patient_middle_name, '0')) = upper(coalesce(ms.patient_middle_name, '0'))
     AND upper(coalesce(mt.patient_middle_name, '1')) = upper(coalesce(ms.patient_middle_name, '1')))
AND (upper(coalesce(mt.patient_last_name, '0')) = upper(coalesce(ms.patient_last_name, '0'))
     AND upper(coalesce(mt.patient_last_name, '1')) = upper(coalesce(ms.patient_last_name, '1')))
AND (upper(coalesce(mt.preferred_name, '0')) = upper(coalesce(ms.preferred_name, '0'))
     AND upper(coalesce(mt.preferred_name, '1')) = upper(coalesce(ms.preferred_name, '1')))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_patient_driver_sk,
        cr_patient_id,
        cn_patient_id,
        cp_patient_id,
        coid,
        company_code,
        network_mnemonic_cs,
        patient_market_urn_text,
        medical_record_num,
        empi_text,
        patient_first_name,
        patient_middle_name,
        patient_last_name,
        preferred_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cancer_patient_driver_sk, ms.cr_patient_id, ms.cn_patient_id, ms.cp_patient_id, ms.coid1, ms.company_code, ms.network_mnemonic_cs, ms.patient_market_urn_text, ms.medical_record_num, ms.empi_text, ms.patient_first_name, ms.patient_middle_name, ms.patient_last_name, ms.preferred_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_patient_driver_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver
      GROUP BY cancer_patient_driver_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CANCER_PATIENT_DRIVER');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;