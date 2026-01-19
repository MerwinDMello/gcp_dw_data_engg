DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_date_driver.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_Patient_Date_Driver;;
 --' FOR SESSION;;
 BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE sc AS
SELECT emr_date.*,
       emr_date.edwcr_date_driver - INTERVAL 90 DAY AS hospemr_date_driver
FROM
  (SELECT info.cancer_patient_driver_sk,
          info.network_mnemonic_cs,
          info.patient_market_urn_text,
          info.medical_record_num,
          info.empi_text,
          info.coid,
          max(info.datesource) AS datesource,
          min(info.date_driver) AS edwcr_date_driver
   FROM
     (SELECT dates.cancer_patient_driver_sk,
             dates.network_mnemonic_cs,
             dates.patient_market_urn_text,
             min(dates.datesourceorder) AS mindatesource
      FROM
        (SELECT t2.cancer_patient_driver_sk,
                t2.network_mnemonic_cs,
                t2.patient_market_urn_text,
                t2.medical_record_num,
                t2.empi_text,
                'Patient_ID' AS datesource,
                3 AS datesourceorder,
                min(t1.user_action_date_time) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t2 ON t1.patient_dw_id = t2.cp_patient_id
         WHERE upper(rtrim(t1.user_action_desc)) = 'CONFIRM'
           AND upper(rtrim(t1.message_flag_code)) <> 'RAD'
         GROUP BY 1,
                  2,
                  3,
                  4,
                  5
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                               t1.network_mnemonic_cs AS network_mnemonic_cs,
                               t1.patient_market_urn_text AS patient_market_urn_text,
                               t1.medical_record_num AS medical_record_num,
                               t1.empi_text AS empi_text,
                               'Nav_Diagno' AS datesource,
                               2 AS datesourceorder,
                               min(diagnosis_date) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_diagnosis AS t3 ON t2.nav_patient_id = t3.nav_patient_id
         WHERE diagnosis_date > '1900-01-01'
         GROUP BY 1,
                  2,
                  3,
                  4,
                  5
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                               t1.network_mnemonic_cs AS network_mnemonic_cs,
                               t1.patient_market_urn_text AS patient_market_urn_text,
                               t1.medical_record_num AS medical_record_num,
                               t1.empi_text AS empi_text,
                               'NAV_CREATE' AS datesource,
                               4 AS datesourceorder,
                               min(nav_create_date) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
         WHERE nav_create_date > '1900-01-01'
         GROUP BY 1,
                  2,
                  3,
                  4,
                  5
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                               t1.network_mnemonic_cs AS network_mnemonic_cs,
                               t1.patient_market_urn_text AS patient_market_urn_text,
                               t1.medical_record_num AS medical_record_num,
                               t1.empi_text AS empi_text,
                               'CR_DIAGNOS' AS datesource,
                               1 AS datesourceorder,
                               min(diagnosis_date) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_diagnosis_detail AS t2 ON t1.cr_patient_id = t2.cr_patient_id
         AND diagnosis_date IS NOT NULL
         WHERE diagnosis_date > '1900-01-01'
         GROUP BY 1,
                  2,
                  3,
                  4,
                  5
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                               t1.network_mnemonic_cs AS network_mnemonic_cs,
                               t1.patient_market_urn_text AS patient_market_urn_text,
                               t1.medical_record_num AS medical_record_num,
                               t1.empi_text AS empi_text,
                               'CR_ADMISSI' AS datesource,
                               5 AS datesourceorder,
                               min(admission_date) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS t2 ON t1.cr_patient_id = t2.cr_patient_id
         AND admission_date IS NOT NULL
         WHERE admission_date > '1900-01-01'
         GROUP BY 1,
                  2,
                  3,
                  4,
                  5) AS dates
      GROUP BY 1,
               2,
               3) AS date_source
   INNER JOIN -- ---- Date Driver ------

     (SELECT t2.cancer_patient_driver_sk,
             t2.network_mnemonic_cs,
             t2.patient_market_urn_text,
             t2.medical_record_num,
             t2.empi_text,
             t2.coid,
             'Patient_ID' AS datesource,
             3 AS datesourceorder,
             min(t1.user_action_date_time) AS date_driver
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS t1
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t2 ON t1.patient_dw_id = t2.cp_patient_id
      WHERE upper(rtrim(t1.user_action_desc)) = 'CONFIRM'
        AND upper(rtrim(t1.message_flag_code)) <> 'RAD'
      GROUP BY 1,
               2,
               3,
               4,
               5,
               6
      UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                            t1.network_mnemonic_cs AS network_mnemonic_cs,
                            t1.patient_market_urn_text AS patient_market_urn_text,
                            t1.medical_record_num AS medical_record_num,
                            t1.empi_text AS empi_text,
                            t1.coid AS coid,
                            'Nav_Diagno' AS datesource,
                            2 AS datesourceorder,
                            min(diagnosis_date) AS date_driver
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_diagnosis AS t3 ON t2.nav_patient_id = t3.nav_patient_id
      WHERE diagnosis_date > '1900-01-01'
      GROUP BY 1,
               2,
               3,
               4,
               5,
               6
      UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                            t1.network_mnemonic_cs AS network_mnemonic_cs,
                            t1.patient_market_urn_text AS patient_market_urn_text,
                            t1.medical_record_num AS medical_record_num,
                            t1.empi_text AS empi_text,
                            t1.coid AS coid,
                            'Nav_Create' AS datesource,
                            4 AS datesourceorder,
                            min(nav_create_date) AS date_driver
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
      WHERE nav_create_date > '1900-01-01'
      GROUP BY 1,
               2,
               3,
               4,
               5,
               6
      UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                            t1.network_mnemonic_cs AS network_mnemonic_cs,
                            t1.patient_market_urn_text AS patient_market_urn_text,
                            t1.medical_record_num AS medical_record_num,
                            t1.empi_text AS empi_text,
                            t1.coid AS coid,
                            'CR_Diagnos' AS datesource,
                            1 AS datesourceorder,
                            min(diagnosis_date) AS date_driver
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_diagnosis_detail AS t2 ON t1.cr_patient_id = t2.cr_patient_id
      AND diagnosis_date IS NOT NULL
      WHERE diagnosis_date > '1900-01-01'
      GROUP BY 1,
               2,
               3,
               4,
               5,
               6
      UNION DISTINCT SELECT t1.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                            t1.network_mnemonic_cs AS network_mnemonic_cs,
                            t1.patient_market_urn_text AS patient_market_urn_text,
                            t1.medical_record_num AS medical_record_num,
                            t1.empi_text AS empi_text,
                            t1.coid AS coid,
                            'CR_Admissi' AS datesource,
                            5 AS datesourceorder,
                            min(admission_date) AS date_driver
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS t2 ON t1.cr_patient_id = t2.cr_patient_id
      AND admission_date IS NOT NULL
      WHERE admission_date > '1900-01-01'
      GROUP BY 1,
               2,
               3,
               4,
               5,
               6) AS info ON date_source.cancer_patient_driver_sk = info.cancer_patient_driver_sk
   AND date_source.mindatesource = info.datesourceorder
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            upper(info.datesource)) AS emr_date;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_date_driver;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_date_driver AS mt USING
  (SELECT DISTINCT a.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                   a.patient_dw_id,
                   DATE(a.edwcr_date_driver) AS cancer_diagnosis_date,
                   DATE(a.hospemr_date_driver) AS cancer_diagnosis_90_day_prior_date,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT fe.patient_dw_id,
             fe.patient_market_urn,
             fe.medical_record_num,
             sc.cancer_patient_driver_sk,
             fe.admission_date_time,
             sc.edwcr_date_driver,
             sc.hospemr_date_driver
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.fact_encounter AS fe
      INNER JOIN sc ON upper(rtrim(fe.medical_record_num)) = upper(rtrim(sc.medical_record_num))
      AND upper(rtrim(fe.coid)) = upper(rtrim(sc.coid))
      WHERE fe.admission_date_time >= sc.hospemr_date_driver ) AS a) AS ms ON mt.cancer_patient_driver_sk = ms.cancer_patient_driver_sk
AND mt.patient_dw_id = ms.patient_dw_id
AND (coalesce(mt.cancer_diagnosis_date, DATE '1970-01-01') = coalesce(ms.cancer_diagnosis_date, DATE '1970-01-01')
     AND coalesce(mt.cancer_diagnosis_date, DATE '1970-01-02') = coalesce(ms.cancer_diagnosis_date, DATE '1970-01-02'))
AND (coalesce(mt.cancer_diagnosis_90_day_prior_date, DATE '1970-01-01') = coalesce(ms.cancer_diagnosis_90_day_prior_date, DATE '1970-01-01')
     AND coalesce(mt.cancer_diagnosis_90_day_prior_date, DATE '1970-01-02') = coalesce(ms.cancer_diagnosis_90_day_prior_date, DATE '1970-01-02'))
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_patient_driver_sk,
        patient_dw_id,
        cancer_diagnosis_date,
        cancer_diagnosis_90_day_prior_date,
        dw_last_update_date_time)
VALUES (ms.cancer_patient_driver_sk, ms.patient_dw_id, ms.cancer_diagnosis_date, ms.cancer_diagnosis_90_day_prior_date, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_patient_driver_sk,
             patient_dw_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_date_driver
      GROUP BY cancer_patient_driver_sk,
               patient_dw_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_date_driver');

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

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_date_driver AS mt USING
  (SELECT DISTINCT b.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                   b.patient_dw_id,
                   DATE(b.edwcr_date_driver) AS cancer_diagnosis_date,
                   DATE(b.hospemr_date_driver) AS cancer_diagnosis_90_day_prior_date,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT fe.patient_dw_id,
             fe.patient_market_urn,
             fe.medical_record_num,
             sc.cancer_patient_driver_sk,
             fe.admission_date_time,
             sc.edwcr_date_driver,
             sc.hospemr_date_driver
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.fact_encounter AS fe
      INNER JOIN `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility AS cf ON upper(rtrim(fe.coid)) = upper(rtrim(cf.coid))
      INNER JOIN --  AND CF.Facility_Active_Ind = 'Y'
 sc ON upper(rtrim(fe.patient_market_urn)) = upper(rtrim(sc.patient_market_urn_text))
      AND rtrim(cf.network_mnemonic_cs) = rtrim(sc.network_mnemonic_cs)
      WHERE fe.admission_date_time >= sc.hospemr_date_driver QUALIFY row_number() OVER (PARTITION BY sc.cancer_patient_driver_sk,
                                                                                                     fe.patient_dw_id
                                                                                        ORDER BY upper(cf.facility_active_ind) DESC) = 1 ) AS b
   WHERE (b.patient_dw_id,
          b.cancer_patient_driver_sk) NOT IN
       (SELECT AS STRUCT cr_patient_date_driver.patient_dw_id,
                         cr_patient_date_driver.cancer_patient_driver_sk
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_date_driver) ) AS ms ON mt.cancer_patient_driver_sk = ms.cancer_patient_driver_sk
AND mt.patient_dw_id = ms.patient_dw_id
AND (coalesce(mt.cancer_diagnosis_date, DATE '1970-01-01') = coalesce(ms.cancer_diagnosis_date, DATE '1970-01-01')
     AND coalesce(mt.cancer_diagnosis_date, DATE '1970-01-02') = coalesce(ms.cancer_diagnosis_date, DATE '1970-01-02'))
AND (coalesce(mt.cancer_diagnosis_90_day_prior_date, DATE '1970-01-01') = coalesce(ms.cancer_diagnosis_90_day_prior_date, DATE '1970-01-01')
     AND coalesce(mt.cancer_diagnosis_90_day_prior_date, DATE '1970-01-02') = coalesce(ms.cancer_diagnosis_90_day_prior_date, DATE '1970-01-02'))
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_patient_driver_sk,
        patient_dw_id,
        cancer_diagnosis_date,
        cancer_diagnosis_90_day_prior_date,
        dw_last_update_date_time)
VALUES (ms.cancer_patient_driver_sk, ms.patient_dw_id, ms.cancer_diagnosis_date, ms.cancer_diagnosis_90_day_prior_date, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_patient_driver_sk,
             patient_dw_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_date_driver
      GROUP BY cancer_patient_driver_sk,
               patient_dw_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_date_driver');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;