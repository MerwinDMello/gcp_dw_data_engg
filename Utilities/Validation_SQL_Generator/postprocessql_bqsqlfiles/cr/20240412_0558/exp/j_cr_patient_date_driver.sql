-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_patient_date_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 WITH sc AS
  (SELECT emr_date.*,
          emr_date.edwcr_date_driver - INTERVAL 90 DAY AS hospemr_date_driver
   FROM
     (SELECT info.cancer_patient_driver_sk,
             info.network_mnemonic_cs,
             max(info.patient_market_urn_text) AS patient_market_urn_text,
             max(info.medical_record_num) AS medical_record_num,
             max(info.empi_text) AS empi_text,
             max(info.coid) AS coid,
             max(info.datesource) AS datesource,
             min(info.date_driver) AS edwcr_date_driver
      FROM
        (SELECT dates.cancer_patient_driver_sk,
                dates.network_mnemonic_cs,
                max(dates.patient_market_urn_text) AS patient_market_urn_text,
                min(dates.datesourceorder) AS mindatesource
         FROM
           (SELECT t2.cancer_patient_driver_sk,
                   t2.network_mnemonic_cs,
                   max(t2.patient_market_urn_text) AS patient_market_urn_text,
                   max(t2.medical_record_num) AS medical_record_num,
                   max(t2.empi_text) AS empi_text,
                   'Patient_ID' AS datesource,
                   3 AS datesourceorder,
                   min(t1.user_action_date_time) AS date_driver
            FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS t1
            INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS t2 ON t1.patient_dw_id = t2.cp_patient_id
            WHERE upper(rtrim(t1.user_action_desc)) = 'CONFIRM'
              AND upper(rtrim(t1.message_flag_code)) <> 'RAD'
            GROUP BY 1,
                     2,
                     upper(t2.patient_market_urn_text),
                     upper(t2.medical_record_num),
                     upper(t2.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'Nav_Diagno' AS datesource,
                                  2 AS datesourceorder,
                                  CAST(min(t3.diagnosis_date) AS DATETIME) AS date_driver
            FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
            INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
            INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient_diagnosis AS t3 ON t2.nav_patient_id = t3.nav_patient_id
            WHERE t3.diagnosis_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'NAV_CREATE' AS datesource,
                                  4 AS datesourceorder,
                                  CAST(min(t2.nav_create_date) AS DATETIME) AS date_driver
            FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
            INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
            WHERE t2.nav_create_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'CR_DIAGNOS' AS datesource,
                                  1 AS datesourceorder,
                                  CAST(min(t2.diagnosis_date) AS DATETIME) AS date_driver
            FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
            INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cr_patient_diagnosis_detail AS t2 ON t1.cr_patient_id = t2.cr_patient_id
            AND t2.diagnosis_date IS NOT NULL
            WHERE t2.diagnosis_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)
            UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                                  t1.network_mnemonic_cs,
                                  max(t1.patient_market_urn_text) AS patient_market_urn_text,
                                  max(t1.medical_record_num) AS medical_record_num,
                                  max(t1.empi_text) AS empi_text,
                                  'CR_ADMISSI' AS datesource,
                                  5 AS datesourceorder,
                                  CAST(min(t2.admission_date) AS DATETIME) AS date_driver
            FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
            INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS t2 ON t1.cr_patient_id = t2.cr_patient_id
            AND t2.admission_date IS NOT NULL
            WHERE t2.admission_date > DATE '1900-01-01'
            GROUP BY 1,
                     2,
                     upper(t1.patient_market_urn_text),
                     upper(t1.medical_record_num),
                     upper(t1.empi_text)) AS dates
         GROUP BY 1,
                  2,
                  upper(dates.patient_market_urn_text)) AS date_source
      INNER JOIN -- ---- Date Driver ------

        (SELECT t2.cancer_patient_driver_sk,
                t2.network_mnemonic_cs,
                max(t2.patient_market_urn_text) AS patient_market_urn_text,
                max(t2.medical_record_num) AS medical_record_num,
                max(t2.empi_text) AS empi_text,
                max(t2.coid) AS coid,
                'Patient_ID' AS datesource,
                3 AS datesourceorder,
                min(t1.user_action_date_time) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS t2 ON t1.patient_dw_id = t2.cp_patient_id
         WHERE upper(rtrim(t1.user_action_desc)) = 'CONFIRM'
           AND upper(rtrim(t1.message_flag_code)) <> 'RAD'
         GROUP BY 1,
                  2,
                  upper(t2.patient_market_urn_text),
                  upper(t2.medical_record_num),
                  upper(t2.empi_text),
                  upper(t2.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'Nav_Diagno' AS datesource,
                               2 AS datesourceorder,
                               CAST(min(t3.diagnosis_date) AS DATETIME) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient_diagnosis AS t3 ON t2.nav_patient_id = t3.nav_patient_id
         WHERE t3.diagnosis_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'Nav_Create' AS datesource,
                               4 AS datesourceorder,
                               CAST(min(t2.nav_create_date) AS DATETIME) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient AS t2 ON t1.cn_patient_id = t2.nav_patient_id
         WHERE t2.nav_create_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'CR_Diagnos' AS datesource,
                               1 AS datesourceorder,
                               CAST(min(t2.diagnosis_date) AS DATETIME) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cr_patient_diagnosis_detail AS t2 ON t1.cr_patient_id = t2.cr_patient_id
         AND t2.diagnosis_date IS NOT NULL
         WHERE t2.diagnosis_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)
         UNION DISTINCT SELECT t1.cancer_patient_driver_sk,
                               t1.network_mnemonic_cs,
                               max(t1.patient_market_urn_text) AS patient_market_urn_text,
                               max(t1.medical_record_num) AS medical_record_num,
                               max(t1.empi_text) AS empi_text,
                               max(t1.coid) AS coid,
                               'CR_Admissi' AS datesource,
                               5 AS datesourceorder,
                               CAST(min(t2.admission_date) AS DATETIME) AS date_driver
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS t1
         INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor AS t2 ON t1.cr_patient_id = t2.cr_patient_id
         AND t2.admission_date IS NOT NULL
         WHERE t2.admission_date > DATE '1900-01-01'
         GROUP BY 1,
                  2,
                  upper(t1.patient_market_urn_text),
                  upper(t1.medical_record_num),
                  upper(t1.empi_text),
                  upper(t1.coid)) AS info ON date_source.cancer_patient_driver_sk = info.cancer_patient_driver_sk
      AND date_source.mindatesource = info.datesourceorder
      GROUP BY 1,
               2,
               upper(info.patient_market_urn_text),
               upper(info.medical_record_num),
               upper(info.empi_text),
               upper(info.coid),
               upper(info.datesource)) AS emr_date)
SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT a.cancer_patient_driver_sk,
          a.patient_dw_id,
          a.edwcr_date_driver AS cancer_diagnosis_date,
          a.hospemr_date_driver AS cancer_diagnosis_90_day_prior_date,
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
      WHERE fe.admission_date_time >= sc.hospemr_date_driver ) AS a
   UNION ALL SELECT b.cancer_patient_driver_sk,
                    b.patient_dw_id,
                    b.edwcr_date_driver AS cancer_diagnosis_date,
                    b.hospemr_date_driver AS cancer_diagnosis_90_day_prior_date,
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
      AND upper(rtrim(cf.facility_active_ind)) = 'Y'
      INNER JOIN sc ON upper(rtrim(fe.patient_market_urn)) = upper(rtrim(sc.patient_market_urn_text))
      AND rtrim(cf.network_mnemonic_cs) = rtrim(sc.network_mnemonic_cs)
      WHERE fe.admission_date_time >= sc.hospemr_date_driver ) AS b
   WHERE b.patient_dw_id + b.cancer_patient_driver_sk NOT IN
       (SELECT a.patient_dw_id + a.cancer_patient_driver_sk
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
           WHERE fe.admission_date_time >= sc.hospemr_date_driver ) AS a) ) AS a