-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_fact_rad_onc_course_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY dp.dimsiteid, dp.factcoursediagnosisid DESC) AS fact_course_diagnosis_sk,
          ra.fact_patient_diagnosis_sk AS fact_patient_diagnosis_sk,
          ra1.patient_course_sk AS patient_course_sk,
          dc.diagnosis_code_sk AS diagnosis_code_sk,
          rr.site_sk AS site_sk,
          dp.factcoursediagnosisid AS source_fact_course_diagnosis_id,
          td_sysfnlib.decode(dp.isprimary, 1, 'Y', 0, 'N') AS primary_course_ind,
          dp.logid AS log_id,
          dp.runid AS run_id,
          'R' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                stg_factcoursediagnosis.factcoursediagnosisid,
                stg_factcoursediagnosis.factpatientdiagnosisid,
                stg_factcoursediagnosis.dimcourseid,
                stg_factcoursediagnosis.dimdiagnosiscodeid,
                stg_factcoursediagnosis.dimsiteid,
                stg_factcoursediagnosis.isprimary,
                stg_factcoursediagnosis.logid,
                stg_factcoursediagnosis.runid
              FROM
                `hca-hin-dev-cur-ops`.edwcr_staging.stg_factcoursediagnosis
          ) AS dp
          INNER JOIN (
            SELECT
                ref_rad_onc_site.source_site_id,
                ref_rad_onc_site.site_sk
              FROM
                `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site
          ) AS rr ON rr.source_site_id = dp.dimsiteid
          LEFT OUTER JOIN (
            SELECT
                ra_0.source_fact_patient_diagnosis_id,
                ra_0.fact_patient_diagnosis_sk,
                rs.source_site_id
              FROM
                (
                  SELECT
                      fact_rad_onc_patient_diagnosis.source_fact_patient_diagnosis_id,
                      fact_rad_onc_patient_diagnosis.fact_patient_diagnosis_sk,
                      fact_rad_onc_patient_diagnosis.site_sk
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient_diagnosis
                ) AS ra_0
                INNER JOIN (
                  SELECT
                      ref_rad_onc_site.source_site_id,
                      ref_rad_onc_site.site_sk
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site
                ) AS rs ON rs.site_sk = ra_0.site_sk
          ) AS ra ON dp.factpatientdiagnosisid = ra.source_fact_patient_diagnosis_id
           AND ra.source_site_id = dp.dimsiteid
          LEFT OUTER JOIN (
            SELECT
                ra1_0.source_patient_course_id,
                ra1_0.patient_course_sk,
                rs1.source_site_id
              FROM
                (
                  SELECT
                      rad_onc_patient_course.source_patient_course_id,
                      rad_onc_patient_course.patient_course_sk,
                      rad_onc_patient_course.site_sk
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_course
                ) AS ra1_0
                INNER JOIN (
                  SELECT
                      ref_rad_onc_site.source_site_id,
                      ref_rad_onc_site.site_sk
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site
                ) AS rs1 ON rs1.site_sk = ra1_0.site_sk
          ) AS ra1 ON dp.dimcourseid = ra1.source_patient_course_id
           AND ra1.source_site_id = dp.dimsiteid
          LEFT OUTER JOIN (
            SELECT
                dc_0.source_diagnosis_code_id,
                dc_0.diagnosis_code_sk,
                rs2.source_site_id
              FROM
                (
                  SELECT
                      ref_rad_onc_diagnosis_code.source_diagnosis_code_id,
                      ref_rad_onc_diagnosis_code.diagnosis_code_sk,
                      ref_rad_onc_diagnosis_code.site_sk
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_diagnosis_code
                ) AS dc_0
                INNER JOIN (
                  SELECT
                      ref_rad_onc_site.source_site_id,
                      ref_rad_onc_site.site_sk
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site
                ) AS rs2 ON rs2.site_sk = dc_0.site_sk
          ) AS dc ON dp.dimdiagnosiscodeid = dc.source_diagnosis_code_id
           AND dc.source_site_id = dp.dimsiteid
    ) AS stg
;
