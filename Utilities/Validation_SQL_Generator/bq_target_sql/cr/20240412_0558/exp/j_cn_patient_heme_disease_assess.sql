-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_heme_disease_assess.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          iq.*
        FROM
          (
            SELECT
                trim(phd.hbsource) AS hashbite_ssk
              FROM
                `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg AS phd
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_test_type AS rt ON upper(trim(phd.testtype)) = upper(trim(rt.test_sub_type_desc))
                 AND upper(trim(rt.test_type_desc)) = 'DISEASE ASSESSMENT'
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_sample_type AS rst ON upper(trim(phd.samplesourcetype)) = upper(trim(rst.sample_type_name))
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_disease_assess_source AS rda ON upper(trim(phd.source)) = upper(trim(rda.disease_assess_source_name))
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_facility AS rf ON upper(trim(phd.facilityname)) = upper(trim(rf.facility_name))
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS rs ON upper(trim(phd.diseasestatus)) = upper(trim(rs.status_desc))
                 AND upper(trim(rs.status_type_desc)) = 'DISEASE'
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS rss ON upper(trim(phd.treatementstatus)) = upper(trim(rss.status_desc))
                 AND upper(trim(rss.status_type_desc)) = 'TREATMENT'
          ) AS iq
          LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_heme_disease_assess AS cphd ON upper(trim(iq.hashbite_ssk)) = upper(trim(cphd.hashbite_ssk))
        WHERE trim(cphd.hashbite_ssk) IS NULL
    ) AS iqq
;
