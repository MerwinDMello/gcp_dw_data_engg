-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ref_disease_assess_source.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT 'X' AS ccnt
   FROM
     (SELECT DISTINCT patient_heme_disease_assessment_stg.source AS disease_assess_source_name
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_disease_assessment_stg
      WHERE patient_heme_disease_assessment_stg.source IS NOT NULL ) AS ssc
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_disease_assess_source AS rtt ON upper(rtrim(ssc.disease_assess_source_name)) = upper(rtrim(rtt.disease_assess_source_name))
   WHERE rtt.disease_assess_source_name IS NULL ) AS iq