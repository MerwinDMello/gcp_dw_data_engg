-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_fact_rad_onc_patient_toxicity.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY dp.dimsiteid,
                                      dp.factpatienttoxicityid DESC) AS fact_patient_toxicity_sk
   FROM
     (SELECT stg_factpatienttoxicity.toxicitycomponentname,
             stg_factpatienttoxicity.toxicityassessmenttype,
             stg_factpatienttoxicity.dimactivitytransactionid,
             stg_factpatienttoxicity.dimpatientid,
             stg_factpatienttoxicity.dimlookupid_scheme,
             stg_factpatienttoxicity.dimlookupid_toxctycsecrtntytyp,
             stg_factpatienttoxicity.dimlookupid_toxicitycausetype,
             stg_factpatienttoxicity.dimdiagnosiscodeid,
             stg_factpatienttoxicity.dimsiteid,
             stg_factpatienttoxicity.factpatienttoxicityid,
             stg_factpatienttoxicity.assessmentdatetime,
             stg_factpatienttoxicity.toxicityeffectivedate,
             stg_factpatienttoxicity.toxicitygrade,
             stg_factpatienttoxicity.validentryindicator,
             stg_factpatienttoxicity.toxicityapproveddatetime,
             stg_factpatienttoxicity.assessmentperformeddatetime,
             stg_factpatienttoxicity.toxicityreason,
             stg_factpatienttoxicity.toxicityapprovedindicator,
             stg_factpatienttoxicity.toxctyheadervalidetryindicator,
             stg_factpatienttoxicity.revisionnumber,
             stg_factpatienttoxicity.logid,
             stg_factpatienttoxicity.runid
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_factpatienttoxicity) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site) AS rr ON dp.dimsiteid = rr.source_site_id) AS stg