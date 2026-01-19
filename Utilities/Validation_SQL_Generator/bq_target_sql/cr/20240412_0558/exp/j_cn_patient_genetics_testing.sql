-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_genetics_testing.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          pgt.geneticstestingfactid,
          pgt.corerecordid,
          pgt.patientdimid,
          pgt.tumortypedimid,
          pgt.diagnosisresultid,
          pgt.diagnosisdimid,
          pgt.coid,
          'H' AS company_code,
          pgt.navigatordimid,
          pgt.geneticsdate,
          pgt.geneticstesttype,
          pgt.geneticsspecialist,
          brca.breast_cancer_type_id,
          pgt.geneticscomments,
          pgt.hbsource,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_genetics_testing_stg AS pgt
          LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_breast_cancer_type AS brca ON upper(rtrim(pgt.geneticsbrcatype)) = upper(rtrim(brca.breast_cancer_type_desc))
        WHERE upper(pgt.hbsource) NOT IN(
          SELECT
              upper(cn_patient_genetics_testing.hashbite_ssk) AS hashbite_ssk
            FROM
              `hca-hin-dev-cur-ops`.edwcr.cn_patient_genetics_testing
        )
    ) AS src
;
