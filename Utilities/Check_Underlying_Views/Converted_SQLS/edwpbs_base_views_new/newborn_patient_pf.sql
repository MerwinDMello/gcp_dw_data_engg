-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/newborn_patient_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.newborn_patient_pf AS SELECT
    newborn_patient.patient_dw_id,
    newborn_patient.pat_acct_num,
    newborn_patient.mother_patient_dw_id,
    newborn_patient.father_name,
    newborn_patient.birth_weight_amt,
    newborn_patient.multiple_birth_ind,
    newborn_patient.adoption_ind,
    newborn_patient.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.newborn_patient
;
