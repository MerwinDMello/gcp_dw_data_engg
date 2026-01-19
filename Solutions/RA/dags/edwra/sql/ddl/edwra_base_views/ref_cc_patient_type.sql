-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_patient_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_patient_type AS SELECT
    ref_cc_patient_type.company_code,
    ref_cc_patient_type.coid,
    ref_cc_patient_type.patient_type_id,
    ref_cc_patient_type.unit_num,
    ref_cc_patient_type.cc_patient_type_code,
    ref_cc_patient_type.pa_patient_type_code,
    ref_cc_patient_type.patient_type_desc,
    ref_cc_patient_type.ip_op_ind,
    ref_cc_patient_type.ce_patient_type_id,
    ref_cc_patient_type.create_date_time,
    ref_cc_patient_type.update_date_time,
    ref_cc_patient_type.dw_last_update_time,
    ref_cc_patient_type.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_patient_type
;
