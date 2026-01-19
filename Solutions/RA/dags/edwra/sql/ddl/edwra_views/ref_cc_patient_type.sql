-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_patient_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_patient_type AS SELECT
    a.company_code,
    a.coid,
    a.patient_type_id,
    a.unit_num,
    a.cc_patient_type_code,
    a.pa_patient_type_code,
    a.patient_type_desc,
    a.ip_op_ind,
    a.ce_patient_type_id,
    a.create_date_time,
    a.update_date_time,
    a.dw_last_update_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_patient_type AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
