-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/physician_detail_crnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.physician_detail_crnt AS SELECT
    a.company_code,
    a.coid,
    a.facility_physician_num,
    a.unit_num,
    a.physician_npi,
    a.physician_name,
    a.physician_last_name,
    a.physician_first_name,
    a.physician_middle_name,
    a.physician_name_prefix,
    a.physician_name_suffix,
    a.medical_specialty_code,
    a.upin_num,
    a.physician_birth_date,
    'XXXX' AS social_security_num,
    a.physician_status_code,
    a.patfin_physician_dw_id,
    a.physician_group_npi,
    a.physician_group_num,
    a.physician_group_name,
    a.dim_physician_name_child,
    a.dim_physician_spcl_name_child,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.physician_detail_crnt AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
