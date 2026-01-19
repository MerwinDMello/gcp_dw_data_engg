-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_location_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_location_cdm AS SELECT
    ROUND(dim_location.location_sk, 0, 'ROUND_HALF_EVEN') AS location_sk,
    dim_location.company_code,
    dim_location.coid,
    dim_location.valid_from_date_time,
    dim_location.location_desc,
    dim_location.location_name,
    dim_location.location_type_code,
    dim_location.location_cost_center_ref_code,
    dim_location.unit_name,
    dim_location.active_ind,
    dim_location.effective_from_date,
    dim_location.psych_unit_ind,
    dim_location.csg_location_grouping_desc,
    ROUND(dim_location.facility_sk, 0, 'ROUND_HALF_EVEN') AS facility_sk,
    dim_location.department_ref_code,
    dim_location.source_system_txt,
    dim_location.source_system_code,
    dim_location.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.dim_location
;
