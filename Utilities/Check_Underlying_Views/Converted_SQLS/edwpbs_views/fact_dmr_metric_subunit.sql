-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_dmr_metric_subunit.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_subunit AS SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code AS coid,
    a.parent_code,
    a.child_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    a.dmr_code,
    a.dmr_metric_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    a.sub_unit_num,
    ROUND(a.dmr_metric_value, 3, 'ROUND_HALF_EVEN') AS dmr_metric_value,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_sub_unit AS fsu ON upper(a.parent_code) = upper(fsu.coid)
     AND upper(a.sub_unit_num) = upper(fsu.facility_sub_unit_id)
     AND upper(a.sub_unit_num) <> '00000'
     AND syslib.length(trim(a.sub_unit_num)) = 5
UNION DISTINCT
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code AS coid,
    a.parent_code,
    a.child_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    a.dmr_code,
    a.dmr_metric_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    '     ' AS sub_unit_num,
    ROUND(a.dmr_metric_value, 3, 'ROUND_HALF_EVEN') AS dmr_metric_value,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_sub_unit AS fsu ON upper(a.parent_code) = upper(fsu.coid)
     AND upper(a.sub_unit_num) = upper(fsu.facility_sub_unit_id)
     AND upper(a.sub_unit_num) = '00000'
  WHERE upper(a.parent_code) NOT IN(
    SELECT DISTINCT
        upper(fact_dmr_metric.parent_code) AS parent_code
      FROM
        `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric
      WHERE upper(fact_dmr_metric.sub_unit_num) <> '00000'
  )
;
