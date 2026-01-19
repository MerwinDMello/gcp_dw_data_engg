-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_ssi_responsible_dept_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_ssi_responsible_dept_pf AS SELECT
    ref_ssi_responsible_dept_pf.ssi_queue_dept_id,
    ref_ssi_responsible_dept_pf.ssi_queue_dept_desc,
    ref_ssi_responsible_dept_pf.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ssi_responsible_dept_pf
;
