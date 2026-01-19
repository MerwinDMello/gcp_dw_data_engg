-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/act/j_im_rcopia_provider_status_stg.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    `hca-hin-dev-cur-comp`.edwim_staging.rcopia_provider_status_stg
;
