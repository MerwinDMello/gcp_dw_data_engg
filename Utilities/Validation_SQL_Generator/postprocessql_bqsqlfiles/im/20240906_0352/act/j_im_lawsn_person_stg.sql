-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/act/j_im_lawsn_person_stg.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM `hca-hin-dev-cur-comp`.edwim_staging.lawsn_person