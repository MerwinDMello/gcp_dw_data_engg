-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/pc_j_pf_dim_reason_code_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT coalesce(trim(format('%20d', count(eis_reason_code_dim.reason_code_sid))), '0') AS source_string
FROM `hca-hin-dev-cur-parallon`.edwpbs.eis_reason_code_dim 