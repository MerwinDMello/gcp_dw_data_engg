-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_rad_onc_procedure_code.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY dp.dimsiteid,
                                      dp.dimprocedurecodeid) AS procedure_code_sk,
                            rtt.treatment_type_sk,
                            rr.site_sk,
                            dp.dimprocedurecodeid,
                            dp.procedurecode,
                            dp.description,
                            dp.procedurecodedescription,
                            dp.activeind,
                            dp.log_id,
                            dp.run_id,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimprocedurecode.dimsiteid) AS INT64) AS dimsiteid,
             CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimprocedurecode.dimprocedurecodeid) AS INT64) AS dimprocedurecodeid,
             stg_dimprocedurecode.procedurecode,
             stg_dimprocedurecode.description,
             stg_dimprocedurecode.procedurecodedescription,
             stg_dimprocedurecode.activeind,
             CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimprocedurecode.logid) AS INT64) AS log_id,
             CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg_dimprocedurecode.runid) AS INT64) AS run_id
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimprocedurecode) AS dp
   LEFT OUTER JOIN
     (SELECT DISTINCT stg_sc_modalities.treatment_type,
                      stg_sc_modalities.treatment_category,
                      stg_sc_modalities.procedure_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_sc_modalities) AS sm ON upper(rtrim(sm.procedure_code)) = upper(rtrim(dp.procedurecode))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_treatment_type AS rtt ON upper(rtrim(sm.treatment_category)) = upper(rtrim(rtt.treatment_category_desc))
   AND upper(rtrim(sm.treatment_type)) = upper(rtrim(rtt.treatment_type_desc))
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON dp.dimsiteid = rr.source_site_id) AS stg