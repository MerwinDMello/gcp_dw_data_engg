-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_cl_hcp_vc.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    (
      SELECT DISTINCT
          t1.hcp_dw_id AS hcp_dw_id,
          t2.hcp_user_id_3_4 AS hcp_user_id_3_4,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
        FROM
          `hca-hin-dev-cur-comp`.edwim_base_views.clinical_health_care_provider AS t1
          INNER JOIN (
            SELECT DISTINCT
                t2_0.hcp_other_id AS hcp_user_id_3_4,
                t1_0.hcp_npi
              FROM
                `hca-hin-dev-cur-comp`.edwim_base_views.hcp AS t1_0
                INNER JOIN `hca-hin-dev-cur-comp`.edwim_base_views.hcp_other_id AS t2_0 ON t1_0.hcp_dw_id = t2_0.hcp_dw_id
                 AND t2_0.id_type_sid = 17248
                 AND upper(rtrim(t2_0.hcp_other_id_active_ind)) = 'Y'
                 AND length(trim(t2_0.hcp_other_id)) = 7
                 AND bqutil.fn.cw_regexp_instr_2(substr(trim(t2_0.hcp_other_id), 4, 4), '[A-Za-z_]') = 0
                 AND bqutil.fn.cw_regexp_instr_2(substr(trim(t2_0.hcp_other_id), 1, 3), '[0-9_]') = 0
              WHERE upper(rtrim(t1_0.hcp_active_ind)) = 'Y'
               AND NOT rtrim(t1_0.hcp_last_name) = ''
               AND NOT rtrim(t1_0.hcp_first_name) = ''
               AND rtrim(t2_0.hcp_other_id) <> ''
              QUALIFY row_number() OVER (PARTITION BY t1_0.hcp_npi ORDER BY hcp_user_id_3_4) = 1
          ) AS t2 ON t1.national_provider_id = t2.hcp_npi
        WHERE NOT t1.national_provider_id IS NULL
         AND NOT t1.national_provider_id = 0.0
    ) AS a
;
