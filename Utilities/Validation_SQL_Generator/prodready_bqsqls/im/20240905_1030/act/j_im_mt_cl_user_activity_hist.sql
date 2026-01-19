-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/act/j_im_mt_cl_user_activity_hist.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_stage_dataset_name }}.mt_cl_user_activity_hist
   WHERE mt_cl_user_activity_hist.dw_last_update_date_time >
       (SELECT max(etl_job_run.job_start_date_time)
        FROM `hca-hin-dev-cur-comp`.edwim_dmx_ac.etl_job_run
        WHERE upper(rtrim(etl_job_run.job_name)) = 'J_IM_MT_CL_USER_ACTIVITY_HIST'
          AND etl_job_run.job_status_code IS NULL ) ) AS a