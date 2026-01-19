-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_esaf_provisioning_event_time_archive.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t2.im_domain_id,
          t1.esaf_user_id,
          t2.esaf_activity_date,
          t1.esaf_application_id,
          t1.esaf_operation_type,
          t1.esaf_event_time,
          t1.source_system_code,
          t1.dw_last_update_date_time
   FROM {{ params.param_im_core_dataset_name }}.esaf_provisioning_event_time AS t1
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.im_person_activity AS t2 ON upper(rtrim(t1.esaf_user_id)) = upper(rtrim(t2.im_person_user_id)) QUALIFY row_number() OVER (PARTITION BY t2.im_domain_id,
                                                                                                                                                                                                   upper(t1.esaf_user_id),
                                                                                                                                                                                                   t2.esaf_activity_date,
                                                                                                                                                                                                   upper(t1.esaf_application_id)
                                                                                                                                                                                      ORDER BY t1.esaf_event_time DESC) = 1) AS a