-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_phone_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT mhb_phone_wrk.rdc_sid
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.mhb_phone_wrk
   WHERE (coalesce(mhb_phone_wrk.rdc_sid, 0),
          coalesce(format('%#20.0f', mhb_phone_wrk.mhb_phone_id), ''),
          upper(coalesce(mhb_phone_wrk.phone_id_text, '')),
          upper(coalesce(mhb_phone_wrk.phone_name, '')),
          upper(coalesce(mhb_phone_wrk.phone_os_code, '')),
          upper(coalesce(mhb_phone_wrk.os_version_text, '')),
          upper(coalesce(mhb_phone_wrk.device_pooling_config_code, '')),
          upper(coalesce(mhb_phone_wrk.active_dw_ind, ''))) NOT IN
       (SELECT AS STRUCT coalesce(mhb_phone.rdc_sid, 0),
                         coalesce(format('%#20.0f', mhb_phone.mhb_phone_id), ''),
                         upper(coalesce(mhb_phone.phone_id_text, '')),
                         upper(coalesce(mhb_phone.phone_name, '')),
                         upper(coalesce(mhb_phone.phone_os_code, '')),
                         upper(coalesce(mhb_phone.os_version_text, '')),
                         upper(coalesce(mhb_phone.device_pooling_config_code, '')),
                         upper(coalesce(mhb_phone.active_dw_ind, ''))
        FROM {{ params.param_clinical_ci_core_dataset_name }}.mhb_phone
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS q