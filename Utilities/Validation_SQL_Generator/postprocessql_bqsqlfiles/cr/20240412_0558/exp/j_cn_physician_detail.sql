-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_physician_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY upper(wrk.physician_name),
                                      upper(wrk.physician_phone_num)) +
     (SELECT coalesce(max(cn_physician_detail.physician_id), 900000) AS physician_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail) AS physician_id,
                            wrk.physician_name,
                            wrk.physician_phone_num,
                            'N' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_detail_stg_wrk AS wrk
   LEFT OUTER JOIN
     (SELECT cn_physician_detail.physician_id,
             cn_physician_detail.physician_name,
             cn_physician_detail.physician_phone_num
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail
      WHERE cn_physician_detail.physician_id > 900000 ) AS tgt ON upper(rtrim(coalesce(trim(wrk.physician_name), 'XX'))) = upper(rtrim(coalesce(trim(tgt.physician_name), 'XX')))
   AND upper(rtrim(coalesce(trim(wrk.physician_phone_num), 'X'))) = upper(rtrim(coalesce(trim(tgt.physician_phone_num), 'X')))
   WHERE tgt.physician_id IS NULL
     AND wrk.physician_id IS NULL
   UNION ALL SELECT wrk.physician_id,
                    wrk.physician_name,
                    wrk.physician_phone_num,
                    'N' AS source_system_code,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_physician_detail_stg_wrk AS wrk
   LEFT OUTER JOIN
     (SELECT cn_physician_detail.physician_id,
             cn_physician_detail.physician_name,
             cn_physician_detail.physician_phone_num
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail
      WHERE cn_physician_detail.physician_id <= 900000 ) AS tgt ON upper(rtrim(coalesce(trim(wrk.physician_name), 'XX'))) = upper(rtrim(coalesce(trim(tgt.physician_name), 'XX')))
   AND upper(rtrim(coalesce(trim(wrk.physician_phone_num), 'X'))) = upper(rtrim(coalesce(trim(tgt.physician_phone_num), 'X')))
   WHERE tgt.physician_id IS NULL
     AND wrk.physician_id IS NOT NULL ) AS src