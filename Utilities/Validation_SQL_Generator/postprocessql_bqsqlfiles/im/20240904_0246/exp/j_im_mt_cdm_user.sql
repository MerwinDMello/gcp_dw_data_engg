-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_cdm_user.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t3.company_code AS company_code,
          t3.coid AS coid,
          t2.mt_user_mnemonic_cs AS mt_user_mnemonic_cs,
          t1.mt_user_id AS mt_user_id,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM `hca-hin-dev-cur-comp`.edwim_staging.mt_cdm_user_3_4 AS t1
   INNER JOIN `hca-hin-dev-cur-comp`.edwim_staging.mt_cdm_user_mnem AS t2 ON t1.role_plyr_sk = t2.role_plyr_sk
   INNER JOIN `hca-hin-dev-cur-comp`.edwim_staging.mt_cdm_user_coid AS t3 ON t1.role_plyr_sk = t3.role_plyr_sk) AS a