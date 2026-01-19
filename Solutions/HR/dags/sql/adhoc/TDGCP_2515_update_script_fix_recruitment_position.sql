-- Delete all SK keys for recruitment_position table
DELETE FROM edwhr_staging.ref_sk_xwlk 
WHERE upper(sk_type)=upper('RECRUITMENT_POSITION');

-- Refresh SK keys from mirrored dev staging table
INSERT INTO edwhr_staging.ref_sk_xwlk
SELECT * FROM `hca-hin-dev-cur-hr.edwhr_staging_copy.ref_sk_xwlk`
WHERE upper(sk_type)=upper('RECRUITMENT_POSITION')
AND CAST(sk_generated_date_time AS DATE) <= DATE '2023-08-08';

-- Update recruitment_job - recruitment_position_sid field
-- Join based on Primary Key for valid records
UPDATE edwhr.recruitment_job tgt
SET recruitment_position_sid = src.recruitment_position_sid
FROM `hca-hin-dev-cur-hr.edwhr_copy.recruitment_job` src
WHERE src.recruitment_job_num = tgt.recruitment_job_num
AND src.source_system_code = tgt.source_system_code
AND CAST(tgt.valid_to_date AS DATE) = '9999-12-31'
AND CAST(src.valid_to_date AS DATE) = '9999-12-31'
AND COALESCE(tgt.recruitment_position_sid,123456) <> COALESCE(src.recruitment_position_sid,123456)
AND (CAST(tgt.dw_last_update_date_time AS DATE) >= '2023-05-05' OR CAST(src.dw_last_update_date_time AS DATE) >= '2023-05-05');

-- Update offer_detail - element_detail_value_text field
-- Join based on Primary Key for valid records
UPDATE edwhr.offer_detail tgt
SET element_detail_value_text = src.element_detail_value_text
FROM (
  SELECT 
    od.element_detail_entity_text, od.element_detail_type_text, od.element_detail_seq_num,
    od.valid_to_date, od.element_detail_value_text, cast(xwlk.sk as int64) as offer_sid, od.dw_last_update_date_time
  FROM `hca-hin-dev-cur-hr.edwhr_copy.offer_detail` od
  INNER JOIN `hca-hin-dev-cur-hr.edwhr_copy.offer` src_off
    ON src_off.offer_sid = od.offer_sid
    AND src_off.source_system_code = od.source_system_code
    AND CAST(src_off.valid_to_date AS DATE) = '9999-12-31'
  INNER JOIN edwhr_staging.ref_sk_xwlk xwlk
    ON CAST(src_off.offer_num AS STRING) = xwlk.sk_source_txt
    AND UPPER(xwlk.sk_type) = 'OFFER'
) src
WHERE src.offer_sid = tgt.offer_sid
AND tgt.element_detail_entity_text = src.element_detail_entity_text
AND tgt.element_detail_type_text = src.element_detail_type_text
AND tgt.element_detail_seq_num = src.element_detail_seq_num
AND CAST(tgt.valid_to_date AS DATE) = '9999-12-31'
AND CAST(src.valid_to_date AS DATE) = '9999-12-31'
AND COALESCE(tgt.element_detail_value_text,'') <> COALESCE(src.element_detail_value_text,'')
AND (CAST(tgt.dw_last_update_date_time AS DATE) >= '2023-05-05' OR CAST(src.dw_last_update_date_time AS DATE) >= '2023-05-05');

-- Update recruitment_job_detail - element_detail_value_text
-- Join based on Primary Key for valid records
UPDATE edwhr.recruitment_job_detail tgt
SET element_detail_value_text = src.element_detail_value_text
FROM (
  SELECT 
    jd.element_detail_entity_text, jd.element_detail_type_text, jd.element_detail_seq_num,
    jd.valid_to_date, jd.element_detail_value_text, cast(xwlk.sk as int64) as recruitment_job_sid, jd.dw_last_update_date_time
  FROM `hca-hin-dev-cur-hr.edwhr_copy.recruitment_job_detail` jd
  INNER JOIN `hca-hin-dev-cur-hr.edwhr_copy.recruitment_job` src_job
    ON src_job.recruitment_job_sid = jd.recruitment_job_sid
    AND src_job.source_system_code = jd.source_system_code
    AND CAST(src_job.valid_to_date AS DATE) = '9999-12-31'
  INNER JOIN edwhr_staging.ref_sk_xwlk xwlk
    ON CAST(src_job.recruitment_job_num AS STRING) = xwlk.sk_source_txt
    AND UPPER(xwlk.sk_type) = 'RECRUITMENT_JOB'
) src
WHERE src.recruitment_job_sid = tgt.recruitment_job_sid
AND tgt.element_detail_entity_text = src.element_detail_entity_text
AND tgt.element_detail_type_text = src.element_detail_type_text
AND tgt.element_detail_seq_num = src.element_detail_seq_num
AND CAST(tgt.valid_to_date AS DATE) = '9999-12-31'
AND CAST(src.valid_to_date AS DATE) = '9999-12-31'
AND COALESCE(tgt.element_detail_value_text,'') <> COALESCE(src.element_detail_value_text,'')
AND (CAST(tgt.dw_last_update_date_time AS DATE) >= '2023-05-05' OR CAST(src.dw_last_update_date_time AS DATE) >= '2023-05-05');