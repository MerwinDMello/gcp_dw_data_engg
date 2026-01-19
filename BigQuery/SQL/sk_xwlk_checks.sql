-- SELECT
--   COALESCE(MAX(sk),0),
--   Max(sk_generated_date_time),
--   COUNT(*)
-- FROM
--   `hca-hin-dev-cur-hr.edwhr_staging_copy.ref_sk_xwlk`
-- WHERE
--   sk_type='CANDIDATE_ONBOARDING';
  -- 507850
SELECT
  DISTINCT employeeid||hrconumber||requisitionnumber||applicantnumber
FROM
  edwhr_staging_copy.enwisen_audit
WHERE
  employeeid||hrconumber||requisitionnumber||applicantnumber NOT IN (
  SELECT
    sk_source_txt
  FROM
    edwhr_staging_copy.ref_sk_xwlk
  WHERE
    sk_type='CANDIDATE_ONBOARDING') 
  --new_sk 
  -- INNER JOIN
  -- `hca-hin-dev-cur-hr.edwhr_staging_copy.ref_sk_type` st
  -- on st.sk_type = new_sk.sk_type;