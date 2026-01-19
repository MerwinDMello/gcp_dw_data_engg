SELECT
  MAX(dw_last_update_date_time),
  COUNT(*)
FROM
  `edwhr_copy.candidate_onboarding` 
FOR SYSTEM_TIME AS OF TIMESTAMP(DATETIME(2024,2,1,4,45,00),"US/Central")