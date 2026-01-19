Locking table edwpbs.Fact_RCOM_PARS_Discrepancy for access
SELECT  'PBMPC310'||','||
COALESCE(Cast( Sum(DISC.Exp_Gross_Rbmt_Rate_Amt ) AS VARCHAR(20)),'0')||','||
COALESCE(Cast( Sum(DISC.Exp_Cont_Alw_Rate_Amt) AS VARCHAR(20)),'0')||','||
COALESCE(Cast( Sum(DISC.Exp_Payment_Rate_Amt ) AS VARCHAR(20)),'0')||',' as SOURCE_STRING
FROM
 EDWPBS.Fact_RCOM_PARS_Discrepancy DISC
WHERE DISC.Patient_Sid = 999999999999
AND  DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')