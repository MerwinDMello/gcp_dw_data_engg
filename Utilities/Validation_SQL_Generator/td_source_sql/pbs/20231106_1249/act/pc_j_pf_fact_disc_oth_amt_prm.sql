SELECT 'PBMPC290-40' || ',' || coalesce(trim(Count(*)),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Gross_Rbmt_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Cont_Alw_Othr_Cor_Amt)) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Payment_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' || 
coalesce(trim(Cast( Sum(zeroifnull(DISC.Var_Charge_Othr_Cor_Amt) ) AS VARCHAR(20))),'0') || ',' as Source_String 
FROM EDWPBS.Fact_RCOM_PARS_Discrepancy DISC 
where DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND coid NOT IN (select coid from edwpbs.parallon_client_detail where Company_code='CHP')