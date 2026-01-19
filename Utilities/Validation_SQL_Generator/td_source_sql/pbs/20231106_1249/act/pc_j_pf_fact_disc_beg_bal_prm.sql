SELECT 'PBMPC270' ||','||trim(A.Count1) ||','|| trim(zeroifnull(sum(A.Exp_Gross_Rbmt_Crnt_Amt))) ||','|| trim(zeroifnull(sum(A.Exp_Cont_Alw_Crnt_Amt))) 
||','|| trim(zeroifnull(sum(A.Exp_Payment_Crnt_Amt))) ||','|| trim(zeroifnull(sum(A.Exp_Charge_Crnt_Amt))) ||','|| 
trim(zeroifnull(sum(A.Var_Gross_Rbmt_Beg_Amt))) ||','|| trim(zeroifnull(sum(A.Var_Cont_Alw_Beg_Amt))) ||','|| 
trim(zeroifnull(sum(A.Var_Payment_Beg_Amt))) ||','|| trim(zeroifnull(sum(A.Var_Charge_Beg_Amt))) 
||',' as Source_String FROM 
(
Select count(*) as Count1, 
sum(Exp_Gross_Rbmt_Crnt_Amt) as Exp_Gross_Rbmt_Crnt_Amt,sum(Exp_Cont_Alw_Crnt_Amt) as Exp_Cont_Alw_Crnt_Amt,
sum(Exp_Payment_Crnt_Amt) as Exp_Payment_Crnt_Amt,sum(Exp_Charge_Crnt_Amt) as Exp_Charge_Crnt_Amt,
sum(Var_Gross_Rbmt_Beg_Amt) as Var_Gross_Rbmt_Beg_Amt,sum(Var_Cont_Alw_Beg_Amt) as Var_Cont_Alw_Beg_Amt,
sum(Var_Payment_Beg_Amt) as Var_Payment_Beg_Amt,sum(Var_Charge_Beg_Amt) as Var_Charge_Beg_Amt
From edwpbs.Fact_RCOM_PARS_Discrepancy
where Date_Sid = Cast(cast((add_months(current_date, -1) (format 'yyyymm')) as char(6)) as integer)
and Discrepancy_Resolved_Date  = '0001-01-01'
) A