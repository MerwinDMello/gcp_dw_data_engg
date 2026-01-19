#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMPC270'

export AC_EXP_SQL_STATEMENT="SELECT 'PBMPC270' ||','||trim(A.Count1) ||','|| trim(zeroifnull(A.Exp_Gross_Rbmt_Crnt_Amt))||','||trim(zeroifnull(A.Exp_Cont_Alw_Crnt_Amt))||','||
trim(zeroifnull(A.Exp_Payment_Crnt_Amt))||','||trim(zeroifnull(A.Exp_Charge_Crnt_Amt))||','||trim(zeroifnull(A.Var_Gross_Rbmt_End_Amt))||','||
trim(zeroifnull(A.Var_Cont_Alw_End_Amt))||','||trim(zeroifnull(A.Var_Payment_End_Amt))||','||trim(zeroifnull(A.Var_Charge_End_Amt))
||',' as Source_String FROM 
(
SELECT 
Count(*) as Count1, 
sum(DISC.Exp_Gross_Rbmt_Crnt_Amt) as Exp_Gross_Rbmt_Crnt_Amt,
sum(DISC.Exp_Cont_Alw_Crnt_Amt) as Exp_Cont_Alw_Crnt_Amt, 
sum(DISC.Exp_Payment_Crnt_Amt)  as Exp_Payment_Crnt_Amt, 
sum(DISC.Exp_Charge_Crnt_Amt) as Exp_Charge_Crnt_Amt, 
sum(DISC.Var_Gross_Rbmt_End_Amt) as Var_Gross_Rbmt_End_Amt, 
sum(DISC.Var_Cont_Alw_End_Amt) as Var_Cont_Alw_End_Amt, 
sum(DISC.Var_Payment_End_Amt) as Var_Payment_End_Amt, 
sum(DISC.Var_Charge_End_Amt)  as Var_Charge_End_Amt
 
FROM
EDWPBS.Fact_RCOM_PARS_Discrepancy DISC
WHERE DISC.Date_Sid = cast((add_months(current_date, -2) (format 'yyyymm')) as char(6))
AND DISC.Patient_Sid <> 999999999999
AND (DISC.Var_Gross_Rbmt_End_Amt <> 0 OR DISC.Var_Cont_Alw_End_Amt <> 0  
OR DISC.Var_Payment_End_Amt <> 0 OR DISC.Var_Charge_End_Amt <> 0
OR ((Reason_Code_1_Sid IN (56,28,136) OR Reason_Code_2_Sid = 136 OR Reason_Code_3_Sid = 136 OR Reason_Code_4_Sid = 136) AND Discrepancy_Resolved_Date = '0001-01-01'))
) A"

export AC_ACT_SQL_STATEMENT="SELECT 'PBMPC270' ||','||trim(A.Count1) ||','|| trim(zeroifnull(sum(A.Exp_Gross_Rbmt_Crnt_Amt))) ||','|| trim(zeroifnull(sum(A.Exp_Cont_Alw_Crnt_Amt))) 
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
) A"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  
