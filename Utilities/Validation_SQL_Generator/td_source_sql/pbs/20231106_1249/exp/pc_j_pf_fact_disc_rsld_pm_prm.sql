Select 'PBMPC290-20'||','||
trim(cast(zeroifnull(A.RowCount) as varchar(10))) ||','||
trim(cast(zeroifnull(A.EOR_Gross_Reimbursement_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.EOR_Contractual_Allowance_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.EOR_Insurance_Payment_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Sum1) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Gross_Reimbursement_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Contractual_Allowance_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Insurance_Payment_Amt) as decimal(20,3))) ||','||
trim(cast(zeroifnull(A.Var_Net_Billed_Charge_Amt) as decimal(20,3))) ||',' as SOURCE_STRING from(
SELECT 
count(RSVD.Patient_DW_ID) as RowCount,
Cast(sum(EOR.EOR_Gross_Reimbursement_Amt) as varchar(20)) as EOR_Gross_Reimbursement_Amt , 
Cast(sum(EOR.EOR_Contractual_Allowance_Amt) as varchar(20)) as EOR_Contractual_Allowance_Amt, 
Cast(sum(EOR.EOR_Insurance_Payment_Amt) as varchar(20)) as EOR_Insurance_Payment_Amt, 
Cast(sum(0) as varchar(20)) as Sum1, 
Cast(sum(ABS(RSVD.Var_Gross_Reimbursement_Amt)) as varchar(20)) as Var_Gross_Reimbursement_Amt, 
Cast(sum(ABS(RSVD.Var_Contractual_Allowance_Amt)) as varchar(20)) as Var_Contractual_Allowance_Amt, 
Cast(sum(ABS(RSVD.Var_Insurance_Payment_Amt)) as varchar(20)) as Var_Insurance_Payment_Amt, 
Cast(sum(ABS(RSVD.Var_Net_Billed_Charge_Amt)) as varchar(20)) as Var_Net_Billed_Charge_Amt
FROM  Edwpf_Base_Views.Resolved_Discrepancy RSVD
Left outer join Edwpf_Base_Views.Explanation_Of_Reimbursement EOR
ON RSVD.Company_Code = EOR.Company_Code
AND RSVD.Coid = EOR.Coid
AND RSVD.Patient_DW_ID = EOR.Patient_DW_ID 
AND RSVD.Payor_DW_ID = EOR.Payor_DW_ID
AND RSVD.IPLAN_Insurance_Order_Num = EOR.IPLAN_Insurance_Order_Num 
AND RSVD.EOR_Log_Date = EOR.EOR_Log_Date
AND RSVD.Log_ID = EOR.Log_ID 
AND RSVD.Log_Sequence_Num = EOR.Log_Sequence_Num
AND COALESCE(RSVD.CC_Calc_Id,999) = COALESCE(EOR.CC_Calc_Id,999)
Left outer join Edwpf_Base_Views.Fact_RCOM_PARS_Discrepancy DISC
ON DISC.Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND DISC.Company_Code = RSVD.Company_Code
AND DISC.Coid = RSVD.Coid
AND DISC.Patient_DW_ID = RSVD.Patient_DW_ID 
AND DISC.Payor_DW_ID = RSVD.Payor_DW_ID
AND DISC.IPLAN_Insurance_Order_Num = RSVD.IPLAN_Insurance_Order_Num 
AND DISC.EOR_Log_Date = RSVD.EOR_Log_Date
AND DISC.Log_Sequence_Num = RSVD.Log_Sequence_Num
AND DISC.AR_Bill_Thru_Date  = EOR.AR_Bill_Thru_Date
AND DISC.Discrepancy_Creation_Date = RSVD.Discrepancy_Origination_Date
AND DISC.Log_ID = RSVD.Log_ID 
AND DISC.Discrepancy_Resolved_Date  = '0001-01-01'
Left outer join 
(Select PT.Patient_Dw_Id, Max(PT.Eff_From_Date)
From Edwpf_Base_Views.Admission_Patient_Type PT Group by 1) ATP
(Patient_Dw_Id,  Eff_From_Date)
on ATP.Patient_DW_Id = RSVD.Patient_DW_Id
Left outer join Edwpf_Base_Views.Admission_Patient_Type APT
on APT.Patient_DW_Id = RSVD.Patient_DW_Id
and APT.Eff_From_Date = ATP.Eff_From_Date
Left outer join Edwpf_Base_Views.Admission_Discharge AD
on AD.Patient_DW_Id = RSVD.Patient_DW_Id
Left outer join Edwfs_Base_Views.FACILITY FF
on FF.Coid = RSVD.Coid
and FF.Company_Code = RSVD.Company_Code
WHERE  
cast((RSVD.Discrepancy_Resolved_Date (format 'yyyymm')) as char(6)) =  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND	
cast((RSVD.Discrepancy_Origination_Date (format 'yyyymm')) as char(6)) <  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
AND EOR.Eff_From_Date = 
(Select Max(ER.Eff_From_Date) From Edwpf_Base_Views.Explanation_Of_Reimbursement ER
Where RSVD.Company_Code = ER.Company_Code
AND RSVD.Coid = ER.Coid
AND RSVD.Patient_DW_ID = ER.Patient_DW_ID 
AND RSVD.Payor_DW_ID = ER.Payor_DW_ID
AND RSVD.IPLAN_Insurance_Order_Num = ER.IPLAN_Insurance_Order_Num 
AND RSVD.EOR_Log_Date = ER.EOR_Log_Date
AND RSVD.Log_ID = ER.Log_ID 
AND RSVD.Log_Sequence_Num = ER.Log_Sequence_Num
AND COALESCE(RSVD.CC_Calc_Id,999) = COALESCE(ER.CC_Calc_Id,999)
AND ER.Eff_From_Date <= RSVD.Discrepancy_Resolved_Date)
AND RSVD.Coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP'))A