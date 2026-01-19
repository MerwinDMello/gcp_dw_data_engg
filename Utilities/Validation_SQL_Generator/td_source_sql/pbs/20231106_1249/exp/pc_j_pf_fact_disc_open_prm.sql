SELECT 
'PBMPC290-30' || ','  ||
'0'  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(0) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Gross_Reimbursement_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Contractual_Allowance_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Insurance_Payment_Amt)) as varchar(20)),0))  || ','  ||
trim(coalesce(Cast(Sum(ABS(RD.Var_Net_Billed_Charge_Amt)) as varchar(20)),0))  || ','  
AS SOURCE_STRING
FROM  Edwpf_Base_Views.Reimbursement_Discrepancy_EOM RD
Left outer join Edwpf_Base_Views.Explanation_Of_Reimbursement EOR
ON RD.Company_Code = EOR.Company_Code
AND RD.Coid = EOR.Coid AND RD.Patient_DW_ID = EOR.Patient_DW_ID 
AND RD.Payor_DW_ID = EOR.Payor_DW_ID AND RD.IPLAN_Insurance_Order_Num = EOR.IPLAN_Insurance_Order_Num 
AND RD.EOR_Log_Date = EOR.EOR_Log_Date AND RD.Log_ID = EOR.Log_ID 
AND RD.Log_Sequence_Num = EOR.Log_Sequence_Num
AND COALESCE(RD.CC_Calc_Id,999) = COALESCE(EOR.CC_Calc_Id,999)
WHERE EOR.Eff_From_Date = 
(Select Max(ER.Eff_From_Date) From Edwpf_Base_Views.Explanation_Of_Reimbursement ER
Where RD.Company_Code = ER.Company_Code
AND RD.Coid = ER.Coid
AND RD.Patient_DW_ID = ER.Patient_DW_ID 
AND RD.Payor_DW_ID = ER.Payor_DW_ID
AND RD.IPLAN_Insurance_Order_Num = ER.IPLAN_Insurance_Order_Num 
AND RD.EOR_Log_Date = ER.EOR_Log_Date
AND RD.Log_ID = ER.Log_ID 
AND RD.Log_Sequence_Num = ER.Log_Sequence_Num
AND COALESCE(RD.CC_Calc_Id,999) = COALESCE(ER.CC_Calc_Id,999)
AND ER.Eff_From_Date <= (cast((cast(((current_date) (format 'yyyy-mm')) as char(7)) || '-01') as date) -1))
AND RD.Coid not in (select coid from edwpbs.parallon_client_detail where Company_code='CHP')