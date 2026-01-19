export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMAR250'

export AC_EXP_SQL_STATEMENT="SELECT 'PBMAR250' ||','|| trim(zeroifnull(count(*))) ||',' As Source_String
from

(
SELECT 
FP.Patient_Sid, FP.Account_Type_Sid, FP.Account_Status_Sid, FP.Age_Month_Sid, FP.Patient_Financial_Class_Sid,FP.Patient_Type_Sid, FP.Collection_Agency_Sid, FP.Payor_Financial_Class_Sid, 
FP.Product_Sid, FP.Contract_Sid, FP.Scenario_Sid,FP.Date_Sid, FP.Source_Sid, FP.Unit_Num_Sid, FP.IPLAN_Insurance_Order_Num,FP.Coid, FP.Company_Code, FP.Patient_Account_Cnt, 
FP.Discharge_Cnt, FP.AR_Patient_Amt, FP.AR_Insurance_Amt, FP.Write_Off_Amt, FP.Total_Collect_Amt, FP.Billed_Patient_Cnt, 
FP.Discharge_To_Billing_Day_Cnt, FP.Gross_Charge_Amt, FP.Prorated_Liability_Sys_Adj_Amt, FP.Late_Charge_Credit_Amt, 
FP.Late_Charge_Debit_Amt, FP.Payor_Prorated_Liability_Amt, FP.Payor_Payment_Amt, FP.Payor_Adjustment_Amt, FP.Payor_Contractual_Amt,
FP.Payor_Denial_Amt, FP.Payor_Denial_Cnt, FP.Payor_Expected_Payment_Amt, FP.Payor_Discrepancy_Ovr_Pmt_Amt, 
FP.Payor_Discrepancy_Undr_Pmt_Amt, FP.Payor_Up_Front_Collection_Amt, FP.Payor_Bill_Cnt, FP.Payor_Rebill_Cnt, FP.Payor_Sid,
FP.Unbilled_Gross_Bus_Ofc_Amt, FP.Unbilled_Gross_Med_Rec_Amt 
FROM
edwpf_base_views.fact_rcom_ar_patient_level FP
WHERE date_sid = (select max(time_id) from edwpf.eis_date_dim where current_mth = 'y') 
)P"

export AC_ACT_SQL_STATEMENT="select 'PBMAR250' ||','|| trim(zeroifnull(count(*))) ||',' as Source_String
 from edwpf.fact_rcom_ar_patient_lvl_cm"
