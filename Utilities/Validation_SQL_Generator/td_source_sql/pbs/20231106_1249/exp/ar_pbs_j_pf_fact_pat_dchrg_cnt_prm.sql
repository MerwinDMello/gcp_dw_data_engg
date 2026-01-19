Locking table edwpf_base_views.FACT_PATIENT for access
Locking table edwpf_base_views.Registration_Service  for access
Locking table edwpf_base_views.Ref_Service  for access
Locking table EDWPF_Staging.Pass_Current for access
Select 'PBMAR900-60' ||','|| trim(zeroifnull(Count(Pat_Acct_Num))) ||','||  trim(sum(cast(Discharge_To_Billing_Day_Cnt as decimal(20,2)))) ||',' as source_string
From(
Select 
a.Pat_Acct_Num,
Cast(cast((add_months(Current_Date, -1) (format 'YYYYMM')) as char(6)) as integer) as Date_sid,
--cast((a.final_bill_date-a.discharge_date) as decimal(20,0)) as Discharge_To_Billing_Day_Cnt, 
(a.final_bill_date-a.discharge_date) as Discharge_To_Billing_Day_Cnt,
a.Company_Code,
a.Coid,
cast((ff.unit_num) as integer) as UnitNum,
Case When fp.account_type_sid IS NULL Then 1 Else fp.account_type_sid End  as account_type_sid,
Case When fp.Account_Status_Sid IS NULL  Then 10 Else fp.Account_Status_Sid End as  account_status_sid,
Case When fp.Age_Month_Sid IS NULL  Then 1 Else fp.Age_Month_Sid End as  age_month_sid,
Case When fp.Patient_Financial_Class_Sid IS NULL  Then 23 Else fp.Patient_Financial_Class_Sid End as  patient_financial_class_sid,
Case When fp.Patient_Type_Sid IS NULL  Then 40 Else fp.Patient_Type_Sid End as  patient_type_sid,
Case When fp.Collection_Agency_Sid IS NULL  Then 1 Else fp.Collection_Agency_Sid End as  collection_agency_sid,
Case When fp.Payor_Sid IS NULL  Then 1 Else fp.Payor_Sid End as  payor_sid,
Case When fp.Payor_Financial_Class_Sid IS NULL  Then 24 Else fp.Payor_Financial_Class_Sid End as  payor_financial_class_sid,
Case When fp.Product_Sid IS NULL  Then 22 Else fp.Product_Sid End as  product_sid,
Case When fp.Contract_Sid IS NULL  Then 1 Else fp.Contract_Sid End as  contract_sid,
Case When fp.Scenario_Sid IS NULL  Then 1 Else fp.Scenario_Sid End as  scenario_sid,
Case When fp.Source_Sid IS NULL  Then 1 Else fp.Source_Sid End as  source_sid,
0 as IPLAN_Insurance_Order_Num,
1 as Billed_Patient_Cnt
From edwpf_base_views.FACT_PATIENT a
Join (Select Y.Patient_DW_ID, Max(Y.Service_Code_Start_Date)
from edwpf_base_views.Registration_Service Y group by 1) Z
(Patient_DW_ID, Service_Code_Start_Date)
on Z.Patient_DW_ID =  a.Patient_DW_ID
Join edwpf_base_views.Registration_Service b
on b.Patient_DW_ID =  a.Patient_DW_ID
and b.Service_Code_Start_Date = Z.Service_Code_Start_Date
Join edwpf_base_views.Ref_Service c
on b.COID = c.COID 
and b.Service_Code = c.Service_Code
LEFT OUTER JOIN EDWPF_Staging.Pass_Current PC
    ON a.Patient_DW_ID = PC.Patient_DW_ID
left outer join edwpbs_base_views.fact_rcom_ar_patient_level fp
on a.pat_acct_num = fp.patient_sid
and fp.company_code = a.company_code
and fp.coid = a.coid
and fp.date_sid = Cast(cast((add_months(Current_Date, -1) (format 'YYYYMM')) as char(6)) as integer)
and fp.iplan_insurance_order_num = 0
and fp.age_month_sid = 20
join edwfs_base_views.fact_facility ff
on ff.coid = a.coid
and ff.company_code = a.company_code
Where (a.company_code='H' OR substr(Trim(ff.Company_Code_Operations),1,1) = 'Y')
and (CASE WHEN PC.EOM_Total_Chgs IS NOT NULL
         AND CAST(PC.EOM_Total_Chgs AS DECIMAL(18,3))  <> 0
        THEN CAST(PC.EOM_Total_Chgs AS DECIMAL(18,3)) / 100
        ELSE a.Total_Billed_Charges
    END) <> 0
and a.Patient_Type_Code_Pos1= 'I'
and c.Service_Exempt_Flag = 'N'
and ( 
        (a.Discharge_Date between ADD_MONTHS(CAST((CAST((add_months(Current_Date, -0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) , -1) 
                                       and (CAST((CAST((add_months(Current_Date, -0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) -1)
             and a.Final_Bill_Date between ADD_MONTHS(CAST((CAST((add_months(Current_Date, -0)  (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) , -1) 
                                  and  (CAST((CAST((add_months(Current_Date, -0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01')   AS DATE) +3))
        OR
          (a.Discharge_Date <= (ADD_MONTHS(CAST((CAST((add_months(Current_Date, -0) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01') AS DATE) , -1)  -1)
           and a.Final_Bill_Date between (CAST((CAST((Add_Months(CURRENT_DATE, - 1) (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01')   AS DATE) +4)
                                              and  (CAST((CAST((add_months(Current_Date, -0)  (FORMAT 'YYYY-MM')) AS CHAR(7)) ||'-01')   AS DATE) +3))
)
and b.Service_Code_Start_Date <= a.Discharge_Date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)K