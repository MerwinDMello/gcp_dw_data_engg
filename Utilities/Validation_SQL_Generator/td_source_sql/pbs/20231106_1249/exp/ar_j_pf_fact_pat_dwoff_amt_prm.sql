select 'PBMAR380'  || ',' || coalesce(cast(EXP1.sum1 as varchar(20)), '0') || ',' || coalesce(cast(EXP1.cnt1 as varchar(20)) , '0') || ','  
AS SOURCE_STRING from
(
SELECT
    K.PE_Date,
    coalesce(cast(cast(Sum( K.W_Off_Denial_Account_Amt) as decimal(18,3)) as varchar(20)), 0) SUM1,
    coalesce(Sum(Case When K.W_Off_Denial_Account_Amt <> 0 Then 1 Else 0 End) , 0)CNT1
    
    
FROM
(
SELECT
     K.Pat_Acct_Num, 
     K.PE_Date, 
     K.IPLAN_Insurance_Order_Num, 
     K.Company_Code as k_company_code, 
     K.Coid as k_coid, 
     k.W_Off_Denial_Account_Amt,
     K.Admission_Date,
     K.Patient_Type_Sid,     
     K.Unit_Num_Sid,
     PD.Payor_Sid,    
     K.Patient_Financial_Class_Sid,
     K.Payor_Financial_Class_Sid,
     fp.Coid,
     fp.Company_Code,
     fp.Account_Type_Sid,
     fp.Account_Status_Sid,
     fp.Age_Month_Sid,
     fp.Collection_Agency_Sid,
     fp.Product_Sid,
     fp.Contract_Sid,
     fp.Scenario_Sid
FROM
(
SELECT 
     DEOM.Pat_Acct_Num, 
     DEOM.PE_Date, 
     DEOM.IPLAN_Insurance_Order_Num, 
     DEOM.Company_Code, 
     DEOM.Coid, 
     --cast(DEOM.Write_Off_Denial_Account_Amt as varchar(22)),  --modified  on 08/27
     cast (Case 
                             When DEOM.source_system_Code = 'N' Then CC_Cash_Adjustment_Amt
                             Else DEOM.Write_Off_Denial_Account_Amt 
                 End as Varchar(22)) as W_Off_Denial_Account_Amt ,
     ADM.Admission_Date,
     PTD.Patient_Type_Sid,     
     DEOM.Unit_Num AS Unit_Num_Sid,
     Case                                                                    
            When rpdeb.payor_id is Not Null                                     
                Then trim(rpdeb.payor_id)                                       
            When rpde.payor_id is Not Null                                      
                Then trim(rpde.payor_id)                                        
      End,    
      Case When Patient_Financial_Class_Code = 99  Then 21
               When Patient_Financial_Class_Code=15 Then 18
               When Patient_Financial_Class_Code=13 Then 5
               When Patient_Financial_Class_Code=4 Then 9
               When Patient_Financial_Class_Code=11 Then 15
               When Patient_Financial_Class_Code=7 Then 4
               When Patient_Financial_Class_Code=0 Then 22
               When Patient_Financial_Class_Code=1 Then 1
               When Patient_Financial_Class_Code=10 Then 14
               When Patient_Financial_Class_Code=14 Then 17
               When Patient_Financial_Class_Code=12 Then 16
               When Patient_Financial_Class_Code=3 Then 3
               When Patient_Financial_Class_Code=9 Then 12
               When Patient_Financial_Class_Code=6 Then 11
               When Patient_Financial_Class_Code=8 Then 6
               When Patient_Financial_Class_Code=2 Then 2
               When Patient_Financial_Class_Code=5 Then 10
              Else 23 
       End,
       Case When Payor_Financial_Class_Code=0 Then 23
                When Payor_Financial_Class_Code=1 Then 1
                When Payor_Financial_Class_Code=2 Then 2
                When Payor_Financial_Class_Code=3 Then 3
                When Payor_Financial_Class_Code=4 Then 9
                When Payor_Financial_Class_Code=5 Then 10
                When Payor_Financial_Class_Code=6 Then 11
                When Payor_Financial_Class_Code=7 Then 4
                When Payor_Financial_Class_Code=8 Then 6
                When Payor_Financial_Class_Code=9 Then 12
                When Payor_Financial_Class_Code=10 Then 14
                When Payor_Financial_Class_Code=11 Then 15
                When Payor_Financial_Class_Code=12 Then 16
                When Payor_Financial_Class_Code=13 Then 5
                When Payor_Financial_Class_Code=14 Then 17
                When Payor_Financial_Class_Code=15 Then 18
                When Payor_Financial_Class_Code=20 Then 20
                When Payor_Financial_Class_Code=99 Then 20 
                Else 24 
         End
 
FROM
     EDWPF_base_views.Denial_EOM DEOM
LEFT OUTER JOIN  EDWPF_base_views.ADMISSION ADM
ON ADM.Patient_DW_ID = DEOM.Patient_DW_ID
inner join Edwpf_base_views.Eis_Patient_Type_Dim PTD
on ('PT_' || DEOM.Patient_Type_Code) = PTD.Patient_Type_Member
and Patient_Type_Gen02 not like 'MC%'
Left Outer Join 
(                                                        
       Select     payor_dw_id, payor_id, eff_from_date, eff_to_date                    
       From     edwpbs_base_views.rcom_payor_dimension_eom                                  
) rpdeb (payor_dw_id, payor_id, eff_from_date, eff_to_date)                 
       On     DEOM.payor_dw_id = rpdeb.payor_dw_id                         
       And     cast(ADM.Admission_Date as date)  Between rpdeb.eff_from_date                  
       And     rpdeb.eff_to_date                                                
Left Outer Join 
(                                                        
       Select     payor_dw_id, payor_id, eff_from_date                                 
       From     edwpbs_base_views.rcom_payor_dimension_eom                                  
) rpde (payor_dw_id, payor_id, eff_from_date)                               
       On     DEOM.payor_dw_id = rpde.payor_dw_id                          
       And     rpde.eff_from_date is Null
 WHERE
W_Off_Denial_Account_Amt<>0 and 
cast((DEOM.PE_Date (format 'yyyymm')) as char(6)) =  cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))
)
K
(     Pat_Acct_Num, 
     PE_Date, 
     IPLAN_Insurance_Order_Num, 
     Company_Code, 
     Coid, 
     W_Off_Denial_Account_Amt,
     Admission_Date,
     Patient_Type_Sid,     
     Unit_Num_Sid,
     Payor_id,    
     Patient_Financial_Class_Sid,
     Payor_Financial_Class_Sid
)
left outer join edwpbs_base_views.eis_payor_dim PD
on PD.Payor_Member = K.Payor_id
left outer join edwpbs_base_views.fact_rcom_ar_patient_level fp
on fp.patient_sid = K.pat_acct_num 
and fp.company_code = K.company_code
and fp.coid = K.coid
and cast(cast((K.PE_Date (format 'yyyymm')) as char(6))  as integer)  = fp.date_sid
and fp.iplan_insurance_order_num = K.IPLAN_Insurance_Order_Num
and fp.payor_sid = PD.Payor_Sid
and fp.unit_num_sid = K.Unit_Num_Sid
and fp.patient_type_sid = K.Patient_Type_Sid
and fp.Patient_Financial_Class_Sid = K.Patient_Financial_Class_Sid
and fp.Payor_Financial_Class_sid = K.Payor_Financial_Class_sid
) K group by 1
) EXP1