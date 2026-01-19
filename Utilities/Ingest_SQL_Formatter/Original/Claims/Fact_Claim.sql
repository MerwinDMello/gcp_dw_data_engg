--FACT_CLAIM
SELECT 
Claim_ID
    ,Vendor_CID      
	,Unit_Num
	,Unit_Num as Numeric_Unit_Num 	
    ,Numeric_Patient_Acct_Num
    ,Service_Code
    ,Taxonomy_Code
    ,Destination_Method
    ,Bill_Type_Code
	,stgb.Billing_Provider_SID as Bill_Provider_SID
	,stgp.Pay_To_Provider_SID as Pay_To_Provider_SID
    ,Total_Charge_Amt
    ,Payor_Seq_Ind      
	,IPlan_ID
    ,Bill_Dt
    ,(case SUBSTRING(patient_acc_num,1,1) 
                    when 'D' then SUBSTRING(patient_acc_num,2,39)
                    when 'R' then SUBSTRING(patient_acc_num,2,39)
                    when 'H' then SUBSTRING(patient_acc_num,2,39)
                    when 'S' then SUBSTRING(patient_acc_num,2,39)
                    when 'P' then SUBSTRING(patient_acc_num,2,39)                 
                    else patient_acc_num end)as Patient_Acct_Num
    ,Med_Rec_Num
    ,Fed_Tax_Num
    ,CAST(Stmt_Cover_From_Dt AS DATE) as Stmt_Cover_From_Dt
    ,CAST(Stmt_Cover_To_Dt AS DATE) as Stmt_Cover_To_Dt
    ,CAST(Admission_Dt AS DATE) as Admission_Dt
    ,cast(Admission_Hr as CHAR(2)) as Admission_Hr
    ,Admission_Type_Ind
    ,Admission_Source as Admission_Source_Cd
    ,cast(Discharge_Hr as CHAR(2)) as Discharge_Hr
    ,Discharge_Status as Discharge_Status_Cd
    ,Accident_State as Accident_St
    ,NPI
    ,Admit_Diag_Code
    ,DRG
    ,Claim_Remark as Claim_Desc
    ,SUBSTRING(Archive_File_Location, 1, DATALENGTH(Archive_File_Location) - CHARINDEX(CHAR(92),REVERSE(Archive_File_Location))) as File_Link_Path_Txt
    ,SUBSTRING(Archive_File_Location, DATALENGTH(Archive_File_Location) - CHARINDEX(CHAR(92),REVERSE(Archive_File_Location)) + 2, CHARINDEX(CHAR(92),REVERSE(Archive_File_Location)) - 1) as Claim_File_Name
    ,(case SUBSTRING(patient_acc_num,1,1) 
                    when 'D' then 'D' 
                    when 'R' then 'R'
                    when 'H' then 'H'
                    when 'S' then 'S'
                    when 'P' then 'P'                 
                    else null end) as Prefix_pat_acct_Num      
	,Vendor_CID as PAS_COID
	,Financial_Class
	,Patient_Type
    ,NULL as DW_Last_Update_Date_Time
    ,b.Source_System_Code as EDI_837_Type
    ,NULL as Source_System_Code
FROM ClaimsConnectDB.dbo.Fact_Claim b With (Nolock)
LEFT JOIN ClaimsConnectDB.dbo.Lu_Billing_Provider bp WITH (nolock)
ON bp.Bill_Provider_NPI = b.NPI and
bp.Billing_Provider_SID = b.Claim_ID
LEFT JOIN ClaimsConnectDB.dbo.Dw_Billing_Provider stgb WITH (nolock)
ON bp.Bill_Prov_Provider_City = stgb.Bill_Prov_Provider_City and
bp.Bill_Provider_Addr1 = stgb.Bill_Provider_Addr1 and
(stgb.Bill_Provider_Addr2 = bp.Bill_Provider_Addr2 or (bp.Bill_Provider_Addr2 is null and stgb.Bill_Provider_Addr2 is null)) and
bp.Bill_Provider_NPI = stgb.Bill_Provider_NPI and
bp.Bill_Provider_Name = stgb.Bill_Provider_Name and
bp.Bill_Provider_St = stgb.Bill_Provider_St and
bp.Bill_Provider_Zip =stgb.Bill_Provider_Zip and
stgb.Bill_Provider_NPI = bp.Bill_Provider_NPI
LEFT JOIN ClaimsConnectDB.dbo.Lu_Pay_To_Provider pp WITH (nolock)
ON pp.Provider_NPI = b.NPI and
pp.Pay_To_Provider_SID = b.Claim_ID
LEFT JOIN ClaimsConnectDB.dbo.Dw_Pay_To_Provider stgp WITH (nolock)
ON pp.Provider_City = stgp.Provider_City and
pp.Provider_Addr1 = stgp.Provider_Addr1 and
(stgp.Provider_Addr2 = pp.Provider_Addr2 or (pp.Provider_Addr2 is null and stgp.Provider_Addr2 is null)) and
pp.Provider_NPI = stgp.Provider_NPI and
(pp.Provider_Name = stgp.Provider_Name or (pp.Provider_Name is null and stgp.Provider_Name is null)) and
pp.Provider_St = stgp.Provider_St and
pp.Provider_Zip =stgp.Provider_Zip and
stgp.Provider_NPI = pp.Provider_NPI