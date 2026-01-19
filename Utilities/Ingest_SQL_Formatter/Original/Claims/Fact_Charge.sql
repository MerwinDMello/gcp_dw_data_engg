--FACT_Charge
SELECT CAST(Claim_ID as varchar(50)) as Claim_ID
      ,Charge_Seq_Num
      ,Revenue_Code as Charge_Revenue_Code
      ,NDC_Code as Charge_NDC_Code
     , NDC_Drug_Qty as Charge_NDC_Drug_Qty
       ,NDC_Drug_UoM as Charge_NDC_Drug_UoM
      ,HCPCS as Charge_HCPCS
      ,Charge_Rate as Charge_Rate_Value
      ,HCPCS_Modifier1 as Charge_HCPCS_Modifier1_Cd
      ,HCPCS_Modifier2 as Charge_HCPCS_Modifier2_Cd
      ,HCPCS_Modifier3 as Charge_HCPCS_Modifier3_Cd
      ,HCPCS_Modifier4 as Charge_HCPCS_Modifier4_Cd
      ,Service_Dt as Charge_Service_Dt
      ,Units_of_Service as Charge_Unit_of_Svc_Num
      ,Total_Amt as Charge_Total_Amt
      ,Cast(Non_Covered_Amt as decimal(18,3)) as Charge_Non_Covered_Amt
      ,NULL as DW_Last_Update_Date_Time
	  ,NULL as Source_System_Code
  FROM ClaimsConnectDB.dbo.Fact_Charge With (Nolock)