SELECT Claim_ID
      ,Payor_Seq_Ind
	  ,ID_Plan as IPlan_ID
      ,Payer_Id
      ,Payer_Sub_id
      ,Payer_Name
      ,Health_Plan_Id
      ,Release_Info_Ind as Release_Info_Cert_Desc
      ,Assign_Benefit_Ind as Assign_Benefit_Cert_Desc
      ,Prior_Pay_Amt
      ,Est_Due_Amt
      ,Oth_Provider_ID as Other_Provider_ID
      ,Insured_Name
      ,Pat_Rel_To_Ins_Ind as Pat_to_Ins_Rel_Ind
      ,Insured_ID
      ,Insured_Grp_Name as Insured_Group_Name
      ,Insured_Grp_Num as Insured_Group_Num
      ,Treatment_Auth_Code
      ,Employer_Name as Employer_Name
      ,Doc_Ctrl_Num as Doc_Cntrl_Num
      ,NULL as DW_Last_Update_Date_Time
      ,NULL as Source_System_Code
FROM ClaimsConnectDB.dbo.Fact_Claim_Insurance ins With (Nolock)