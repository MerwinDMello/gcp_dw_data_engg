SELECT Claim_ID
      ,Code_Seq_Num
      ,Code_Type_ID
      ,Code_Value
      ,Code_Amt
      ,CAST(Code_From_Dt AS DATE) as Code_From_Dt
      ,CAST(Code_Thru_Dt AS DATE) as Code_Thru_Dt
      ,(case when Code_POA_Ind = 'Y' then 'Y'
       when code_poa_ind = 'N' then 'N'
      when code_poa_ind='U' then 'U'
      else null end) as CODE_POA_IND
      ,NULL as DW_Last_Update_Date_Time
	  ,NULL as Source_System_Code
  FROM ClaimsConnectDB.dbo.Fact_Code With (Nolock)