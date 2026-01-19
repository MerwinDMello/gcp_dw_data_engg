Select 'PBMAR710' || ',' || 
coalesce(trim(Count(Unit_Num_Sid)),'0') || ',' || 
coalesce(trim(cast(Sum(Cash_Receipt_Amt)as varchar(20))),'0') || ',' || 
coalesce(trim(cast(Sum(Gross_Revenue_Amt)as varchar(20))),'0')   || ',' as Source_String 
From edwpbs.Fact_RCOM_AR_Pat_FC_Level Where Time_Sid = CAST(((Add_Months(Current_Date,-1))(Format 'YYYYMM')) AS Char(6))