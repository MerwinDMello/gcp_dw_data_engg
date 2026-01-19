SELECT 'j_ADA_Patient_Level_Detail' || ',' || 
	coalesce(cast(trim(SUM (Inhouse_Charity_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Insured_Charity_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Insured_Self_Pay_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Uninsured_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Charity_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Patient_Liab_Prot_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Total_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Secn_Agcy_Unins_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Secn_Agcy_Charity_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Secn_Agcy_Pat_Liab_Prot_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Total_Secn_Agcy_Discount_Amt))as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Non_Secn_Unins_Disc_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Non_Secn_Charity_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Non_Secn_Patient_Liab_Prot_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Total_Non_Secn_Discount_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Self_Pay_AR_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Secn_Agcy_Acct_Bal_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Non_Secn_Self_Pay_AR_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Gross_Non_Secn_Self_Pay_AR_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Total_Patient_Due_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Discharged_Not_Final_Bill_Self_Pay_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Inhouse_Self_Pay_Amt) )as varchar (20)),'0')|| ',' || 
	coalesce(cast(trim(SUM (Discharged_Not_Final_Bill_Charity_Amt) )as varchar (20)),'0')|| ',' as Source_string
FROM
	
	EDWPBS.ADA_Patient_Level_Detail x WHERE Month_ID =  CAST( ( ADD_MONTHS(CURRENT_DATE , -1) (FORMAT 'YYYYMM') ) AS CHAR(6) )