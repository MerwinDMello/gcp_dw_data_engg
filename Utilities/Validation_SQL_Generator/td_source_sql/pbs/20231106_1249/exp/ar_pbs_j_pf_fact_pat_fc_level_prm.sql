Select 
'PBMAR710' ||','||
trim(Count(X.Unit_Num_Sid)) ||','||
trim(Sum(X.Cash_Receipt_Amt)) ||','||
trim(cast(Sum(X.Gross_Revenue_Amt)as decimal(15,2)))  ||',' as Source_string
From 
(
Select 
Z.Unit_Num_Sid
,Z.Patient_Financial_Class_Sid
,Z.Patient_Type_Sid
,Z.Scenario_Sid
,Z.Time_Sid
,Z.Source_Sid
,Z.Company_Code
,Z.Coid
,Sum(Z.Cash_Receipt_Amt) AS Cash_Receipt_Amt
,Sum(Z.Gross_Revenue_Amt) AS Gross_Revenue_Amt
From 
(Select 
SAR.Unit_Num AS Unit_Num_Sid
,Case When SAR.Financial_Class_Code = 999 Then 23 
	Else EPF.Patient_Financial_Class_Sid 
End AS Patient_Financial_Class_Sid
,1 AS Scenario_Sid
,SAR.Rptg_Period AS Time_Sid
,1 AS Source_Sid
,Case When SAR.Derived_Patient_Type_Code = 'NA' Then 40
	Else PT.Patient_Type_Sid 
End AS Patient_Type_Sid
,SAR.Company_Code AS Company_Code
,SAR.Coid AS Coid
,Sum(SAR.Cash_Amt) AS Cash_Receipt_Amt
,Sum(0.000) AS Gross_Revenue_Amt
From Edwpf_base_views.Smry_AR_Cash_Collections SAR
Left Outer Join Edwpf_base_views.EIS_Patient_Fin_Class_Dim EPF
on Substr(EPF.Patient_Financial_Class_Member,5,2) = SAR.Financial_Class_Code
And EPF.Patient_Financial_Class_Member <> 'No_FC'
Left Outer Join Edwpf_base_views.EIS_Patient_Type_Dim PT
on PT.Patient_Type_Member = 'PT_' || Case When Trim(SAR.Derived_Patient_Type_Code) = 'IE' Then 'E' Else SAR.Derived_Patient_Type_Code End
and Substr(PT.Patient_Type_Gen02,1,2) <> 'MC'
Where SAR.Payor_DW_ID <> 999
and SAR.Rptg_Period = CAST(((Add_Months(Current_Date,-1))(Format 'YYYYMM')) AS Char(6)) 
Group by 1,2,3,4,5,6,7,8
Union 
Select 
FD.Unit_Num AS Unit_Num_Sid
,EPF.Patient_Financial_Class_Sid AS Patient_Financial_Class_Sid
,1 AS Scenario_Sid
,CAST(((Add_Months(Current_Date,-1))(Format 'YYYYMM')) AS Char(6)) AS Time_Sid
,1 AS Source_Sid
,CASE When RD.Data_Type_Text = 'IP AMOUNT' Then 21
	When RD.Data_Type_Text = 'OP AMOUNT' Then 27
End AS Patient_Type_Sid
,FD.Company_Code AS Company_Code
,RD.Coid AS Coid
,Sum(0.000) AS Cash_Receipt_Amt
,Sum(RD.Month_Minus_1_Amt) AS Gross_Revenue_Amt
From Edwpf_base_views.RevStats_Department RD
Join EDWPF_base_views.FACILITY_DIMENSION FD
on FD.COID = RD.COID
Left Outer Join Edwpf_base_views.EIS_Patient_Fin_Class_Dim EPF
on Substr(EPF.Patient_Financial_Class_Member,5,2) = RD.Financial_Class_Code
And EPF.Patient_Financial_Class_Member <> 'No_FC'
Where RD.Data_Type_Text  in ('IP AMOUNT','OP AMOUNT')
AND RD.Sub_Unit_Num <> '00000'
AND (FD.Company_Code = 'H' or Substr(Trim(FD.Company_Code_Operations),1,1) = 'Y')
Group by 1,2,3,4,5,6,7,8) Z
Group by 1,2,3,4,5,6,7,8) X