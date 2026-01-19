Select 

'ONE' as Artiva_Instance_Code,
Current_Date as Load_Date,
SQLUser.CSCHARITY.CSCHARHOSPNUM as Unit_Num

, SQLUser.CSCHARITY.CSCHARPATACCT as Pat_Acct_Num

, SQLUser.CSCHARITY.CSCHARFAA as FAA_Returned_Flag

, SQLUser.CSCHARITY.CSCHARFAADTE as Charity_FAA_Flag_Date

, SQLUser.CSCHARITY.CSCHAREXTCIRCUM as Extenuating_Circumstances

, SQLUser.CSCHARITY.CSCHARECOTHREAS as Extenuating_Circumstance_Other

, SQLUser.CSCHARITY.CSCHARAPPEAL as Appeal_Flag

, SQLUser.CSCHARITY.CSCHARFAMSIZE as Family_Size

, SQLUser.CSCHARITY.CSCHARHINCOME as Household_Income

, SQLUser.CSCHARITY.CSCHARPOVLEVL as Charity_Poverty_Level

, SQLUser.CSCHARITY.CSCHARAPPDEN as HCA_Approve_Deny_Flag

, SQLUser.CSCHARITY.CSCHARDENREAS as Denial_Reason

, SQLUser.CSCHARITY.CSCHARDENLAMT as Charity_Denial_Amount

, SQLUser.CSCHARITY.CSCHARAPPDENUSR as Approve_Deny_User

, SQLUser.CSCHARITY.CSCHARAPPDENDTE as Approve_Deny_Date

, SQLUser.CSCHARITY.CSCHARSAFAMSIZE as Inc_Val_Family_Size

, SQLUser.CSCHARITY.CSCHARSAINCOME as Inc_Val_Income

, SQLUser.CSCHARITY.CSCHARSAPOVLEVL as Inc_Val_Poverty_Level

, SQLUser.CSCHARITY.CSCHARSAAPPDEN as Inc_Val_Approve_Deny_Flag

, SQLUser.CSCHARITY.CSCHARSARETNDTE as Inc_Val_Response_Date

, SQLUser.CSCHARITY.CSCHARSASENTDTE as Inc_Val_Sent_Date

, SQLUser.CSCHARITY.CSCHARWKINCRP as RespParty_Weekly_Income

, SQLUser.CSCHARITY.CSCHARMOINCRP as RespParty_Monthly_Income

, SQLUser.CSCHARITY.CSCHARYRINCRP as RespParty_Yearly_Income

, SQLUser.CSCHARITY.CSCHARLSTWRKDTE as Uneomplyed_Last_Work_Date

, SQLUser.CSCHARITY.CSCHARWKINCSP as Spouse_Weekly_Income

, SQLUser.CSCHARITY.CSCHARMOINCSP as Spouse_Monthly_Income

, SQLUser.CSCHARITY.CSCHARYRINCSP as Spouse_Yearly_Income

, SQLUser.CSCHARITY.CSCHARAGE as Age_In_Charity_Process

, SQLUser.CSCHARITY.CSCHARMEETSFPG as Meets_Fed_Poverty_Guidelines

, Cast(SQLUser.CSCHARITY.CSCHARDISCPCT as decimal(18,0)) as Calculated_Discount_Percent

, SQLUser.CSCHARITY.CSCHARDISCAMT as Discount_Amount

, LEFT(SQLUser.CSCHARITY.CSCHARIPLAN,5) as Iplan_Id

, SQLUser.CSCHARITY.CSCHARW2 as W2_Return_Flag

, SQLUser.CSCHARITY.CSCHARFED1040 as Federal_1040_Returned_Flag

, SQLUser.CSCHARITY.CSCHARSTATETAX as State_Tax_Returned_Flag

, SQLUser.CSCHARITY.CSCHARWAGESTMT as Employer_Wage_Returned_Flag

, SQLUser.CSCHARITY.CSCHARBANKSTMT as Bank_Statement_Returned_Flag

, SQLUser.CSCHARITY.CSCHARDOS as Calc_Poverty_Lvl_By_DOS_Flag

, SQLUser.CSCHARITY.CSCHARCREDRPT as Credit_Report_Returned_Flag

, SQLUser.CSCHARITY.CSCHAR1099 as Form_1099_Returned_Flag

, SQLUser.CSCHARITY.CSCHARQMB as QMB_Returned_Flag

, SQLUser.CSCHARITY.CSCHAROTHER as Other_Documents_Returned_Flag

, substring(SQLUser.CSCHARITY.CSCHAROTHERDESC,1,45) as Other_Documents_Description

,Case when (Trim(SQLUser.CSCHARITY.CSCHARAPPLEVL) is null or  Trim(SQLUser.CSCHARITY.CSCHARAPPLEVL) ='' or  Trim(SQLUser.CSCHARITY.CSCHARAPPLEVL) ='?' ) then 0 else SQLUser.CSCHARITY.CSCHARAPPLEVL end as Application_Approval_Level

, SQLUser.CSCHARITY.CSCHARCURAPPLEVL as Current_Approval_Level

, SQLUser.CSCHARITY.CSCHARAPPLEVL1USR as Approval_Level_1_User_Id

, SQLUser.CSCHARITY.CSCHARAPPLEVL2USR as Approval_Level_2_User_Id

, SQLUser.CSCHARITY.CSCHARAPPLEVL3USR as Approval_Level_3_User_Id

, SQLUser.CSCHARITY.CSCHARAPPLEVL4USR as Approval_Level_4_User_Id

, SQLUser.CSCHARITY.CSCHARAPPLEVL5USR as Approval_Level_5_User_Id

, SQLUser.CSCHARITY.CSCHARAPPLVL1DTE as Approval_Level_1_Date

, SQLUser.CSCHARITY.CSCHARAPPLVL2DTE as Approval_Level_2_Date

, SQLUser.CSCHARITY.CSCHARAPPLVL3DTE as Approval_Level_3_Date

, SQLUser.CSCHARITY.CSCHARAPPLVL4DTE as Approval_Level_4_Date

, SQLUser.CSCHARITY.CSCHARAPPLVL5DTE as Approval_Level_5_Date

, SQLUser.CSCHARITY.CSCHARAGEDTE as Charity_Review_Aging_Date

, SQLUser.CSCHARITY.CSCHARCOMPUSR as User_That_Completed_Appl

, SQLUser.CSCHARITY.CSCHARSTARTDTE as Start_Charity_Review_Date

, SQLUser.CSCHARITY.CSCHARENDDTE as End_Charity_Review_Date

, SQLUser.CSCHARITY.CSCHARSENDTOSA as Send_To_Inc_Val_Flag

FROM SQLUser.CSCHARITY