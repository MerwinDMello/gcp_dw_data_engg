select 'J_IM_MEDITECH_User_Archive' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT
T2.IM_Domain_Id
,T1.MT_User_Id
,T2.eSAF_Activity_Date
,T1.National_Provider_Id
,T1.Facility_Mnemonic_CS
,T1.MT_User_Full_Name
,T1.MT_User_Mnemonic_CS
,T1.MT_User_Page_1_Provider_Type_Desc
,T1.MT_User_Page_2_Provider_Type_Desc
,T1.MT_User_Exempt_Sw
,T1.MT_User_Active_Ind
,T1.MT_User_MIS_User_Mnemonic
,T1.MT_Linked_User_Code
,T1.MT_User_Last_Activity_Date
,T1.Source_System_Code
,T1.DW_Last_Update_Date_Time
FROM 
EDWIM_BASE_VIEWS.MEDITECH_USER T1	
INNER JOIN 
EDWIM_BASE_VIEWS.IM_PERSON_ACTIVITY T2
ON T1.MT_User_Id = T2.IM_Person_User_Id
AND T1.IM_DOMAIN_ID = T2.IM_DOMAIN_ID
 UNION
 
 SELECT	
T3.IM_Domain_Id, 
T1.MT_User_Id, 
T3.eSAF_Activity_Date,
T1.National_Provider_Id,
T1.Facility_Mnemonic_CS,
T1.MT_User_Full_Name, 
T1.MT_User_Mnemonic_CS, 
T1.MT_User_Page_1_Provider_Type_Desc,
T1.MT_User_Page_2_Provider_Type_Desc, 
T1.MT_User_Exempt_Sw,
T1. MT_User_Active_Ind,
T1.MT_User_MIS_User_Mnemonic, 
T1.MT_Linked_User_Code, 
T1.MT_User_Last_Activity_Date,
T1.Source_System_Code, 
T1.DW_Last_Update_Date_Time
		
FROM	EDWIM_BASE_VIEWS.MEDITECH_User T1
INNER JOIN
				(
									SELECT DISTINCT
									
									   H2.HPF_Domain_Id,
									   H2.MT_Domain_Id,                                
									   H1.MT_User_Id
									                                          									
									FROM EDWIM_BASE_VIEWS.MEDITECH_User H1									
									JOIN  EDWIM_BASE_VIEWS.Platform_Domain_Xwalk H2																						
									ON H1.IM_Domain_Id = H2.MT_Domain_Id  
									
									 
				)T2
				ON T1.IM_Domain_Id = T2.MT_Domain_Id
		       AND T1.MT_User_Id = T2.MT_User_Id
INNER JOIN EDWIM_BASE_VIEWS.IM_Person_Activity T3
ON  T2.MT_User_Id = T3.IM_Person_User_Id
AND  T2.HPF_Domain_Id = T3.IM_Domain_Id
QUALIFY ROW_NUMBER() OVER (PARTITION BY T3.IM_Domain_Id, T1.MT_User_Id, T1.Facility_Mnemonic_CS ORDER BY   T1.MT_User_Exempt_Sw, MT_User_Last_Activity_Date DESC) = 1     
)A;