select 'J_IM_MEDITECH_USER_ACTIVITY_ARCHIVE' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT
T2.IM_Domain_Id
,T1.MT_User_Id
,T2.eSAF_Activity_Date
,T1.MT_User_Last_Activity_Date
,T1.MT_User_Activity_Sw
,T1.MT_Excluded_User_Sw
,T1.MT_Staff_PM_User_Sw
,T1.MT_PCP_User_Sw
,T1.MT_Alias_Exempt_Sw
,T1.MT_Linked_User_Sw
,T1.MT_Open_Deficiency_Sw
,T1.DW_Last_Update_Date_Time
FROM 
EDWIM_BASE_VIEWS.MEDITECH_USER_ACTIVITY T1	
INNER JOIN 
EDWIM_BASE_VIEWS.IM_PERSON_ACTIVITY T2
ON T1.MT_User_Id = T2.IM_Person_User_Id
AND T1.IM_DOMAIN_ID = T2.IM_DOMAIN_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY T2.IM_Domain_Id, T1.MT_User_Id  ORDER BY  MT_User_Last_Activity_Date DESC) = 1    
)A;