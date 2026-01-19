select 'J_IM_ESAF_Provisioning_Event_Time_Archive' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
 (
SELECT 
T2.IM_Domain_Id
,T1.eSAF_User_Id
,T2.eSAF_Activity_Date
,T1.eSAF_Application_Id
,T1.eSAF_Operation_Type
,T1.eSAF_Event_Time
,T1.Source_System_Code
,T1.DW_Last_Update_Date_Time
FROM  EDWIM.eSAF_Provisioning_Event_Time T1
INNER JOIN EDWIM_BASE_VIEWS.IM_Person_Activity T2
ON T1.eSAF_User_Id = T2.IM_Person_User_Id
QUALIFY ROW_NUMBER() OVER (PARTITION BY  T2.IM_Domain_Id,  T1.eSAF_User_Id,  T2.eSAF_Activity_Date, T1.eSAF_Application_Id
ORDER BY  T1.eSAF_Event_Time DESC) = 1
)A;