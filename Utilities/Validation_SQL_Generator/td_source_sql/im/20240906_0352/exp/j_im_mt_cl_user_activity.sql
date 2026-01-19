select 'J_IM_MT_CL_USER_ACTIVITY' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT 
Company_Code as Company_Code
, Coid as coid
,UPPER(Audit_Event_User_Mnem_CS) AS User_Mnemonic
,Network_Mnemonic_CS as network_mnemonic_cs
,CAST(Audit_Event_Date_Time AS DATE) AS MT_Activity_Date
,CURRENT_TIMESTAMP(0) as time_stamp
FROM
EDWIM_BASE_VIEWS.Clinical_User_Patient_Audit
QUALIFY ROW_NUMBER() OVER(PARTITION BY Network_Mnemonic_CS, User_Mnemonic ORDER BY MT_Activity_Date DESC) = 1         
)A;