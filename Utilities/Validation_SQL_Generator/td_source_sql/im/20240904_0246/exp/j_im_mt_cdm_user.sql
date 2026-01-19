select 'J_IM_MT_CDM_USER' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT 
t3.Company_Code as company_code
,t3.Coid as Coid
,t2.MT_User_Mnemonic_CS as MT_User_Mnemonic_CS
,t1.MT_User_Id as MT_User_Id
,CURRENT_TIMESTAMP(0) as  time_stamp
FROM EDWIM_STAGING.MT_CDM_User_3_4 t1
INNER JOIN
EDWIM_STAGING.MT_CDM_User_Mnem t2
ON t1.ROLE_PLYR_SK = t2.ROLE_PLYR_SK
INNER JOIN
EDWIM_STAGING.MT_CDM_User_Coid t3
ON t1.ROLE_PLYR_SK = t3.ROLE_PLYR_SK
        
)A;