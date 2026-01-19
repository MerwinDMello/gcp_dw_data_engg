select 'J_IM_MT_CL_USER_ACTIVITY_HIST' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT DISTINCT Network_Mnemonic_CS, MT_User_Mnemonic_CS FROM Edwim_Staging.MT_CL_User_Activity 
)A;