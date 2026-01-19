select 'J_IM_MT_CDM_USER_3_4' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CDM_User_3_4         
           
           )A;