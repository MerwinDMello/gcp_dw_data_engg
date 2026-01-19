select 'J_IM_Meditech_User_Activity_Ins' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM.Meditech_User_Activity        
           
           )A;