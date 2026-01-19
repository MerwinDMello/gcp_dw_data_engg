select 'J_IM_MT_CL_USER_ACTIVITY' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM_STAGING.MT_CL_User_Activity             
           
           )A;