select 'J_IM_Meditech_Person_Activity_Ins' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts FROM  EDWIM.IM_PERSON_ACTIVITY        
           
           )A;