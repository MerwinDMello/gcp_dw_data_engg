select 'J_IM_Cactus_Provider' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM.Cactus_Provider        
           
           )A;