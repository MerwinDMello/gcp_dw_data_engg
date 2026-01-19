select 'J_IM_MEDITECH_USER' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim.MEDITECH_User        
           
           )A;