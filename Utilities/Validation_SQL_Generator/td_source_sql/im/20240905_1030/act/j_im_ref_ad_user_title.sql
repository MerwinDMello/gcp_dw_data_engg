  select 'J_IM_Ref_IM_Domain' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
FROM EDWIM.Ref_AD_User_Title                
           
           )A;