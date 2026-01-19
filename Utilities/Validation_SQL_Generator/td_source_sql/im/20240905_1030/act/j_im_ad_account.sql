select 'J_IM_AD_Account' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM_staging.AD_Account_Stg                
)A;