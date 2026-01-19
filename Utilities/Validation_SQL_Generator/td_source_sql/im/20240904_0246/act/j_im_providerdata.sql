select 'J_IM_ProviderData' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM_Staging.Stag_MD_Staff_Provider_Detail        
)A;