select 'J_IM_Ref_AD_Employee_Type' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.Ref_AD_Employee_Type                
)A;