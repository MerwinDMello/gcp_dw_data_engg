select 'J_IM_Ref_Job_Code' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.Ref_IM_Job_Code                
)A;