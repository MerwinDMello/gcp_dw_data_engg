select 'J_IM_ESAF_Facility_Business_Unit' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.eSAF_Facility_Business_Unit                
)A;