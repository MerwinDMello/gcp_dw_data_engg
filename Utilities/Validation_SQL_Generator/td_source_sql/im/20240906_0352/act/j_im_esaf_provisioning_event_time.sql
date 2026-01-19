select 'J_IM_ESAF_Provisioning_Event_Time' ||','||cast(A.counts as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.eSAF_Provisioning_Event_Time                
)A;