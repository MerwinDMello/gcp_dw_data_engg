select 'J_IM_MT_CL_HCP_VC' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CL_HCP_VC        
           
           )A;