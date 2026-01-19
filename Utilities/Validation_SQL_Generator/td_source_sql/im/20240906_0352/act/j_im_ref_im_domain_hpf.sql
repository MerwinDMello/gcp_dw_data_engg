
select 'J_IM_Ref_IM_Domain_HPF' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
 SELECT COUNT(*) as counts FROM
 EDWIM.Ref_IM_Domain where
application_system_id=3)A