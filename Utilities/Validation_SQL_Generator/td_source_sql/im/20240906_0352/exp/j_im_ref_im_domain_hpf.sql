 select 'J_IM_Ref_IM_Domain_HPF' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
select  count(distinct Instance_Connection_String) as counts from  EDWDW_BASE_VIEWS.Document_Work_Flow_Instance) a