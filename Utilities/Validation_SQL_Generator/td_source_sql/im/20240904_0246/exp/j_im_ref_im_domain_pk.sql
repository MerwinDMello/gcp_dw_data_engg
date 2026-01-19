 select 'J_IM_Ref_IM_Domain_PK' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
select  count(distinct PK_Database_Instance_Code) as counts from  EDWIM_BASE_VIEWS.Ref_PK_Data_Base_Instance) a