
select 'J_IM_MT_REF_IM_DOMAIN' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
 sel count(*) as counts from  edwim.ref_im_domain
 where cast (Dw_Last_Update_Date_Time as date) = current_date
 and Application_System_Id in (5,6)
)A