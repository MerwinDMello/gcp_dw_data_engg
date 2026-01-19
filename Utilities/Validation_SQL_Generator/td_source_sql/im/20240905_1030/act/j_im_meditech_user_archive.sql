select 'J_IM_MEDITECH_User_Archive' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM.MEDITECH_User_Activity_Archive where cast(dw_last_update_date_time as date)=current_date      
           
)A;