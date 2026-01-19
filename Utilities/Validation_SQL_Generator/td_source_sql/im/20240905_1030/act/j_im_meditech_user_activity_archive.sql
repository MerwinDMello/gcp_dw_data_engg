select 'J_IM_MEDITECH_USER_ACTIVITY_ARCHIVE' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM.MEDITECH_USER_ACTIVITY_ARCHIVE where cast(dw_last_update_date_time as date)=current_date      
           
)A;