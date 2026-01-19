SELECT 'J_MHB_Ref_MHB_Dynamic_Role'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.REF_MHB_DYNAMIC_ROLE where dw_last_update_date_time(date)=current_date
 ) Q