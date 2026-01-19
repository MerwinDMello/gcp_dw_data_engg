SELECT 'J_MHB_Alerting_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_ALERTING_AUDIT_DETAIL where dw_last_update_date_time(date)=current_date
 ) Q