SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM 
(
SEL CASE WHEN coalesce(Y.User_Action_Type_Desc,'Unknown') = 'Unknown' THEN -1 
	   ELSE (SEL COALESCE(MAX(User_Action_Type_SID),0) 
		   FROM EDWCI_BASE_VIEWS.Ref_MHB_User_Action_Type) + ROW_NUMBER() OVER (ORDER BY Y.User_Action_Type_Desc) 
    END AS User_Action_Type_SID,  
    Y.User_Action_Type_Desc AS User_Action_Type_Desc,
    'B' AS SOURCE_SYSTEM_CODE,
    CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM 
	(SEL * FROM (
		SEL trim(User_Action) AS User_Action_Type_Desc
 		FROM EDWCI_STAGING.vwWCTPInboundMessages
 		/*UNION 
 		SEL 'Unknown' As User_Action_Type_Desc
 		FROM EDWCI_STAGING.vwWCTPInboundMessages*/
 		group by 1)X
 	WHERE X.User_Action_Type_Desc NOT IN (
		SEL	User_Action_Type_Desc 
		FROM	EDWCI_BASE_VIEWS.Ref_MHB_User_Action_Type  ))Y) A
