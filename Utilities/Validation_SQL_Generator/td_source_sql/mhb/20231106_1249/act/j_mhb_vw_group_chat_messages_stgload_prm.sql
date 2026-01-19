SELECT 'J_MHB_Vw_Group_Chat_Messages_StgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_Staging.vwGroupChatMessages