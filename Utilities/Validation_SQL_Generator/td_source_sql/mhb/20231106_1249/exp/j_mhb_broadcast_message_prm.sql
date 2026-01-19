SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM EDWCI_Staging.MHB_Broadcast_Message_WRK