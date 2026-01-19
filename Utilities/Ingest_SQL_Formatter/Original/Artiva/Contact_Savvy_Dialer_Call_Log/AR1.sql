SELECT	
		 --CTC Rows will duplicate in the case of split calls.  These calls in CTCALLLOG (CTCLKEY) will have more than one entry in STMULTIMEDIA (STM.STMMKEY)
		 --Be careful to only use distinct CTCLKEY to identify individual calls.
		 CAST(DATEPART(yyyy, CTC.CTCLINITDT) as CHAR(4)) || $MVFMT(CAST(DATEPART(mm, CTC.CTCLINITDT) as CHAR(2)), '2"0"R') as Rptg_Period
        ,'TMP' AS Artiva_Instance
		 ,PAS.HCPSID AS SSC
		 
		 ,CPT.CSPERSISTTIMEFAC --Facility
		 ,CPT.CSPERSISTTIMEID --Account ID (HCAID)
		 ,CPT.CSPERSISTTIMEPTNUM --Patient #
		 
		 ,FAC.CSFACOIDNUM --COID #
		 ,FAC.CSFAHOSPNUM --Facility #
		 
		 ,CTC.CTCLKEY --Call Log Key 
		 ,CTC.CSCLHOLDTIME --Hold Time (Seconds)
		 ,CTC.CSCTCLPOOL --CT Call Log Pool (Character)
		 ,CTC.CTCLACCT--Account/Record ID for Call
		 --,CTC.CTCLADDTI--Date Number Dialed
		 ,CAST(CTC.CTCLADDTI as VARCHAR(10)) as CTC_Dialed_Date --Date Number Dialed
		 ,(CONVERT(INT, CTC.CTCLADDTI))  as CTC_Dialed_Date_Int
		 --,CTC.CTCLADTMI --Time Number Dialed
		 ,CAST(CTC.CTCLADTMI as VARCHAR(8)) as CTC_Dialed_Time --Time Number Dialed
		 ,(CONVERT(INT, CTC.CTCLADTMI))  as CTC_Dialed_Time_Int
		 ,CTC.CTCLMANUAL --Manual Time for Call
		 ,CTC.CTCLITALK --Inbound Talk Time for Call
		 ,CTC.CTCLTALK--Talk Time for Call
		 ,CTC.CTCLOTALK --Outbound Talk Time for Call
		 ,CTC.CTCLPHONE --Phone Number Dialed for Call
		 ,CTC.CTCLPOOL --Pool for Call
		 ,CTC.CTCLRECFILE --VoiceTrak Record File for Call
		 ,CTC.CTCLRECORD --VoiceTrak Recorded for Call
		 ,CTC.CTCLUPDATE --Update Time for Call
		 ,CTC.CTCLWAIT --Wait Time for Call 
		 ,CTC.CTCLTYPE --Type of Call
		 ,CASE 
            WHEN CTC.CTCLTYPE = 'PG' THEN 'Progressive'
            ELSE 'Manual'
     	END AS Call_Type
		,CTC.CTCLAGENT --Agent ID for Call
		,substr(CTC.CTCLAGENT,1,7) as Agent_ID	
		--,CTC.CTCLINITDT --Date Call Initiated/Received
		,CAST(CTC.CTCLINITDT as VARCHAR(10)) as CTC_Init_Date --Date Call Initiated/Received
		 ,(CONVERT(INT, CTC.CTCLINITDT))  as CTC_Init_Date_Int
		--,CTC.CTCLINITTM --Time Call Initiated/Received
		,CAST(CTC.CTCLINITTM as VARCHAR(8)) as CTC_Call_Init_Time --Time Call Initiated/Received
		,(CONVERT(INT, CTC.CTCLINITTM)) as CTC_Call_Init_Time_Int
		--,CTC.CTCLENDDT --Date Call Terminated
		 ,CAST(CTC.CTCLENDDT as VARCHAR(10)) as CTC_End_Date --Date Call Terminated
		 ,(CONVERT(INT, CTC.CTCLENDDT))  as CTC_End_Date_Int
		--,CTC.CTCLENDTM --Time Call Terminated
		,CAST(CTC.CTCLENDTM as VARCHAR(8)) as CTC_Call_End_Time --Time Call Terminated
		,(CONVERT(INT, CTC.CTCLENDTM)) as CTC_Call_End_Time_Int
		,((CONVERT(INT, CTC.CTCLENDTM)) - (CONVERT(INT, CTC.CTCLINITTM)) ) as CTC_Call_Length
		,(((CONVERT(INT, CTC.CTCLENDTM)) - (CONVERT(INT, CTC.CTCLINITTM)) )/60)  as CTC_Call_Length_Min
		,CASE
			WHEN (((CONVERT(INT, CTC.CTCLENDTM)) - (CONVERT(INT, CTC.CTCLINITTM)) )/60)  >= 1 THEN 'N'
			WHEN (((CONVERT(INT, CTC.CTCLENDTM)) - (CONVERT(INT, CTC.CTCLINITTM)) )/60)  <1 THEN 'Y'
		END as CTC_Short_Call
		,STU.HCUADEPT --User's Department or Group
	    ,substr(STU.HCUADEPT,1,3) as Department
		,STU.UAFULLNAME --User's Full Name
		
		 ,GCP.GCDID --Dialer ID/Name
		 ,GCP.GCPOOLNUM --Pool Number (Key)
		 ,GCP.GCPDESC --Pool Description
		 ,GCP.GCPTYPE --Dialing Status of Pool
		 ,CASE
	        WHEN GCP.GCPTYPE = 'D' THEN 'Progressive'
	        WHEN GCP.GCPTYPE = 'N' AND GCP.GCDID = 'HCACTS' THEN 'Manual'
	        WHEN GCP.GCPTYPE = 'N' AND GCP.GCDID IS NULL THEN 'Non-Dialing'
    	END as Dialer_Type

FROM   SQLUser.CTCALLLOG CTC

LEFT JOIN SQLUser.GCPOOL GCP
    ON CTC.CSCTCLPOOL=GCP.GCPOOLNUM
	
LEFT JOIN SQLUser.STUSER STU
    ON CTC.CTCLAGENT=STU.USERID
	
LEFT JOIN 
	(SELECT 
		DISTINCT
			CSPERSISTTIMEFAC --Facility
			,CSPERSISTTIMEID --Account ID (HCACID)
			,CSPERSISTTIMEPTNUM --Patient #
	FROM	SQLUser.CSPERSISTTIMEACCT
	WHERE  CSPERSISTTIMEFAC IS NOT NULL
		AND CSPERSISTTIMEID IS NOT NULL
	) CPT
	ON CTC.CTCLACCT = CPT.CSPERSISTTIMEID

LEFT JOIN SQLUser.HCFACILITY FAC
	ON CPT.CSPERSISTTIMEFAC = FAC.HCFAID

LEFT JOIN SQLUser.HCPAS PAS
	ON FAC.HCFAPASID = PAS.HCPSID
	
	--Get Iplan and MPG
	
 WHERE CTC.CTCLINITDT  = DATEADD(DD,-1,CURRENT_DATE)
             AND (STU.HCUADEPT LIKE '902%' 
                    OR STU.HCUADEPT LIKE '602%'
					OR STU.HCUADEPT LIKE '913%' 
                    OR STU.HCUADEPT LIKE '613%'
					OR STU.HCUADEPT LIKE '916%' 
                    OR STU.HCUADEPT LIKE '616%')
					
ORDER BY  CTC.CTCLKEY