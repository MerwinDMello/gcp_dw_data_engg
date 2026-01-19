SELECT
	Current_Date AS Report_Date ,
	'AR1' AS Artiva_Instance ,
	Cast(RIGHT('00000' || CSMRHFACILITY, 5) AS CHAR(5)) AS Unit_Num ,
	CASE
		WHEN CSMRHPATACCT = 0
		THEN E.HCENPTACCT 
		ELSE CSMRHPATACCT
	END AS Pat_Acct_Num ,
	E.HCENBAL AS Encounter_Balance ,
	E.HCENORIGBAL AS Total_Charges ,
	E.HCENTOTADJ*-1 AS Total_Adjustments ,
	E.HCENTOTPAY AS Total_Payments ,
	E.HCENACCTBAL AS Total_Balance_Of_Liabilities ,
	--Doc Request Main Screen Fields
	substring(CSMRHID,1,19)  AS Doc_Req_ID ,
	substring(CSMRHIPLAN,1,5) AS Doc_Req_Iplan_ID ,
	substring(CSMRHPAYERDESC,1,30) AS Doc_Req_Payer_Name , 
	substring(CSMRHSTATUS,1,6) AS Doc_Req_Status ,
	CSMRHDTE AS Doc_Req_Submit_Date ,
	CSMRHREQDTE AS Doc_Req_Request_Date ,
	CSMRHSENTDTE AS Doc_Req_Sent_Date ,
	CSMRHRECVDDTE AS Doc_Req_Received_Date ,
	--Doc Request History Detail Screen Fields
	substring(CSMRHIPLAN,1,5) AS Doc_Req_Detail_Iplan_ID ,
	substring(CSMRHPAYERDESC,1,30) AS Doc_Req_Detail_Payer_Name , 
	CSMRHDTE AS Doc_Req_Detail_Create_Date ,
	substring(CSMRHREQUSERID,1,8) AS Doc_Req_Detail_User_ID ,
	substring(CSMRHDEPT,1,20) AS Doc_Req_Detail_Dept ,
	substring(CSMRHREVIEWTYPE,1,30) AS Doc_Req_Detail_Review_Type , 
	CSMRHREQDTE AS Doc_Req_Detail_Request_Date ,
	substring(CSMRHREQREASON,1,50) AS Doc_Req_Detail_Request_Reason,
	substring(CSMRHREQOTHREASON,1,50) AS Doc_Req_Detail_Request_Other_Reason , 
	--Docs Requested Detail Portion of Screen
	substring(CSMRHREQMEDREC,1,1) AS Doc_Req_Detail_Comp_Med_Rec_Ind , 
	substring(CSMRHAPPMEDREC,1,1) AS Doc_Req_Detail_Comp_Med_Rec_Approved ,
	substring(CSMRHREQADMITORDER,1,1) AS Doc_Req_Detail_Admit_Order_Ind ,
	substring(CSMRHAPPADMITORDER,1,1) AS Doc_Req_Detail_Admit_Order_Approved ,
	substring(CSMRHREQDSCHSUMMARY,1,1) AS Doc_Req_Detail_Dsh_Summary_Ind ,
	substring(CSMRHAPPDSCHSUMMARY,1,1) AS Doc_Req_Detail_Dsh_Summary_Approved ,
	substring(CSMRHREQERREPORT,1,1) AS Doc_Req_Detail_ER_Report_Ind , 
	substring(CSMRHAPPERREPORT,1,1) AS Doc_Req_Detail_ER_Report_Approved ,
	substring(CSMRHREQACCDETAIL,1,1) AS Doc_Req_Detail_Accident_Det_Ind ,
	substring(CSMRHAPPACCDETAIL,1,1) AS Doc_Req_Detail_Accident_Det_Approved ,
	substring(CSMRHREQITEMIZEDBILL,1,1) AS Doc_Req_Detail_Itemized_Bill_Ind ,
	substring(CSMRHAPPITEMIZEDBILL,1,1) AS Doc_Req_Detail_Itemized_Bill_Approved ,
	substring(CSMRHREQIMPLANTINV,1,1) AS Doc_Req_Detail_Implant_Inv_Ind ,
	substring(CSMRHAPPIMPLANTINV,1,1) AS Doc_Req_Detail_Implant_Inv_Approved ,
	substring(CSMRHREQOTHER,1,30) AS Doc_Req_Detail_Other_Desc , 
	substring(CSMRAPPQOTHER,1,1) AS Doc_Req_Detail_Other_Approved ,
	substring(CSMRHCOVLTR,1,1) AS Doc_Req_Detail_Cover_Letter_Required ,
	--Docs Requested Mail To Section
	substring(CSMRHPMAILNAME,1,40) AS Doc_Req_Detail_Payer_Name_MailTo ,
	substring(CSMRHVENDOR,1,3) AS Doc_Req_Detail_Vendor ,
	substring(CSMRHVMAILNAME,1,40) AS Doc_Req_Detail_Vendor_Mail_Name ,
	substring(CSMRHMAILADDR1,1,60) AS Doc_Req_Detail_Address1_MailTo , 
	substring(CSMRHMAILADDR2,1,40) AS Doc_Req_Detail_Address2_MailTo ,
	substring(CSMRHMAILCITY,1,35) AS Doc_Req_Detail_City_MailTo ,
	substring(CSMRHMAILST,1,2) AS Doc_Req_Detail_State_MailTo ,
	substring(CSMRHMAILZIP,1,20) AS Doc_Req_Detail_Zip_MailTo ,
	substring(CSMRHDELIVERYMETHOD,1,10) AS Doc_Req_Detail_Delivery_Method ,
	Coalesce(CSMRHPYRFAX,0) AS Doc_Req_Detail_Payer_Fax ,
	substring(CSMRHPYREMAIL,1,40) AS Doc_Req_Detail_Payer_Email ,
	--Docs Requested Sent Section
	CSMRHSENTDTE AS Doc_Req_Detail_Sent_Date ,
	substring(CSMRHMAILTRACKNBR,1,60) AS Doc_Req_Detail_Tracking_Number ,
	--Docs Requested Received Section
	CSMRHRECVDDTE AS Doc_Req_Detail_Received_Date ,
	substring(CSMRHRECPTCONFIRMNO,1,30) AS Doc_Req_Detail_Receipt_Conf_Number ,
	substring(CSMRHMAILSIGNBY,1,50) AS Doc_Req_Detail_Signed_By,
    CSMRHAPPROVEDTE AS Doc_Req_Detail_Approve_Date ,
	CSMRHCLAIMSUBDTE AS Doc_Req_Detail_Claim_Submit_Date ,
	CSMRHDENYDTE AS Doc_Req_Detail_Deny_Date,
	CSMRHDTE AS Doc_Req_Detail_RH_Date,
	CSMRHDUEDTE AS Doc_Req_Detail_RH_Due_Date,
	CSMRHIBREQDTE AS Doc_Req_Detail_IB_Request_Date,
	CSMRHIBSENTDTE AS Doc_Req_Detail_IB_Sent_Date,
	CSMRHLTR1SENTDTE AS Doc_Req_Detail_Letter_1_Sent_Date,
	CSMRHLTR2SENTDTE AS Doc_Req_Detail_Letter_2_Sent_Date,
	CSMRHLTR3SENTDTE AS Doc_Req_Detail_Letter_3_Sent_Date,
	CSMRHMAILRCPTDTE AS Doc_Req_Detail_Mail_RCPT_Date,
	CSMRHMRREQDTE AS Doc_Req_Detail_MR_Request_Date,
	CSMRHMRSENTDTE AS Doc_Req_Detail_MR_Sent_Date,
	CSMRHPPMCSENTDTE AS Doc_Req_Detail_PPMC_Sent_Date,
	substring(CSMRHPPMCFLAG,1,1) AS Doc_Req_Detail_PPMC_Flag
	
FROM Sqluser.CSMEDICALRECHIST H
	LEFT OUTER JOIN SQLUser.HCENCOUNTER E ON E.HCENID = H.CSMRHENID
	LEFT OUTER JOIN SQLUser.HCFACILITY F ON F.HCFAID = E.HCENFACILITY
	LEFT OUTER JOIN SQLUser.HCPAS S ON S.HCPSID = F.HCFAPASID
		
WHERE CSMRHID IS NOT NULL 
	AND CSMRHPATACCT IS NOT NULL
AND (CASE
			WHEN CSMRHPATACCT = 0
			AND H.CSMRHENID IS NULL
			THEN 'N'
			ELSE 'Y'
			END) = 'Y'