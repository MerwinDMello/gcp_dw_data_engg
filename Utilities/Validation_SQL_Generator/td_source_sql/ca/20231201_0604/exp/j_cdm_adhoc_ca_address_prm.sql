		
SELECT '$JOBNAME' || ',' || CAST(COUNT(*) AS VARCHAR(30)) || ',' AS SOURCE_STRING FROM (
	SELECT * FROM (
	select DISTINCT 
		CASE WHEN Trim(WRK1.Address_Line_1_Text) ='' THEN NULL  ELSE CAST(Trim(WRK1.Address_Line_1_Text) AS VARCHAR(100)) 	 END AS Address_Line_1_Text
		,CASE WHEN Trim(WRK1.Address_Line_2_Text) ='' THEN NULL  ELSE  CAST(Trim(WRK1.Address_Line_2_Text) AS VARCHAR(100)) END AS Address_Line_2_Text
		,CASE WHEN Trim(WRK1.Address_Line_3_Text) ='' THEN NULL  ELSE  CAST(Trim(WRK1.Address_Line_3_Text)AS VARCHAR(100)) END AS Address_Line_3_Text
		,CASE WHEN Trim(WRK1.City_Name) ='' THEN NULL  ELSE  CAST(Trim(WRK1.City_Name) AS VARCHAR(50))		 END AS City_Name
		,CASE WHEN Trim(WRK1.State_Name) ='' THEN NULL  ELSE  CAST(Trim(WRK1.State_Name)  AS VARCHAR(24))END AS State_Name
		,CASE WHEN Trim(WRK1.Zip_Code) ='' THEN NULL  ELSE  CAST(Trim(WRK1.Zip_Code) AS VARCHAR(10))	 END AS Zip_Code
		,CASE WHEN Trim(WRK1.County_Name) ='' THEN NULL  ELSE  CAST(Trim(WRK1.County_Name)AS VARCHAR(50)) END AS County_Name
		,CASE WHEN Trim(WRK1.Country_Id) ='' THEN NULL  ELSE  CAST(Trim(WRK1.Country_Id)AS INTEGER)	 END AS Country_Id
	from (
		SELECT DISTINCT
			CAST(PatAddr AS VARCHAR(100)) 		AS Address_Line_1_Text
			, CAST(PatAddr2 AS VARCHAR(100))	AS Address_Line_2_Text
			, CAST(NULL AS VARCHAR(100))  		AS Address_Line_3_Text
			, CAST(PatCity	AS VARCHAR(50))		AS City_Name
			, CAST (PatState AS VARCHAR(24))		AS State_Name
			, CAST(PatZip AS VARCHAR(10))			AS Zip_Code
			, CAST(County	AS VARCHAR(50))		AS County_Name
			, CAST(Country AS INTEGER)		AS Country_Id
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION ALL
		SELECT DISTINCT
			CAST(NULL AS VARCHAR(100))  		AS Address_Line_1_Text
			, CAST(NULL AS VARCHAR(100))   		AS Address_Line_2_Text
			, CAST(NULL AS VARCHAR(100))  		AS Address_Line_3_Text
			, BirthCit		AS City_Name
			, BirthSta		AS State_Name
			, BirthZip		AS Zip_Code
			, CAST(NULL AS VARCHAR(50)) 		AS County_Name
			, BirthCou	AS Country_Id
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION ALL 
		SELECT DISTINCT
			Address						AS Address_Line_1_Text
			, Address2					AS Address_Line_2_Text
			, Address3					AS Address_Line_3_Text
			, City							AS City_Name
			, StateOrProvince	AS State_Name
			, PostalCode				AS Zip_Code
			, County						AS County_Name
			, (Select Lookup_Id from EDWCDM.Ref_CA_Global_Lookup RCGL where RCGL.STS_Code_Text =  CACS.Country and short_name = 'ISOCountry')		AS Country_Id
		FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
		UNION ALL
		SELECT DISTINCT
			HospAddress AS Address_Line_1_Text
			, HospAddress2 AS Address_Line_2_Text
			, CAST(NULL AS VARCHAR(100))  	AS Address_Line_3_Text
			, HospCity AS City_Name
			, HospState AS State_Name
			, HospZip	AS Zip_Code
			, CAST(NULL AS VARCHAR(50))   AS County_Name
			, (Select Lookup_Id from EDWCDM.Ref_CA_Global_Lookup RCGL where RCGL.STS_Code_Text =  CAHS.HospCountry and short_name = 'ISOCountry')		AS Country_Id
		FROM EDWCDM_STAGING.CardioAccess_Hospital_STG CAHS) WRK1
	WHERE (CASE WHEN Trim(WRK1.Address_Line_1_Text) ='' THEN NULL  ELSE Trim(WRK1.Address_Line_1_Text) END IS NOT NULL
		OR CASE WHEN Trim(WRK1.Address_Line_2_Text) ='' THEN NULL  ELSE Trim(WRK1.Address_Line_2_Text) END  IS NOT NULL
		OR CASE WHEN Trim(WRK1.Address_Line_3_Text) ='' THEN NULL  ELSE Trim(WRK1.Address_Line_3_Text) END IS NOT NULL
		OR CASE WHEN Trim(WRK1.City_Name) ='' THEN NULL  ELSE Trim(WRK1.City_Name) END IS NOT NULL
		OR CASE WHEN Trim(WRK1.State_Name) ='' THEN NULL  ELSE Trim(WRK1.State_Name) END IS NOT NULL
		OR CASE WHEN Trim(WRK1.Zip_Code) ='' THEN NULL  ELSE Trim(WRK1.Zip_Code) END  IS NOT NULL
		OR CASE WHEN Trim(WRK1.County_Name) ='' THEN NULL  ELSE Trim(WRK1.County_Name) END IS NOT NULL
		OR CASE WHEN Trim(WRK1.Country_Id) ='' THEN NULL  ELSE Trim(WRK1.Country_Id) END IS NOT NULL) )WRK1
	
	WHERE NOT EXISTS(
	SELECT 1 FROM EDWCDM.CA_Address CAA
	WHERE COALESCE(WRK1.Address_Line_1_Text,'NULL') = COALESCE(CAA.Address_Line_1_Text,'NULL')
			AND  COALESCE(WRK1.Address_Line_2_Text,'NULL') = COALESCE(CAA.Address_Line_2_Text,'NULL')
			AND  COALESCE(WRK1.Address_Line_3_Text,'NULL') = COALESCE(CAA.Address_Line_3_Text,'NULL')
			AND  COALESCE(WRK1.City_Name,'NULL') = COALESCE(CAA.City_Name ,'NULL')
			AND  COALESCE(WRK1.State_Name,'NULL') = COALESCE(CAA.State_Name,'NULL')
			AND  COALESCE(WRK1.Zip_Code,'NULL') = COALESCE(CAA.Zip_Code,'NULL')
			AND  COALESCE(WRK1.County_Name,'NULL') = COALESCE(CAA.County_Name,'NULL')
			AND  COALESCE(WRK1.Country_Id,'NULL') = COALESCE(CAA.Country_Id,'NULL'))
) a;