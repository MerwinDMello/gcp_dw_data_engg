SELECT
	ROW_NUMBER() OVER ( PARTITION BY 1
ORDER BY
	t1.ProviderId,
	t1.CarrierName,
	t1.ExpiredDate DESC 
        ) AS MD_Staff_Provider_Ins_Id,
	t1.ProviderID AS MD_Staff_Provider_Id,
	t2.FirstName AS MD_Staff_First_Name,
	t2.MiddleName AS MD_Staff_Middle_Name,
	t2.[Name] AS MD_Staff_Full_Name,
	t2.Suffix AS MD_Staff_Suffix_Name,
	t1.ExpiredDate AS MD_Staff_Expired_Date,
	t2.FormalName AS MD_Staff_Formal_Name_Txt,
	CAST((CASE
		WHEN LTRIM(RTRIM(ISNULL(t2.FormalName, ''))) <> ''
		AND LTRIM(RTRIM(ISNULL(t2.Degree, ''))) <> ''
          THEN CONCAT(LTRIM(RTRIM(ISNULL(t2.FormalName, ''))), ', ', LTRIM(RTRIM(ISNULL(t2.Degree, ''))) )
		ELSE FormalName
	END) AS VARCHAR(255)) AS MD_Staff_Professional_Name,
	t1.IssuedDate AS MD_Staff_Issued_Date,
	t2.NameWithDegree AS MD_Staff_Name_With_Degree_Txt,
	t1.PolicyNumber AS MD_Staff_Policy_Number,
	t1.RetroDate AS MD_Staff_Retroactive_Date,
	t2.DownloadDate AS MD_Staff_Download_Date,
	t1.CarrierName AS MD_Staff_Ins_Carr_Name,
	t1.CarrierAddress AS MD_Staff_Ins_Carr_Addr_Txt_1,
	t1.CarrierAddress2 AS MD_Staff_Ins_Carr_Addr_Txt_2,
	t1.CarrierCity AS MD_Staff_Ins_Carr_City_Name,
	t1.CarrierState AS MD_Staff_Ins_Carr_State_Code,
	t1.CarrierZip AS MD_Staff_Ins_Carr_Zip_Code,
	t1.CarrierTelephone AS MD_Staff_Ins_Carr_Phone_Num,
	t1.Terms AS MD_Staff_Ins_Terms_Desc,
	t1.[Coverage] AS MD_Staff_Ins_Cov_Desc,
	t2.Department AS MD_Staff_Dept_Desc,
	'S' AS Source_System_Code,
	'v_currtimestamp' AS dw_last_update_date_time
FROM
	(
	SELECT
		DISTINCT
    CAST([ProviderID] AS VARCHAR(255)) AS [ProviderID],
		CAST([ExpiredDate] AS DATE) AS ExpiredDate,
		CAST([IssuedDate] AS DATE) AS IssuedDate,
		CAST([PolicyNumber] AS VARCHAR(255)) AS PolicyNumber,
		CAST([RetroDate] AS DATE) AS RetroDate,
		CAST([CarrierName] AS VARCHAR(255)) AS [CarrierName],
		CAST([CarrierAddress] AS VARCHAR(255)) AS [CarrierAddress] ,
		CAST([CarrierAddress2] AS VARCHAR(255)) AS [CarrierAddress2],
		CAST(CarrierCity AS VARCHAR(255)) AS CarrierCity,
		CAST(CarrierState AS VARCHAR(255)) AS CarrierState,
		CAST(CarrierZip AS VARCHAR(255)) AS CarrierZip,
		CAST(CarrierTelephone AS VARCHAR(255)) AS CarrierTelephone,
		LTRIM(RTRIM(CAST([Terms] AS VARCHAR(255)))) AS Terms,
		CAST([Coverage] AS VARCHAR(255)) AS Coverage
	FROM
		[MD-Staff_Import].MDStaff.InsuranceData WITH (NOLOCK)
	WHERE
		ProviderId IS NOT NULL
		AND CarrierName IS NOT NULL
) t1
INNER JOIN 
(
	SELECT
		CAST(ProviderID AS VARCHAR(50)) AS ProviderId,
		CAST(FirstName AS VARCHAR(255)) AS FirstName,
		CAST(MiddleName AS VARCHAR(255)) AS MiddleName,
		CAST(LastName AS VARCHAR(255)) AS LastName,
		CAST(Suffix AS VARCHAR(255)) AS Suffix,
		CAST(DEGREE AS VARCHAR(255)) AS DEGREE,
		CAST([Name] AS VARCHAR(255)) AS [Name],
		CAST(NameWithDegree AS VARCHAR(255)) AS NameWithDegree,
		CAST((CASE
			WHEN ISNULL(FirstName,
			'') <> ''
				AND ISNULL(LastName,
				'') <> '' 
       THEN CONCAT(ISNULL(FirstName, '') 
         , (CASE WHEN LEN(REPLACE(LTRIM(RTRIM(ISNULL(middlename, ''))), '.', '')) > 0
           THEN 
           (CASE WHEN LEN(LTRIM(RTRIM(ISNULL(middlename, '')))) = 2 AND LTRIM(RTRIM(ISNULL(middlename, ''))) LIKE '%[A-Z]%.' THEN CONCAT(' ', LTRIM(RTRIM(ISNULL(middlename, ''))))
              ELSE CONCAT(' ', SUBSTRING(LTRIM(RTRIM(ISNULL(middlename, ''))), 1, 1), '.')
            END)
          ELSE ''
         END)   
         , ' ', ISNULL(LastName, '') 
         , (CASE WHEN LTRIM(RTRIM(ISNULL(suffix, ''))) <> '' 
           THEN CONCAT(' ', LTRIM(RTRIM(ISNULL(suffix, ''))))
           ELSE ''
         END)
         )
				ELSE ''
			END) AS VARCHAR(255)) AS FormalName,
		CAST(Department1 AS VARCHAR(255)) AS Department,
		CAST(DownloadDate AS datetime) AS DownloadDate,
		ROW_NUMBER() OVER ( PARTITION BY CAST(ProviderID AS VARCHAR(50))
	ORDER BY
		ISNULL(Archived,
		'') ASC,
		(CASE
			WHEN LTRIM(RTRIM(ISNULL([PRIMARY Facility], ''))) <> '' THEN 1
			ELSE 2
		END) ASC
            ) AS RowNum
	FROM
		[MDStaff].dbo.vw_ProviderData WITH (NOLOCK)
) t2
ON
	t1.ProviderId = t2.ProviderId
	AND t2.rownum = 1