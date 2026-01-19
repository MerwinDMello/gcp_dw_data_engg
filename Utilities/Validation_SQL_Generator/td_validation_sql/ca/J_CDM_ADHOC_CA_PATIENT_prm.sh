export JOBNAME='J_CDM_ADHOC_CA_PATIENT'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT  * FROM (
select 
DISTINCT
TRIM(EDWCDM.CA_Server.Server_SK) as Server_SK
,TRIM(PatID) as PatID
,TRIM(HospitalID) as HospitalID
,TRIM(HospName) as HospName
,TRIM(PatNationalID) as PatNationalID
,TRIM(STSVendorID) as STSVendorID
,TRIM(MedRecN) as MedRecN
,TRIM(PatLName) as PatLName
,TRIM(PatFName) as PatFName
,TRIM(PatMInit) as PatMInit
,DOB as DOB
,TimeOfBirth as TimeOfBirth
,TRIM(SSN) as SSN
,TRIM(Gender) as Gender
,TRIM(PatAddr) as PatAddr
,TRIM(PatAddr2) as PatAddr2
,TRIM(PatCity) as PatCity
,TRIM(PatState) as PatState
,TRIM(PatZip) as PatZip
,TRIM(PatPhone) as PatPhone
,TRIM(PatFax) as PatFax
,TRIM(PatWPhone) as PatWPhone
,TRIM(PatCPhone) as PatCPhone
,TRIM(PatEmail) as PatEmail
,TRIM(County) as County
,TRIM(Country) as Country
,TRIM(PatForeign) as PatForeign
,TRIM(Pager) as Pager
,TRIM(DemogDataVrsn) as DemogDataVrsn
,TRIM(DataVrsn) as Data_Version_Code
,TRIM(Ethnicity) as Ethnicity_Id
,TRIM(Race) as Race_Id
,TRIM(RaceCaucasian) as RaceCaucasian
,TRIM(RaceBlack) as RaceBlack
,TRIM(RaceHispanic) as RaceHispanic
,TRIM(RaceAsian) as RaceAsian
,TRIM(RaceNativeAm) as RaceNativeAm
,TRIM(RaceNativePacific) as RaceNativePacific
,TRIM(RaceOther) as RaceOther
,TRIM(RaceOthSpcfy) as RaceOthSpcfy
,TRIM(BirthCit) as BirthCit
,TRIM(BirthSta) as BirthSta
,TRIM(BirthCou) as BirthCou
,TRIM(BirthWtKg) as Birth_Weight_Kg_Amt
,TRIM(Allergies) as Allergy_Text
,TRIM(Premature) as Premature_Birth_Id
,TRIM(GestAgeWeeks) as Gestational_Age_Week_Num
,TRIM(OrganDonor) as Organ_Donor_Id
,CreateDate as Source_Create_Date_Time
,LastUpdate as Source_Last_Update_Date_Time
,TRIM(UpdatedBy) as Updated_By_3_4_Id
,TRIM(ZipCodeNA) as ZipCodeNA
,TRIM(PatMName) as Patient_Middle_Name
,TRIM(SSNNA) as Social_Security_Num
,TRIM(Aux1) as Aux1
,TRIM(Aux2) as Aux2
,TRIM(ACCExclude) as ACCExclude
,TRIM(PatGUID) as PatGUID
,TRIM(EthnicityRecorded) as EthnicityRecorded 
,TRIM(PC4VendorCode) as PC4VendorCode
,TRIM(PC4DataVrsn) as PC4DataVrsn
,TRIM(BirthZip) as BirthZip
,TRIM(MatNameKnown) as MatNameKnown
,TRIM(STSCongDVConv) as STSCongDVConv
,TRIM(RaceDocumented) as RaceDocumented
,TRIM(BirthLocKnown) as Birth_Location_Available_Id
,TRIM(RaceAsianIndian) as RaceAsianIndian
,TRIM(RaceChinese) as RaceChinese
,TRIM(RaceFilipino) as RaceFilipino
,TRIM(RaceJapanese) as RaceJapanese
,TRIM(RaceKorean) as RaceKorean
,TRIM(RaceVietnamese) as RaceVietnamese
,TRIM(RaceAsianOther) as RaceAsianOther
,TRIM(RaceNativeHawaii) as RaceNativeHawaii
,TRIM(RaceGuamChamorro) as RaceGuamChamorro
,TRIM(RaceSamoan) as RaceSamoan
,TRIM(RacePacificIslandOther) as RacePacificIslandOther
,TRIM(HispEthnicityMexican) as HispEthnicityMexican
,TRIM(HispEthnicityPuertoRico) as HispEthnicityPuertoRico
,TRIM(HispEthnicityCuban) as HispEthnicityCuban
,TRIM(HispEthnicityOtherOrigin) as HispEthnicityOtherOrigin
,TRIM(STG.Server_Name) AS Server_name
,TRIM(Full_Server_NM) as Full_Server_NM
,'C' as Source_System_Code
,Current_Timestamp(0) as  DW_Last_Update_Date_Time 
 from  EDWCDM_STAGING.CardioAccess_Demographics_STG Stg
 Inner Join EDWCDM.CA_Server
 on Stg.Full_Server_Nm = CA_Server.Server_Name)a)b;"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_PATIENT)a;"

