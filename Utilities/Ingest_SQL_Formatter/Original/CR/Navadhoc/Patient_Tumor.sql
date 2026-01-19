Select 
PatientTumorFactID as cn_patient_tumor_sid,
PatientDimID as nav_patient_id,
TumorTypeDimID as tumor_type_id,
NavigatorDimID as navigator_id,
Coid,
REPLACE(REPLACE(LTRIM(RTRIM(EFolderID)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as electronic_folder_id_text,
REPLACE(REPLACE(LTRIM(RTRIM(TumorReferralSource)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as tumorreferralsource,
REPLACE(REPLACE(LTRIM(RTRIM(NavigationStatus)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as navigationstatus,
REPLACE(REPLACE(LTRIM(RTRIM(TumorEntryPoint)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as identification_period_text,
TumorReferralDate as referral_date,
ReferringMDDimID as referring_physician_id,
REPLACE(REPLACE(LTRIM(RTRIM(EndNavigationReason)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as nav_end_reason_text,
REPLACE(REPLACE(LTRIM(RTRIM(EndTreatmentReason)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as treatment_end_reason_text,
REPLACE(REPLACE(LTRIM(RTRIM(EndTreatmentPhysician)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as endtreatmentphysician,
REPLACE(REPLACE(LTRIM(RTRIM(EndTreatmentLocation)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as endtreatmentlocation,
concat('0x',CONVERT(varchar(50),HBSource,2)) as hashbite_ssk,
'v_currtimestamp' as dw_last_update_date_time 
from 
navadhoc.dbo.PatientTumor (NOLOCK)