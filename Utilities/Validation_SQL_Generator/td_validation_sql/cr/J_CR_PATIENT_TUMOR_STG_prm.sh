export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_TUMOR_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_TUMOR_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
from
(
Select
Tum.TumorId,
Tum.PatientId,
Tum.Grade,
Exta.CaseStatFlag,
Tum.PrimarySite,
Tum.Primsitesubcode,
Tum.MstDefChemoSumm,
Tum.MstDefDxStageSumm,
Tum.MstDefHormSumm,
Tum.MstDefImmunoSumm,
Tum.MstDefMarginsSumm,
Tum.MstDefPallSumm,
Tum.MstDefRTBoost,
Tum.MstDefRTHosp,
Tum.MstDefRTLoc,
Extb.ReasonNoChemo,
Extb.ReasonNoHormone,
Extb.ReasonNoImmuno,
Extb.ReasonNoRad,
Tum.MstDefSurgPrimSumm,
TUM.CS_SSFactor1,
TUM.CS_SSFactor2,
TUM.CS_SSFactor15,
TUM.CS_SSFactor16,
TUM.CS_SSFactor22,
TUM.CS_SSFactor23,
TUM.CS_SSFactor25,
Extb.ChemoSrgSeq,
Tum.MstDefRTMod,
Tum.MstDefRTVol,
Tum.MstDefScopeLNSumm,
Ext2.SurgAppHosp,
Tum.MstDefSurgPrimHosp,
Tum.CS_TumSize,
cast(Tum.MstDefChemoDt as varchar(20)) MstDefChemoDt,
cast(Tum.MstDefRTDt as varchar(20)) MstDefRTDt,
cast(Tum.MstDefImmunoDt as varchar(20)) MstDefImmunoDt,
cast(Tum.MstDefHormDt as varchar(20)) MstDefHormDt,
cast(Tum.MstDefRTStopDt as varchar(20)) MstDefRTStopDt,
Extb.Elapsed1stSurgChem,
Extb.Elapsed1stSurgLstCont,
Extb.ElapsedChemo1stSurg,
Extb.LengthToDtChemo,
Extb.LengthToDtHorm,
Extb.LengthToDtImmuno,
Extb.LengthToDtMstDefSurg,
Extb.LengthToDtRadStarted,
Extb.LengthToDtTrnsplnt,
Extb.LengthToFirstTx,
cast(Tum.DtFirstSurg as varchar(20)) DtFirstSurg,
Extb.ElapsedRTStartEnd,
Exta.TumorSizeSumm,
cast(Tum.DtMstDefSurg as varchar(20)) DtMstDefSurg,
cast(Exta.DateAdmis as varchar(20)) DateAdmis,
Tum.Survival,
Tum.BestCSSummStage,
Tum.BestCSTNMStage,
Tum14.T14UDF116,
Tum14.T14UDF117,
Tum16.T16UDF118,
Exta.Abstractor,
Exta.ClassCase,
Exta.SeqPrim,
Extb.Manage_MD,
Extb.MedOncMD,
Extb.RadOncMD,
Extb.PrimSurgeon,
tum.Histology
from
mRegistry.dbo.Tumor Tum
Left Outer Join mRegistry.dbo.TumorExt1b Extb
on Tum.Tumorid=Extb.Tumorid 
Left Outer Join mRegistry.dbo.TumorExt1a Exta
on Tum.Tumorid=Exta.Tumorid 
Left Outer Join mRegistry.dbo.TumorExt2 Ext2
on Tum.Tumorid=Ext2.Tumorid
Left Outer Join mRegistry.dbo.TumorExt14 Tum14
on Tum.Tumorid=Tum14.Tumorid
Left Outer Join mRegistry.dbo.TumorExt16 Tum16
on Tum.Tumorid=Tum16.Tumorid
) SRC"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_TUMOR_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CR_Patient_Tumor_STG"
