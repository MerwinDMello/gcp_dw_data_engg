export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_PHONE_NUM_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_PHONE_NUM_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
select
coalesce(c.ContactId,-99) as Patient_Contact_Id, p.PatientId as Metriq_Patient_Id, 'H' as Phone_Num_Type_Code, p.phone as Phone_Num from mregistry.dbo.Patient p left outer join ( select c.ContactId, PatientId, Relation, CellPhone, HomePhone

   from mregistry.dbo.contacts c
inner join
(select contactid,count(*) cnt from mregistry.dbo.contacts group by contactid having count(*)=1) a on c.contactid=a.contactid union all select ContactId, PatientId, Relation, CellPhone, HomePhone

 from
(select *,row_number() over(partition by contactid order by contactshospid desc) rn from mregistry.dbo.contacts where contactid in (
select  c.contactid   from mregistry.dbo.contacts c
inner join
(select contactid,count(*) cnt from mregistry.dbo.contacts group by contactid having count(*)>1) a on c.contactid=a.contactid inner join mregistry.dbo.contacts c1 on c.contactid=c1.contactid and c1.contactshospid is null
)
) b
where b.rn=1
)  c
on p.PatientId=c.PatientId
and c.Relation='00'
Union
select
ContactId as Patient_Contact_Id,
coalesce(c.PatientId,-99) as Metriq_Patient_Id, 'C' as Phone_Num_Type_Code, c.cellphone as Phone_Num from
 
( select
c.ContactId,
PatientId,
Relation,
CellPhone,
HomePhone
   from mregistry.dbo.contacts c
inner join
(select contactid,count(*) cnt from mregistry.dbo.contacts group by contactid having count(*)=1) a on c.contactid=a.contactid union all select ContactId, PatientId, Relation, CellPhone, HomePhone  from (select *,row_number() over(partition by contactid order by contactshospid desc) rn from mregistry.dbo.contacts where contactid in (
select  c.contactid   from mregistry.dbo.contacts c
inner join
(select contactid,count(*) cnt from mregistry.dbo.contacts group by contactid having count(*)>1) a on c.contactid=a.contactid inner join mregistry.dbo.contacts c1 on c.contactid=c1.contactid and c1.contactshospid is null
)
) b
where b.rn=1
)  c where c.Relation<>'00' and c.cellphone is not null and c.cellphone<>''
Union
select
ContactId as Patient_Contact_Id,
coalesce(c.PatientId,-99) as Metriq_Patient_Id, 'H' as Phone_Num_Type_Code, c.homephone as Phone_Num from 

( select
c.ContactId,
PatientId,
Relation,
CellPhone,
HomePhone

   from mregistry.dbo.contacts c
inner join
(select contactid,count(*) cnt from mregistry.dbo.contacts group by contactid having count(*)=1) a on c.contactid=a.contactid union all select ContactId, PatientId, Relation, CellPhone, HomePhone  from (select *,row_number() over(partition by contactid order by contactshospid desc) rn from mregistry.dbo.contacts where contactid in (
select  c.contactid   from mregistry.dbo.contacts c
inner join
(select contactid,count(*) cnt from mregistry.dbo.contacts group by contactid having count(*)>1) a on c.contactid=a.contactid inner join mregistry.dbo.contacts c1 on c.contactid=c1.contactid and c1.contactshospid is null
)
) b
where b.rn=1
)  c


where c.Relation<>'00' and c.homephone is not null and c.homephone<>''

)a"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_PHONE_NUM_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_Staging.CR_Patient_Phone_Num_Stg"
