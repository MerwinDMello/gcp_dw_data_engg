CREATE OR REPLACE VIEW edwpsc_views.`3m_factprofeenotbillable`
AS SELECT
  `3m_factprofeenotbillable`.profeenotbillable3mkey,
  `3m_factprofeenotbillable`.cacaccountnumber,
  `3m_factprofeenotbillable`.visitnumber,
  `3m_factprofeenotbillable`.visittype,
  `3m_factprofeenotbillable`.facilityid,
  `3m_factprofeenotbillable`.location,
  `3m_factprofeenotbillable`.patientfirstname,
  `3m_factprofeenotbillable`.patientlastname,
  `3m_factprofeenotbillable`.patientmiddlename,
  `3m_factprofeenotbillable`.admitdate,
  `3m_factprofeenotbillable`.dischargedate,
  `3m_factprofeenotbillable`.servicedatekey,
  `3m_factprofeenotbillable`.servicedatetime,
  `3m_factprofeenotbillable`.cacworklistname,
  `3m_factprofeenotbillable`.lookupkey,
  `3m_factprofeenotbillable`.codingstatus,
  `3m_factprofeenotbillable`.codingstatusdatetime,
  `3m_factprofeenotbillable`.comment,
  `3m_factprofeenotbillable`.docnotbillablereason,
  `3m_factprofeenotbillable`.coderid,
  `3m_factprofeenotbillable`.careprovider,
  `3m_factprofeenotbillable`.careproviderfirstname,
  `3m_factprofeenotbillable`.careproviderlastname,
  `3m_factprofeenotbillable`.careprovidermiddlename,
  `3m_factprofeenotbillable`.filename,
  `3m_factprofeenotbillable`.filedate,
  `3m_factprofeenotbillable`.insertedby,
  `3m_factprofeenotbillable`.inserteddtm,
  `3m_factprofeenotbillable`.modifiedby,
  `3m_factprofeenotbillable`.modifieddtm,
  `3m_factprofeenotbillable`.coid,
  `3m_factprofeenotbillable`.practiceid,
  `3m_factprofeenotbillable`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`3m_factprofeenotbillable`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`3m_factprofeenotbillable`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;