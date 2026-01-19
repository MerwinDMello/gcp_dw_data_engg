CREATE OR REPLACE VIEW edwpsc_views.`3m_factprofeeinventory`
AS SELECT
  `3m_factprofeeinventory`.profeeinventory3mkey,
  `3m_factprofeeinventory`.cacaccountnumber,
  `3m_factprofeeinventory`.visitnumber,
  `3m_factprofeeinventory`.visittype,
  `3m_factprofeeinventory`.facilityid,
  `3m_factprofeeinventory`.location,
  `3m_factprofeeinventory`.patientfirstname,
  `3m_factprofeeinventory`.patientlastname,
  `3m_factprofeeinventory`.patientmiddlename,
  `3m_factprofeeinventory`.admitdate,
  `3m_factprofeeinventory`.dischargedate,
  `3m_factprofeeinventory`.servicedatekey,
  `3m_factprofeeinventory`.servicedatetime,
  `3m_factprofeeinventory`.cacworklistname,
  `3m_factprofeeinventory`.lookupkey,
  `3m_factprofeeinventory`.codingstatus,
  `3m_factprofeeinventory`.codingstatusdatetime,
  `3m_factprofeeinventory`.comment,
  `3m_factprofeeinventory`.docholdreason,
  `3m_factprofeeinventory`.holddate,
  `3m_factprofeeinventory`.holdby,
  `3m_factprofeeinventory`.careprovider,
  `3m_factprofeeinventory`.careproviderfirstname,
  `3m_factprofeeinventory`.careproviderlastname,
  `3m_factprofeeinventory`.careprovidermiddlename,
  `3m_factprofeeinventory`.filename,
  `3m_factprofeeinventory`.filedate,
  `3m_factprofeeinventory`.insertedby,
  `3m_factprofeeinventory`.inserteddtm,
  `3m_factprofeeinventory`.modifiedby,
  `3m_factprofeeinventory`.modifieddtm,
  `3m_factprofeeinventory`.coid,
  `3m_factprofeeinventory`.practiceid,
  `3m_factprofeeinventory`.openinventoryflag,
  `3m_factprofeeinventory`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`3m_factprofeeinventory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`3m_factprofeeinventory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;