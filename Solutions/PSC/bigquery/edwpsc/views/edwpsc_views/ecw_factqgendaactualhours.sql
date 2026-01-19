CREATE OR REPLACE VIEW edwpsc_views.`ecw_factqgendaactualhours`
AS SELECT
  `ecw_factqgendaactualhours`.qgendaprovideractualhourskey,
  `ecw_factqgendaactualhours`.provideractualhoursk,
  `ecw_factqgendaactualhours`.coid,
  `ecw_factqgendaactualhours`.companycode,
  `ecw_factqgendaactualhours`.locationsk,
  `ecw_factqgendaactualhours`.providerscheduledtypedesc,
  `ecw_factqgendaactualhours`.providershifttypedesc,
  `ecw_factqgendaactualhours`.providerstaffcategorydesc,
  `ecw_factqgendaactualhours`.lastname,
  `ecw_factqgendaactualhours`.firstname,
  `ecw_factqgendaactualhours`.npi,
  `ecw_factqgendaactualhours`.providerstafftypedesc,
  `ecw_factqgendaactualhours`.provideractualstartdatetime,
  `ecw_factqgendaactualhours`.provideractualenddatetime,
  `ecw_factqgendaactualhours`.provideractualminuteqty,
  `ecw_factqgendaactualhours`.providereffectivestartdatetime,
  `ecw_factqgendaactualhours`.providereffectiveenddatetime,
  `ecw_factqgendaactualhours`.providereffectiveminuteqty,
  `ecw_factqgendaactualhours`.deletedflag,
  `ecw_factqgendaactualhours`.sourcesystemcode,
  `ecw_factqgendaactualhours`.dwlastupdatedatetime,
  `ecw_factqgendaactualhours`.insertedby,
  `ecw_factqgendaactualhours`.inserteddtm,
  `ecw_factqgendaactualhours`.modifiedby,
  `ecw_factqgendaactualhours`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factqgendaactualhours`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factqgendaactualhours`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;