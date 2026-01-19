CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factqgendaproviderhours`
AS SELECT
  `ecw_factqgendaproviderhours`.qgendaproviderhourskey,
  `ecw_factqgendaproviderhours`.providerschedulesk,
  `ecw_factqgendaproviderhours`.scheduledcoid,
  `ecw_factqgendaproviderhours`.companycode,
  `ecw_factqgendaproviderhours`.providershifttypedesc,
  `ecw_factqgendaproviderhours`.providerstaffcategorydesc,
  `ecw_factqgendaproviderhours`.providerlastname,
  `ecw_factqgendaproviderhours`.providerfirstname,
  `ecw_factqgendaproviderhours`.providerpersondwid,
  `ecw_factqgendaproviderhours`.providernpi,
  `ecw_factqgendaproviderhours`.providerstafftypedesc,
  `ecw_factqgendaproviderhours`.scheduledstartdatetime,
  `ecw_factqgendaproviderhours`.scheduledenddatetime,
  `ecw_factqgendaproviderhours`.providercreditedhourqty,
  `ecw_factqgendaproviderhours`.deleteflag,
  `ecw_factqgendaproviderhours`.providerscheduledtypedesc,
  `ecw_factqgendaproviderhours`.sourcelastupdateddatetime,
  `ecw_factqgendaproviderhours`.sourcesystemcode,
  `ecw_factqgendaproviderhours`.dwlastupdatedatetime,
  `ecw_factqgendaproviderhours`.insertedby,
  `ecw_factqgendaproviderhours`.inserteddtm,
  `ecw_factqgendaproviderhours`.modifiedby,
  `ecw_factqgendaproviderhours`.modifieddtm
  FROM
    edwpsc.`ecw_factqgendaproviderhours`
;