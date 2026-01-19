CREATE OR REPLACE VIEW edwpsc_base_views.`pk_factencounterprovider`
AS SELECT
  `pk_factencounterprovider`.pkencounterproviderkey,
  `pk_factencounterprovider`.pkregionname,
  `pk_factencounterprovider`.pkencounterkey,
  `pk_factencounterprovider`.encounterid,
  `pk_factencounterprovider`.begineffectivedate,
  `pk_factencounterprovider`.endeffectivedate,
  `pk_factencounterprovider`.providertype,
  `pk_factencounterprovider`.providerlastname,
  `pk_factencounterprovider`.providerfirstname,
  `pk_factencounterprovider`.providermiddlename,
  `pk_factencounterprovider`.providernpi,
  `pk_factencounterprovider`.deleteflag,
  `pk_factencounterprovider`.dwlastupdatedatetime,
  `pk_factencounterprovider`.sourceaprimarykeyvalue,
  `pk_factencounterprovider`.sourcesystemcode,
  `pk_factencounterprovider`.insertedby,
  `pk_factencounterprovider`.inserteddtm,
  `pk_factencounterprovider`.modifiedby,
  `pk_factencounterprovider`.modifieddtm,
  `pk_factencounterprovider`.pkfinancialnumber,
  `pk_factencounterprovider`.personid
  FROM
    edwpsc.`pk_factencounterprovider`
;