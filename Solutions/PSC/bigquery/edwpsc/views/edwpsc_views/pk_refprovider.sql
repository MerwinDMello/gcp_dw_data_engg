CREATE OR REPLACE VIEW edwpsc_views.`pk_refprovider`
AS SELECT
  `pk_refprovider`.pkproviderkey,
  `pk_refprovider`.providerusername,
  `pk_refprovider`.providerdomusername,
  `pk_refprovider`.providerfirstname,
  `pk_refprovider`.providerlastname,
  `pk_refprovider`.providermiddlename,
  `pk_refprovider`.providerfullname,
  `pk_refprovider`.providerdeanumber,
  `pk_refprovider`.providernpi,
  `pk_refprovider`.authindicatorflag,
  `pk_refprovider`.deleteflag,
  `pk_refprovider`.createddatetime,
  `pk_refprovider`.updateddatetime,
  `pk_refprovider`.pkregionname,
  `pk_refprovider`.sourceaprimarykeyvalue,
  `pk_refprovider`.insertedby,
  `pk_refprovider`.inserteddtm,
  `pk_refprovider`.modifiedby,
  `pk_refprovider`.modifieddtm,
  `pk_refprovider`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pk_refprovider`
;