create table if not exists `{{ params.param_hr_core_dataset_name }}.address`
(
  addr_sid INT64 NOT NULL OPTIONS(description="this field is etl generated unique sequence number for each business or individual address."),
  addr_type_code STRING NOT NULL OPTIONS(description="unique list of address type codes are maintained in this field. address type code field values are  bus and ind."),
  addr_line_1_text STRING NOT NULL OPTIONS(description="contains the first line of the supplemental address"),
  addr_line_2_text STRING OPTIONS(description="contains the second line of the supplemental address"),
  addr_line_3_text STRING OPTIONS(description="contains te third line of the supplemental address"),
  addr_line_4_text STRING OPTIONS(description="contains the fourth line of the supplemental address"),
  city_name STRING NOT NULL OPTIONS(description="the city name is the city associated with the address for  a person or organization."),
  zip_code STRING NOT NULL OPTIONS(description="this is used for the zip code when the zip code extension is included as one number or field."),
  county_name STRING NOT NULL OPTIONS(description="maintains county name of an business or individual person address"),
  country_code STRING OPTIONS(description="it maintains country code of an address in this field"),
  state_code STRING NOT NULL OPTIONS(description="the state code is the abbreviated 2 character code associated with the address state for a person or organization."),
  location_code STRING OPTIONS(description="lawson assigned process level location code of each address maintained in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY addr_sid, addr_type_code
OPTIONS(description="unique business facility and personal employees addresses are maintained in this table. it maintains individual person address, working location address & also process level address.");

