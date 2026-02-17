--Hospital A
-- departments

create external table if not exists `project-b2d1202e-c674-4674-a8a.bronze_dataset.departments_ha`
OPTIONS(
  format = 'json',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-a/departments/*.json']
);


-- providers

CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.providers_ha`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-a/providers/*.json']
);


-- patients

CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.patients_ha`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-a/patients/*.json']
);


-- transactions

CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.transactions_ha`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-a/transactions/*.json']
);


-- encounters

CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.encounters_ha`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-a/encounters/*.json']
);


-------------------------------------------------------------------------------------------------
-- Hospital B
-- encounters


CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.encounters_hb`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-b/encounters/*.json']
);


-- patients


CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.patients_hb`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-b/patients/*.json']
);


-- transactions


CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.transactions_hb`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-b/transactions/*.json']
);


-- providers


CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.providers_hb`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-b/providers/*.json']
);


-- departments


CREATE EXTERNAL TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.bronze_dataset.departments_hb`
OPTIONS(
  format = 'JSON',
  uris = ['gs://health-care-rcm-bucket-26jan029/landing/hospital-b/departments/*.json']
);

