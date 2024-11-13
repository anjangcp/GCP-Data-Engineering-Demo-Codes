CREATE TABLE census (
     trid INT64 NOT NULL,
     age INT64 NOT NULL, 
     workclass STRING(MAX), 
     education STRING(MAX),
     marital_status STRING(MAX),
     occupation STRING(MAX),
     relationship STRING(MAX),
     sex STRING(MAX),
     native_country STRING(MAX),
     income_bracket STRING(MAX)
) PRIMARY KEY (trid);
