CREATE TABLE MESSAGEPIPELINE (
    exchangeid varchar(128),
    serviceName varchar(128),
    operationName varchar(128),
    content varchar(8192),
    status INTEGER, 
    primary key (exchangeid)
);
