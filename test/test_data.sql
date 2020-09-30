
IF NOT EXISTS(SELECT name
          FROM master.dbo.sysdatabases
          WHERE
              name = N'w3')
    BEGIN
        CREATE DATABASE [w3];
        ALTER DATABASE [w3] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS);
        ALTER DATABASE [w3] COLLATE Latin1_General_BIN;

    END
GO


USE [w3]

IF NOT exists(SELECT *
              FROM w3.sys.schemas
              WHERE
                  name = 'fact')
    BEGIN
        EXEC sp_executesql N'CREATE SCHEMA fact'
    END;
IF NOT exists(SELECT *
              FROM w3.sys.schemas
              WHERE
                  name = 'dev')
    BEGIN
        EXEC sp_executesql N'CREATE SCHEMA dev'
    END;


GO
;

IF OBJECT_ID('w3.fact.Orders', 'U') IS NOT NULL
    DROP TABLE w3.fact.Orders;
IF OBJECT_ID('w3.dbo.Customers', 'U') IS NOT NULL
    DROP TABLE w3.dbo.Customers;
IF OBJECT_ID('w3.dbo.Agents', 'U') IS NOT NULL
    DROP TABLE w3.dbo.Agents;
IF OBJECT_ID('w3.dbo.Types', 'U') IS NOT NULL
    DROP TABLE w3.dbo.Types;

IF OBJECT_ID('w3.dbo.PrePost', 'U') IS NOT NULL
    DROP TABLE w3.dbo.PrePost;

IF OBJECT_ID('w3.dbo.CompositeKey', 'U') IS NOT NULL
    DROP TABLE w3.dbo.CompositeKey;

IF OBJECT_ID('w3.dbo.RealTime', 'U') IS NOT NULL
    DROP TABLE w3.dbo.RealTime;
IF OBJECT_ID('w3.dbo.RealTimeAux', 'U') IS NOT NULL
    DROP TABLE w3.dbo.RealTimeAux;

CREATE TABLE w3.dbo.CompositeKey
(
    id1   INT NOT NULL,
    id2   INT NOT NULL,
    value VARCHAR(10),
    PRIMARY KEY (id1, id2),
)

ALTER TABLE w3.dbo.CompositeKey
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)


CREATE TABLE w3.dbo.RealTime
(
    id          INT         NOT NULL IDENTITY PRIMARY KEY,
    ownValue    VARCHAR(10) UNIQUE,
    mergeValue  VARCHAR(10) NULL,
    spreadValue VARCHAR(10) NULL,
)

ALTER TABLE w3.dbo.RealTime
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)

GO

CREATE OR ALTER VIEW dbo.[RealTimeDuplicateView] (recordID, ownValue, mergeValue, spreadValue)
AS
SELECT id + '', ownValue, mergeValue, spreadValue
FROM w3.dbo.RealTime

GO

CREATE TABLE w3.dbo.RealTimeAux
(
    id         INT NOT NULL IDENTITY PRIMARY KEY,
    realTimeID INT NOT NULL,
    data       VARCHAR(10),
)

GO



ALTER TABLE w3.dbo.RealTimeAux
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)

GO

CREATE OR ALTER VIEW dbo.[RealTimeDerivedView] (id, ownValue, data)
AS
SELECT RT.id, RT.ownValue, RTA.data
FROM w3.dbo.RealTime RT
         JOIN w3.dbo.RealTimeAux RTA
              ON RTA.realTimeID = RT.id


GO

CREATE OR ALTER VIEW dbo.[RealTimeMergeView] (mergeValue, count)
AS
SELECT mergeValue, count(1)
FROM w3.dbo.RealTime
WHERE
    mergeValue IS NOT NULL
GROUP BY mergeValue
GO


CREATE OR ALTER VIEW dbo.[RealTimeSpreadView] (row, id, ownValue, spreadValue)
AS
SELECT ROW_NUMBER() OVER (ORDER BY A.id ASC), A.id, A.ownValue, A.spreadValue
FROM w3.dbo.RealTime A
         JOIN w3.dbo.RealTime B ON A.spreadValue = B.spreadValue
WHERE
    A.spreadValue IS NOT NULL
GO
;

INSERT INTO w3.dbo.RealTime
VALUES ('a1', 'a', NULL),
       ('a2', 'a', NULL),
       ('b1', 'b', NULL),
       ('b2', 'b', NULL),
       ('c1', NULL, 'c'),
       ('c2', NULL, 'c')
GO

INSERT INTO w3.dbo.RealTime
VALUES ('test', 'a', NULL)


INSERT INTO w3.dbo.RealTimeAux
VALUES (1, 'a1-data'),
       (2, 'a2-data'),
       (3, 'b1-data'),
       (4, 'b2-data')
GO

IF OBJECT_ID('w3.dev.Assignments', 'U') IS NOT NULL
    DROP TABLE w3.dev.Assignments;
IF OBJECT_ID('w3.dev.Developers', 'U') IS NOT NULL
    DROP TABLE w3.dev.Developers;
IF OBJECT_ID('w3.dev.Tasks', 'U') IS NOT NULL
    DROP TABLE w3.dev.Tasks;
IF OBJECT_ID('w3.dev.Sprints', 'U') IS NOT NULL
    DROP TABLE w3.dev.Sprints;

CREATE TABLE w3.dev.Developers
(
    id   INT NOT NULL PRIMARY KEY,
    name VARCHAR(20) UNIQUE,

)
ALTER TABLE w3.dev.Developers
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)

INSERT INTO w3.dev.Developers (id, name)
VALUES (1, 'chris'),
       (2, 'derek'),
       (3, 'steve'),
       (4, 'wyatt')

CREATE TABLE w3.dev.Tasks
(
    id   INT NOT NULL PRIMARY KEY,
    name VARCHAR(100) UNIQUE,
    size INT
)
ALTER TABLE w3.dev.Tasks
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)

INSERT INTO w3.dev.Tasks (id, name, size)
VALUES (1, 'DQ Check Execution', 5),
       (2, 'DQ Check Authoring', 8),
       (3, 'Schedule/Execute Writebacks', 5),
       (4, 'Define mappings from target source to shape', 3),
       (5, 'Register target sources to shapes', 3),
       (6, 'Define Target Schemas for Writebacks', 2),
       (7, 'Conditional Mapping Rules', 13),
       (8, 'Publish additional composite record data to kafka', 1),
       (9, 'Update Sage Plugin to Handle Writebacks', 2),
       (10, 'Update Zoho Plugin to handle Writebacks', 8)

CREATE TABLE w3.dev.Sprints
(
    id   INT NOT NULL PRIMARY KEY,
    name VARCHAR(20) UNIQUE,
)
ALTER TABLE w3.dev.Sprints
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)
INSERT INTO w3.dev.Sprints (id, name)
VALUES (1, '2019s1'),
       (2, '2019s2')


CREATE TABLE w3.dev.Assignments
(
    id          INT NOT NULL IDENTITY PRIMARY KEY,
    developerID INT NOT NULL,
    taskID      INT NOT NULL,
    sprintID    INT NULL,
    FOREIGN KEY (developerID) REFERENCES w3.dev.Developers (id),
    FOREIGN KEY (taskID) REFERENCES w3.dev.Tasks (id),
    FOREIGN KEY (sprintID) REFERENCES w3.dev.Sprints (id)
)

ALTER TABLE w3.dev.Assignments
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)

INSERT INTO w3.dev.Assignments (developerID, taskID, sprintID)
VALUES (1, 1, 1),
       (1, 2, 1),
       (1, 3, 2),
       (2, 4, 1),
       (2, 5, 2),
       (3, 6, 1),
       (3, 7, 2),
       (4, 8, 1),
       (4, 9, 1),
       (4, 10, 2);

GO



CREATE TABLE w3.dbo.Types
(
    "int"              INT NOT NULL PRIMARY KEY,
    "bigint"           BIGINT,
    "numeric"          NUMERIC(18, 5),
    "bit"              BIT NOT NULL,
    "smallint"         SMALLINT,
    "decimal"          DECIMAL(18, 4),
    "smallmoney"       SMALLMONEY,
    "tinyint"          TINYINT,
    "money"            MONEY,
    "float"            FLOAT,
    "real"             REAL,
    "date"             DATE,
    "datetimeoffset"   DATETIMEOFFSET,
    "datetime2"        DATETIME2,
    "smalldatetime"    SMALLDATETIME,
    "datetime"         DATETIME,
    "time"             TIME,
    "char"             CHAR(6),
    "varchar"          VARCHAR(10),
    "text"             TEXT,
    "nchar"            NCHAR(6),
    "nvarchar"         NVARCHAR(10),
    "ntext"            NTEXT,
    "binary"           BINARY(3),
    "varbinary"        VARBINARY(100),
    "uniqueidentifier" UNIQUEIDENTIFIER
)


GO
;

INSERT INTO w3.dbo.Types
VALUES (42,
        9223372036854775807, -- bigint
        1234.5678,-- numeric
        1,-- bit
        123,-- smallint
        1234.5678,-- decimal
        12.56,-- smallmoney
        12,-- tinyint
        1234.56,-- money
        123456.789,-- float
        123456.789,-- real
        '1970-1-1',-- date
        '2007-05-08 12:35:29.1234567 +12:15', -- datetimeoffset
        '2007-05-08 12:35:29.1234567+12:15', -- datetime2
        '2007-05-08 12:35:29.123',-- smalldatetime
        '2007-05-08 12:35:29.123',-- datetime
        '12:35:29.123',-- time
        'char',-- char
        'abc',-- varchar
        'abc',-- text
        'nchar',-- nchar
        'nvarchar',-- nvarchar
        'ntext',-- ntext
        CAST('abc' AS BINARY(3)),-- binary
        CAST('cde' AS VARBINARY(6)),-- varbinary
        'ff0df4a2-c2b6-11ea-93a8-e335bc66abae')

GO
;

CREATE TABLE w3.dbo.Agents
(
    "AGENT_CODE"   CHAR(4) NOT NULL PRIMARY KEY,
    "AGENT_NAME"   VARCHAR(40),
    "WORKING_AREA" VARCHAR(35),
    "COMMISSION"   FLOAT,
    "PHONE_NO"     CHAR(12),
    "UPDATED_AT"   DATETIMEOFFSET,
    "BIOGRAPHY"    VARCHAR(MAX)
)


GO
;

INSERT INTO w3.dbo.Agents
VALUES ('A007', 'Ramasundar', 'Bangalore', NULL, '077-25814763', '1969-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A003', 'Alex', 'London', '0.13', '075-12458969', '1970-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A008', 'Alford', 'New York', '0.12', '044-25874365', '1970-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A011', 'Ravi Kumar', 'Bangalore', '0.15', '077-45625874', '1970-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A010', 'Santakumar', 'Chennai', '0.14', '007-22388644', '1970-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A012', 'Lucida', 'San Jose', '0.12', '044-52981425', '1971-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A005', 'Anderson', 'Brisban', '0.13', '045-21447739', '1971-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A001', 'Subbarao', 'Bangalore', '0.14', '077-12346674', '1971-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A002', 'Mukesh', 'Mumbai', '0.11', '029-12358964', '1971-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A006', 'McDen', 'London', '0.15', '078-22255588', '1971-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A004', 'Ivan', 'Torento', '0.15', '008-22544166', '1971-01-02T00:00:00Z', '');
INSERT INTO w3.dbo.Agents
VALUES ('A009', 'Benjamin', 'Hampshair', '0.11', '008-22536178', '1971-01-02T00:00:00Z', '');

GO
;

CREATE TABLE w3.dbo.Customers
(
    "CUST_CODE"       VARCHAR(6)     NOT NULL PRIMARY KEY,
    "CUST_NAME"       VARCHAR(40)    NOT NULL,
    "CUST_CITY"       VARCHAR(MAX),
    "WORKING_AREA"    VARCHAR(35)    NOT NULL,
    "CUST_COUNTRY"    VARCHAR(20)    NOT NULL,
    "GRADE"           NUMERIC,
    "OPENING_AMT"     NUMERIC(12, 2) NOT NULL,
    "RECEIVE_AMT"     NUMERIC(12, 2) NOT NULL,
    "PAYMENT_AMT"     NUMERIC(12, 2) NOT NULL,
    "OUTSTANDING_AMT" NUMERIC(12, 2) NOT NULL,
    "PHONE_NO"        VARCHAR(17)    NOT NULL,
    "AGENT_CODE"      CHAR(4)        NOT NULL REFERENCES w3.dbo.Agents
);

GO
;

INSERT INTO w3.dbo.Customers
VALUES ('C00013', 'Holmes', 'London', 'London', 'UK', '2', '6000.00', '5000.00', '7000.00', '4000.00', 'BBBBBBB',
        'A003');
INSERT INTO w3.dbo.Customers
VALUES ('C00001', 'Micheal', 'New York', 'New York', 'USA', '2', '3000.00', '5000.00', '2000.00', '6000.00', 'CCCCCCC',
        'A008');
INSERT INTO w3.dbo.Customers
VALUES ('C00020', 'Albert', 'New York', 'New York', 'USA', '3', '5000.00', '7000.00', '6000.00', '6000.00', 'BBBBSBB',
        'A008');
INSERT INTO w3.dbo.Customers
VALUES ('C00025', 'Ravindran', 'Bangalore', 'Bangalore', 'India', '2', '5000.00', '7000.00', '4000.00', '8000.00',
        'AVAVAVA', 'A011');
INSERT INTO w3.dbo.Customers
VALUES ('C00024', 'Cook', 'London', 'London', 'UK', '2', '4000.00', '9000.00', '7000.00', '6000.00', 'FSDDSDF', 'A006');
INSERT INTO w3.dbo.Customers
VALUES ('C00015', 'Stuart', 'London', 'London', 'UK', '1', '6000.00', '8000.00', '3000.00', '11000.00', 'GFSGERS',
        'A003');
INSERT INTO w3.dbo.Customers
VALUES ('C00002', 'Bolt', 'New York', 'New York', 'USA', '3', '5000.00', '7000.00', '9000.00', '3000.00', 'DDNRDRH',
        'A008');
INSERT INTO w3.dbo.Customers
VALUES ('C00018', 'Fleming', 'Brisban', 'Brisban', 'Australia', '2', '7000.00', '7000.00', '9000.00', '5000.00',
        'NHBGVFC', 'A005');
INSERT INTO w3.dbo.Customers
VALUES ('C00021', 'Jacks', 'Brisban', 'Brisban', 'Australia', '1', '7000.00', '7000.00', '7000.00', '7000.00',
        'WERTGDF', 'A005');
INSERT INTO w3.dbo.Customers
VALUES ('C00019', 'Yearannaidu', 'Chennai', 'Chennai', 'India', '1', '8000.00', '7000.00', '7000.00', '8000.00',
        'ZZZZBFV', 'A010');
INSERT INTO w3.dbo.Customers
VALUES ('C00005', 'Sasikant', 'Mumbai', 'Mumbai', 'India', '1', '7000.00', '11000.00', '7000.00', '11000.00',
        '147-25896312', 'A002');
INSERT INTO w3.dbo.Customers
VALUES ('C00007', 'Ramanathan', 'Chennai', 'Chennai', 'India', '1', '7000.00', '11000.00', '9000.00', '9000.00',
        'GHRDWSD', 'A010');
INSERT INTO w3.dbo.Customers
VALUES ('C00022', 'Avinash', 'Mumbai', 'Mumbai', 'India', '2', '7000.00', '11000.00', '9000.00', '9000.00',
        '113-12345678', 'A002');
INSERT INTO w3.dbo.Customers
VALUES ('C00004', 'Winston', 'Brisban', 'Brisban', 'Australia', '1', '5000.00', '8000.00', '7000.00', '6000.00',
        'AAAAAAA', 'A005');
INSERT INTO w3.dbo.Customers
VALUES ('C00023', 'Karl', 'London', 'London', 'UK', '0', '4000.00', '6000.00', '7000.00', '3000.00', 'AAAABAA', 'A006');
INSERT INTO w3.dbo.Customers
VALUES ('C00006', 'Shilton', 'Torento', 'Torento', 'Canada', '1', '10000.00', '7000.00', '6000.00', '11000.00',
        'DDDDDDD', 'A004');
INSERT INTO w3.dbo.Customers
VALUES ('C00010', 'Charles', 'Hampshair', 'Hampshair', 'UK', '3', '6000.00', '4000.00', '5000.00', '5000.00', 'MMMMMMM',
        'A009');
INSERT INTO w3.dbo.Customers
VALUES ('C00017', 'Srinivas', 'Bangalore', 'Bangalore', 'India', '2', '8000.00', '4000.00', '3000.00', '9000.00',
        'AAAAAAB', 'A007');
INSERT INTO w3.dbo.Customers
VALUES ('C00012', 'Steven', 'San Jose', 'San Jose', 'USA', '1', '5000.00', '7000.00', '9000.00', '3000.00', 'KRFYGJK',
        'A012');
INSERT INTO w3.dbo.Customers
VALUES ('C00008', 'Karolina', 'Torento', 'Torento', 'Canada', '1', '7000.00', '7000.00', '9000.00', '5000.00',
        'HJKORED', 'A004');
INSERT INTO w3.dbo.Customers
VALUES ('C00003', 'Martin', 'Torento', 'Torento', 'Canada', '2', '8000.00', '7000.00', '7000.00', '8000.00', 'MJYURFD',
        'A004');
INSERT INTO w3.dbo.Customers
VALUES ('C00009', 'Ramesh', 'Mumbai', 'Mumbai', 'India', '3', '8000.00', '7000.00', '3000.00', '12000.00', 'Phone No',
        'A002');
INSERT INTO w3.dbo.Customers
VALUES ('C00014', 'Rangarappa', 'Bangalore', 'Bangalore', 'India', '2', '8000.00', '11000.00', '7000.00', '12000.00',
        'AAAATGF', 'A001');
INSERT INTO w3.dbo.Customers
VALUES ('C00016', 'Venkatpati', 'Bangalore', 'Bangalore', 'India', '2', '8000.00', '11000.00', '7000.00', '12000.00',
        'JRTVFDD', 'A007');
INSERT INTO w3.dbo.Customers
VALUES ('C00011', 'Sundariya', 'Chennai', 'Chennai', 'India', '3', '7000.00', '11000.00', '7000.00', '11000.00',
        'PPHGRTS', 'A010');

GO
;


CREATE TABLE w3.fact.Orders
(
    "ORD_NUM"         NUMERIC(6, 0)  NOT NULL PRIMARY KEY,
    "ORD_AMOUNT"      NUMERIC(12, 2) NOT NULL,
    "ADVANCE_AMOUNT"  NUMERIC(12, 2) NULL,
    "ORD_DATE"        DATE           NOT NULL,
    "CUST_CODE"       VARCHAR(6)     NOT NULL REFERENCES w3.dbo.Customers,
    "AGENT_CODE"      CHAR(4)        NOT NULL REFERENCES w3.dbo.Agents,
    "ORD_DESCRIPTION" VARCHAR(60)    NOT NULL
);

GO
;

INSERT INTO w3.fact.Orders
VALUES ('200100', '1000.00', NULL, '08/01/2008', 'C00013', 'A003', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200110', '3000.00', '500.00', '04/15/2008', 'C00019', 'A010', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200107', '4500.00', '900.00', '08/30/2008', 'C00007', 'A010', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200112', '2000.00', '400.00', '05/30/2008', 'C00016', 'A007', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200113', '4000.00', '600.00', '06/10/2008', 'C00022', 'A002', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200102', '2000.00', '300.00', '05/25/2008', 'C00012', 'A012', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200114', '3500.00', '2000.00', '08/15/2008', 'C00002', 'A008', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200122', '2500.00', '400.00', '09/16/2008', 'C00003', 'A004', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200118', '500.00', '100.00', '07/20/2008', 'C00023', 'A006', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200119', '4000.00', '700.00', '09/16/2008', 'C00007', 'A010', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200121', '1500.00', '600.00', '09/23/2008', 'C00008', 'A004', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200130', '2500.00', '400.00', '07/30/2008', 'C00025', 'A011', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200134', '4200.00', '1800.00', '09/25/2008', 'C00004', 'A005', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200108', '4000.00', '600.00', '02/15/2008', 'C00008', 'A004', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200103', '1500.00', '700.00', '05/15/2008', 'C00021', 'A005', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200105', '2500.00', '500.00', '07/18/2008', 'C00025', 'A011', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200109', '3500.00', '800.00', '07/30/2008', 'C00011', 'A010', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200101', '3000.00', '1000.00', '07/15/2008', 'C00001', 'A008', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200111', '1000.00', '300.00', '07/10/2008', 'C00020', 'A008', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200104', '1500.00', '500.00', '03/13/2008', 'C00006', 'A004', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200106', '2500.00', '700.00', '04/20/2008', 'C00005', 'A002', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200125', '2000.00', '600.00', '10/10/2008', 'C00018', 'A005', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200117', '800.00', '200.00', '10/20/2008', 'C00014', 'A001', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200123', '500.00', '100.00', '09/16/2008', 'C00022', 'A002', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200120', '500.00', '100.00', '07/20/2008', 'C00009', 'A002', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200116', '500.00', '100.00', '07/13/2008', 'C00010', 'A009', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200124', '500.00', '100.00', '06/20/2008', 'C00017', 'A007', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200126', '500.00', '100.00', '06/24/2008', 'C00022', 'A002', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200129', '2500.00', '500.00', '07/20/2008', 'C00024', 'A006', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200127', '2500.00', '400.00', '07/20/2008', 'C00015', 'A003', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200128', '3500.00', '1500.00', '07/20/2008', 'C00009', 'A002', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200135', '2000.00', '800.00', '09/16/2008', 'C00007', 'A010', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200131', '900.00', '150.00', '08/26/2008', 'C00012', 'A012', 'SOD');
INSERT INTO w3.fact.Orders
VALUES ('200133', '1200.00', '400.00', '06/29/2008', 'C00009', 'A002', 'SOD');

GO


CREATE OR ALTER VIEW dbo.[Agents per Working Area] (WORKING_AREA, COUNT)
AS
SELECT WORKING_AREA, COUNT(AGENT_CODE)
FROM w3.dbo.Agents
GROUP BY WORKING_AREA;

GO
;

CREATE TABLE w3.dbo.PrePost
(
    Message VARCHAR(50)
)

GO

CREATE OR
ALTER PROCEDURE InsertIntoTypes @int INT,
                                @bigint BIGINT,
                                @numeric NUMERIC(18, 5),
                                @bit BIT,
                                @smallint SMALLINT,
                                @decimal DECIMAL(18, 4),
                                @smallmoney SMALLMONEY,
                                @tinyint TINYINT,
                                @money MONEY,
                                @float FLOAT,
                                @real REAL,
                                @date DATE,
                                @datetimeoffset DATETIMEOFFSET,
                                @datetime2 DATETIME2,
                                @smalldatetime SMALLDATETIME,
                                @datetime DATETIME,
                                @time TIME,
                                @char CHAR(6),
                                @varchar VARCHAR(10),
                                @text TEXT,
                                @nchar NCHAR(6),
                                @nvarchar NVARCHAR(10),
                                @ntext NTEXT,
                                @uniqueidentifier UNIQUEIDENTIFIER
AS
BEGIN
    INSERT INTO w3.dbo.Types
    VALUES (@int,
            @bigint,
            @numeric,
            @bit,
            @smallint,
            @decimal,
            @smallmoney,
            @tinyint,
            @money,
            @float,
            @real,
            @date,
            @datetimeoffset,
            @datetime2,
            @smalldatetime,
            @datetime,
            @time,
            @char,
            @varchar,
            @text,
            @nchar,
            @nvarchar,
            @ntext,
            NULL,
            NULL,
            @uniqueidentifier)
END
GO

CREATE OR
ALTER PROCEDURE dev.TEST @AgentId CHAR(4),
                         @Name VARCHAR(40),
                         @Commission FLOAT
AS
BEGIN
    UPDATE w3.dbo.Agents
    SET AGENT_NAME = @Name,
        COMMISSION = @Commission
    WHERE AGENT_CODE = @AgentId
END
GO


IF TYPE_ID(N'AccountNumber') IS NULL
    BEGIN

        CREATE TYPE [AccountNumber] FROM NVARCHAR(15) NULL;
        CREATE TYPE [Flag] FROM BIT NOT NULL;
        CREATE TYPE [NameStyle] FROM BIT NOT NULL;
        CREATE TYPE [Name] FROM NVARCHAR(50) NULL;

        CREATE TYPE [OrderNumber] FROM NVARCHAR(25) NULL;
        CREATE TYPE [Phone] FROM NVARCHAR(25) NULL;
    END

GO


IF NOT exists(SELECT *
              FROM w3.sys.schemas
              WHERE
                  name = 'HumanResources')
    BEGIN
        EXEC sp_executesql N'CREATE SCHEMA HumanResources'
    END;

IF NOT exists(SELECT *
              FROM w3.sys.schemas
              WHERE
                  name = 'Person')
    BEGIN
        EXEC sp_executesql N'CREATE SCHEMA Person'
    END;


/******************************************88


The tables below are copied from the Adventureworks2012 database.


 */

IF OBJECT_ID('w3.HumanResources.Employee', 'U') IS NOT NULL
    DROP TABLE w3.HumanResources.[Employee];


IF OBJECT_ID('w3.Person.Person', 'U') IS NOT NULL
    DROP TABLE w3.Person.Person;


CREATE TABLE Person.Person
(
    BusinessEntityID      INT                               NOT NULL
        CONSTRAINT PK_Person_BusinessEntityID
            PRIMARY KEY,
    PersonType            NVARCHAR(2),
    NameStyle             NameStyle,
    Title                 NVARCHAR(8),
    FirstName             Name                              NOT NULL,
    MiddleName            Name,
    LastName              Name                              NOT NULL,
    Suffix                NVARCHAR(10),
    EmailPromotion        INT
        CONSTRAINT DF_Person_EmailPromotion DEFAULT 0       NOT NULL,
    AdditionalContactInfo XML,
    Demographics          XML,
    rowguid               UNIQUEIDENTIFIER
        CONSTRAINT DF_Person_rowguid DEFAULT newid()        NOT NULL,
    ModifiedDate          DATETIME
        CONSTRAINT DF_Person_ModifiedDate DEFAULT getdate() NOT NULL
)
GO

INSERT INTO w3.Person.Person (BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName,
                              LastName, Suffix, EmailPromotion, AdditionalContactInfo, Demographics,
                              rowguid, ModifiedDate)
VALUES (1, 'EM', 0, NULL, 'Terri', 'Lee', 'Duffy', NULL, 0, NULL,
        '
        <IndividualSurvey xmlns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey">
            <TotalPurchaseYTD>0</TotalPurchaseYTD>
        </IndividualSurvey>',
        '92C4279F-1207-48A3-8448-4636514EB7E2', '2003-02-08 00:00:00.000');
GO
INSERT INTO w3.Person.Person (BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName,
                              LastName, Suffix, EmailPromotion, AdditionalContactInfo, Demographics,
                              rowguid, ModifiedDate)
VALUES (2, 'EM', 0, NULL, 'Terri', 'Lee', 'Duffy', NULL, 1, NULL,
        '
        <IndividualSurvey xmlns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey">
            <TotalPurchaseYTD>0</TotalPurchaseYTD>
        </IndividualSurvey>',
        'D8763459-8AA8-47CC-AFF7-C9079AF79033', '2002-02-24 00:00:00.000');
GO

INSERT INTO w3.Person.Person (BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName,
                              LastName, Suffix, EmailPromotion, AdditionalContactInfo, Demographics,
                              rowguid, ModifiedDate)
VALUES (3, 'EM', 0, NULL, 'Roberto', NULL, 'Tamburello', NULL, 0, NULL,
        '
        <IndividualSurvey xmlns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey">
            <TotalPurchaseYTD>0</TotalPurchaseYTD>
        </IndividualSurvey>',
        'E1A2555E-0828-434B-A33B-6F38136A37DE', '2001-12-05 00:00:00.000');
GO

INSERT INTO w3.Person.Person (BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName,
                              LastName, Suffix, EmailPromotion, AdditionalContactInfo, Demographics,
                              rowguid, ModifiedDate)
VALUES (4, 'EM', 0, NULL, 'Rob', NULL, 'Walters', NULL, 0, NULL,
        '
        <IndividualSurvey xmlns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey">
            <TotalPurchaseYTD>0</TotalPurchaseYTD>
        </IndividualSurvey>',
        'F2D7CE06-38B3-4357-805B-F4B6B71C01FF', '2001-12-29 00:00:00.000');
GO

INSERT INTO w3.Person.Person (BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName,
                              LastName, Suffix, EmailPromotion, AdditionalContactInfo, Demographics,
                              rowguid, ModifiedDate)
VALUES (5, 'EM', 0, 'Ms.', 'Gail', 'A', 'Erickson', NULL, 0, NULL,
        '
        <IndividualSurvey xmlns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey">
            <TotalPurchaseYTD>0</TotalPurchaseYTD>
        </IndividualSurvey>',
        'F3A3F6B4-AE3B-430C-A754-9F2231BA6FEF', '2002-01-30 00:00:00.000');
GO


CREATE TABLE w3.HumanResources.[Employee]
(
    BusinessEntityID    INT                         NOT NULL
        CONSTRAINT PK_Employee_BusinessEntityID
            PRIMARY KEY
        CONSTRAINT FK_Employee_Person_BusinessEntityID
            REFERENCES Person.Person (BusinessEntityID),
    [NationalIDNumber]  [nvarchar](15)              NOT NULL,
    [LoginID]           [nvarchar](256)             NOT NULL,
    [OrganizationNode]  [hierarchyid]               NULL,
    [OrganizationLevel] AS OrganizationNode.GetLevel(),
    [JobTitle]          [nvarchar](50)              NOT NULL,
    [BirthDate]         [date]                      NOT NULL,
    [MaritalStatus]     [nchar](1)                  NOT NULL,
    [Gender]            [nchar](1)                  NOT NULL,
    [HireDate]          [date]                      NOT NULL,
    [SalariedFlag]      [Flag]                      NOT NULL
        CONSTRAINT [DF_Employee_SalariedFlag] DEFAULT (1),
    [VacationHours]     [smallint]                  NOT NULL
        CONSTRAINT [DF_Employee_VacationHours] DEFAULT (0),
    [SickLeaveHours]    [smallint]                  NOT NULL
        CONSTRAINT [DF_Employee_SickLeaveHours] DEFAULT (0),
    [CurrentFlag]       [Flag]                      NOT NULL
        CONSTRAINT [DF_Employee_CurrentFlag] DEFAULT (1),
    [rowguid]           UNIQUEIDENTIFIER ROWGUIDCOL NOT NULL
        CONSTRAINT [DF_Employee_rowguid] DEFAULT (NEWID()),
    [ModifiedDate]      [datetime]                  NOT NULL
        CONSTRAINT [DF_Employee_ModifiedDate] DEFAULT (GETDATE()),
    CONSTRAINT [CK_Employee_BirthDate] CHECK ([BirthDate] BETWEEN '1930-01-01' AND DATEADD(YEAR, -18, GETDATE())),
    CONSTRAINT [CK_Employee_MaritalStatus] CHECK (UPPER([MaritalStatus]) IN ('M', 'S')), -- Married or Single
    CONSTRAINT [CK_Employee_HireDate] CHECK ([HireDate] BETWEEN '1996-07-01' AND DATEADD(DAY, 1, GETDATE())),
    CONSTRAINT [CK_Employee_Gender] CHECK (UPPER([Gender]) IN ('M', 'F')),               -- Male or Female
    CONSTRAINT [CK_Employee_VacationHours] CHECK ([VacationHours] BETWEEN -40 AND 240),
    CONSTRAINT [CK_Employee_SickLeaveHours] CHECK ([SickLeaveHours] BETWEEN 0 AND 120),
) ON [PRIMARY];

GO

ALTER TABLE w3.HumanResources.Employee
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)

GO
INSERT INTO HumanResources.Employee (BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, JobTitle, BirthDate,
                                     MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours,
                                     CurrentFlag, rowguid, ModifiedDate)
VALUES (1, '295847284', 'adventure-works\ken0', 0x, 'Chief Executive Officer', '1963-03-28', 'S', 'M', '2003-02-15', 1,
        99, 69, 1, 'F01251E5-96A3-448D-981E-0F99D789110D', '2008-07-31 00:00:00.000');
GO

INSERT INTO HumanResources.Employee (BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, JobTitle, BirthDate,
                                     MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours,
                                     CurrentFlag, rowguid, ModifiedDate)
VALUES (2, '245797967', 'adventure-works\terri0', 0x58, 'Vice President of Engineering', '1965-09-27', 'S', 'F',
        '2002-03-03', 1, 1, 20, 1, '45E8F437-670D-4409-93CB-F9424A40D6EE', '2008-07-31 00:00:00.000');
GO
INSERT INTO HumanResources.Employee (BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, JobTitle, BirthDate,
                                     MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours,
                                     CurrentFlag, rowguid, ModifiedDate)
VALUES (3, '509647174', 'adventure-works\roberto0', 0x5AC0, 'Engineering Manager', '1945-05-24', 'M', 'M', '2001-12-12',
        1, 2, 21, 1, '9BBBFB2C-EFBB-4217-9AB7-F97689328841', '2008-07-31 00:00:00.000');
GO
INSERT INTO HumanResources.Employee (BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, JobTitle, BirthDate,
                                     MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours,
                                     CurrentFlag, rowguid, ModifiedDate)
VALUES (4, '112457891', 'adventure-works\rob0', 0x5AD6, 'Senior Tool Editor', '1954-02-08', 'M', 'F', '2002-01-05', 0,
        48, 80, 1, '59747955-87B8-443F-8ED4-F8AD3AFDF3A9', '2008-07-31 00:00:00.000');

GO

IF NOT exists(SELECT *
              FROM w3.sys.schemas
              WHERE
                  name = 'ReplicationTest')
    BEGIN
        EXEC sp_executesql N'CREATE SCHEMA ReplicationTest'
    END;


GO

/*


 The following tables are for testing change tracking with unique identifiers

 */

IF OBJECT_ID('w3.dbo.UniqueIdentifier', 'U') IS NOT NULL
    DROP TABLE w3.dbo.UniqueIdentifier;
GO

CREATE TABLE w3.dbo.UniqueIdentifier
(
    "id"        UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    "firstName" VARCHAR(40),
    "lastName"  VARCHAR(40)
)

GO


CREATE OR ALTER VIEW dbo.[UniqueIdentifierView] (id, fullName)
AS
SELECT id, firstName + ' ' + lastName AS fullName
FROM w3.dbo.UniqueIdentifier;

GO

INSERT INTO w3.dbo.UniqueIdentifier
VALUES ('0E984725-FACE-4BF4-9960-E1C80E27ABA0', 'steve', 'ruble');
INSERT INTO w3.dbo.UniqueIdentifier
VALUES ('6F9619FF-CAFE-D011-B42D-00C04FC964FF', 'casey', 'brady');

GO

/* After inserting, enable change tracking. Otherwise inserts show up as changes, making testing confusing */
ALTER TABLE w3.dbo.UniqueIdentifier
    ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)
GO
/*

 Clean up replication tables if present:

 */

IF OBJECT_ID(' w3.ReplicationTest.Golden', 'U') IS NOT NULL
    DROP TABLE w3.ReplicationTest.Golden;

IF OBJECT_ID(' w3.ReplicationTest.Versions', 'U') IS NOT NULL
    DROP TABLE w3.ReplicationTest.Versions;
