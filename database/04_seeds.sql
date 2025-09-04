-- database/04_seed.sql
USE FinDB;
GO
EXEC dbo.usp_CreateAccount @CustomerName='Alice', @OpeningBalance=1000.00;
EXEC dbo.usp_CreateAccount @CustomerName='Bob',   @OpeningBalance=200.00;

SELECT * FROM dbo.Accounts;

