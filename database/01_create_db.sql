-- database/01_create_db.sql
IF DB_ID('FinDB') IS NULL
BEGIN
  CREATE DATABASE FinDB;
END
GO
