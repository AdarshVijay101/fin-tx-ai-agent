-- database/02_schema.sql
USE FinDB;
GO

-- Accounts
IF OBJECT_ID('dbo.Accounts') IS NULL
BEGIN
  CREATE TABLE dbo.Accounts
  (
    AccountID    INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName NVARCHAR(100) NOT NULL,
    Balance      DECIMAL(19,4) NOT NULL CONSTRAINT DF_Accounts_Balance DEFAULT (0),
    Status       TINYINT NOT NULL CONSTRAINT DF_Accounts_Status DEFAULT (1), -- 1=Active
    CreatedAt    DATETIME2(3) NOT NULL CONSTRAINT DF_Accounts_CreatedAt DEFAULT SYSUTCDATETIME()
  );
END
GO
CREATE INDEX IX_Accounts_Status ON dbo.Accounts(Status);
GO

-- Transactions
IF OBJECT_ID('dbo.Transactions') IS NULL
BEGIN
  CREATE TABLE dbo.Transactions
  (
    TransactionID   BIGINT IDENTITY(1,1) PRIMARY KEY,
    FromAccountID   INT NULL,
    ToAccountID     INT NULL,
    Amount          DECIMAL(19,4) NOT NULL,
    Kind            CHAR(1) NOT NULL, -- 'T' transfer, 'D' deposit, 'W' withdrawal
    Ref             NVARCHAR(64) NULL, -- idempotency key (optional)
    Status          TINYINT NOT NULL CONSTRAINT DF_Tx_Status DEFAULT (1), -- 1=Success, 0=Failed
    CreatedAt       DATETIME2(3) NOT NULL CONSTRAINT DF_Tx_CreatedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CK_Tx_PositiveAmount CHECK (Amount > 0),
    CONSTRAINT FK_Tx_FromAccount FOREIGN KEY (FromAccountID) REFERENCES dbo.Accounts(AccountID),
    CONSTRAINT FK_Tx_ToAccount   FOREIGN KEY (ToAccountID)   REFERENCES dbo.Accounts(AccountID)
  );
END
GO
CREATE INDEX IX_Transactions_Ref ON dbo.Transactions(Ref);
GO

-- Error log
IF OBJECT_ID('dbo.ErrorLog') IS NULL
BEGIN
  CREATE TABLE dbo.ErrorLog
  (
    ErrorID       BIGINT IDENTITY(1,1) PRIMARY KEY,
    ProcName      SYSNAME NULL,
    ErrorNumber   INT NULL,
    ErrorSeverity INT NULL,
    ErrorState    INT NULL,
    ErrorLine     INT NULL,
    ErrorMessage  NVARCHAR(4000) NOT NULL,
    Context       NVARCHAR(4000) NULL,
    OccurredAt    DATETIME2(3) NOT NULL CONSTRAINT DF_Error_Occ DEFAULT SYSUTCDATETIME()
  );
END
GO
