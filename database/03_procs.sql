-- database/03_procs.sql
USE FinDB;
GO

-- CreateAccount
IF OBJECT_ID('dbo.usp_CreateAccount') IS NOT NULL DROP PROC dbo.usp_CreateAccount;
GO
CREATE PROC dbo.usp_CreateAccount
  @CustomerName NVARCHAR(100),
  @OpeningBalance DECIMAL(19,4) = 0
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  BEGIN TRY
    BEGIN TRAN;

      INSERT INTO dbo.Accounts(CustomerName, Balance)
      VALUES(@CustomerName, @OpeningBalance);

    COMMIT;
  END TRY
  BEGIN CATCH
    DECLARE
      @ErrNum INT = ERROR_NUMBER(), @ErrSev INT = ERROR_SEVERITY(),
      @ErrSt INT = ERROR_STATE(), @ErrLine INT = ERROR_LINE(),
      @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();

    IF @@TRANCOUNT > 0 ROLLBACK;

    INSERT INTO dbo.ErrorLog(ProcName, ErrorNumber, ErrorSeverity, ErrorState, ErrorLine, ErrorMessage, Context)
    VALUES('usp_CreateAccount', @ErrNum, @ErrSev, @ErrSt, @ErrLine, @ErrMsg, CONCAT('CustomerName=', @CustomerName));

    THROW;
  END CATCH
END
GO

-- Deposit
IF OBJECT_ID('dbo.usp_Deposit') IS NOT NULL DROP PROC dbo.usp_Deposit;
GO
CREATE PROC dbo.usp_Deposit
  @AccountID INT,
  @Amount    DECIMAL(19,4),
  @Ref       NVARCHAR(64) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  BEGIN TRY
    IF @Amount <= 0 THROW 50001, 'Amount must be > 0', 1;

    BEGIN TRAN;

      UPDATE A WITH (UPDLOCK, ROWLOCK)
      SET Balance = Balance + @Amount
      FROM dbo.Accounts AS A
      WHERE A.AccountID = @AccountID AND A.Status = 1;

      IF @@ROWCOUNT = 0
        THROW 50002, 'Account not found or inactive', 1;

      INSERT INTO dbo.Transactions (FromAccountID, ToAccountID, Amount, Kind, Ref, Status)
      VALUES (NULL, @AccountID, @Amount, 'D', @Ref, 1);

    COMMIT;
  END TRY
  BEGIN CATCH
    DECLARE
      @ErrNum INT = ERROR_NUMBER(), @ErrSev INT = ERROR_SEVERITY(),
      @ErrSt INT = ERROR_STATE(), @ErrLine INT = ERROR_LINE(),
      @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();

    IF @@TRANCOUNT > 0 ROLLBACK;

    INSERT INTO dbo.ErrorLog(ProcName, ErrorNumber, ErrorSeverity, ErrorState, ErrorLine, ErrorMessage, Context)
    VALUES('usp_Deposit', @ErrNum, @ErrSev, @ErrSt, @ErrLine, @ErrMsg,
           CONCAT('AccountID=', @AccountID, '; Amount=', @Amount, '; Ref=', @Ref));

    THROW;
  END CATCH
END
GO

-- Withdraw
IF OBJECT_ID('dbo.usp_Withdraw') IS NOT NULL DROP PROC dbo.usp_Withdraw;
GO
CREATE PROC dbo.usp_Withdraw
  @AccountID INT,
  @Amount    DECIMAL(19,4),
  @Ref       NVARCHAR(64) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  BEGIN TRY
    IF @Amount <= 0 THROW 50001, 'Amount must be > 0', 1;

    BEGIN TRAN;

      DECLARE @bal DECIMAL(19,4);

      SELECT @bal = Balance
      FROM dbo.Accounts WITH (UPDLOCK, ROWLOCK)
      WHERE AccountID = @AccountID AND Status = 1;

      IF @bal IS NULL
        THROW 50002, 'Account not found or inactive', 1;

      IF @bal < @Amount
        THROW 50003, 'Insufficient funds', 1;

      UPDATE dbo.Accounts
      SET Balance = Balance - @Amount
      WHERE AccountID = @AccountID;

      INSERT INTO dbo.Transactions (FromAccountID, ToAccountID, Amount, Kind, Ref, Status)
      VALUES (@AccountID, NULL, @Amount, 'W', @Ref, 1);

    COMMIT;
  END TRY
  BEGIN CATCH
    DECLARE
      @ErrNum INT = ERROR_NUMBER(), @ErrSev INT = ERROR_SEVERITY(),
      @ErrSt INT = ERROR_STATE(), @ErrLine INT = ERROR_LINE(),
      @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();

    IF @@TRANCOUNT > 0 ROLLBACK;

    INSERT INTO dbo.ErrorLog(ProcName, ErrorNumber, ErrorSeverity, ErrorState, ErrorLine, ErrorMessage, Context)
    VALUES('usp_Withdraw', @ErrNum, @ErrSev, @ErrSt, @ErrLine, @ErrMsg,
           CONCAT('AccountID=', @AccountID, '; Amount=', @Amount, '; Ref=', @Ref));

    THROW;
  END CATCH
END
GO

-- TransferFunds
IF OBJECT_ID('dbo.usp_TransferFunds') IS NOT NULL DROP PROC dbo.usp_TransferFunds;
GO
CREATE PROC dbo.usp_TransferFunds
  @FromAccountID INT,
  @ToAccountID   INT,
  @Amount        DECIMAL(19,4),
  @Ref           NVARCHAR(64) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  BEGIN TRY
    IF @Amount <= 0             THROW 50001, 'Amount must be > 0', 1;
    IF @FromAccountID = @ToAccountID
                                THROW 50004, 'From and To accounts must differ', 1;

    BEGIN TRAN;

      DECLARE @fromBal DECIMAL(19,4);

      SELECT @fromBal = A.Balance
      FROM dbo.Accounts AS A WITH (UPDLOCK, ROWLOCK)
      WHERE A.AccountID = @FromAccountID AND A.Status = 1;

      IF @fromBal IS NULL
        THROW 50002, 'From account not found or inactive', 1;

      IF @fromBal < @Amount
        THROW 50003, 'Insufficient funds', 1;

      IF NOT EXISTS(SELECT 1 FROM dbo.Accounts WITH (UPDLOCK, ROWLOCK)
                    WHERE AccountID = @ToAccountID AND Status = 1)
        THROW 50005, 'To account not found or inactive', 1;

      UPDATE dbo.Accounts SET Balance = Balance - @Amount WHERE AccountID = @FromAccountID;
      UPDATE dbo.Accounts SET Balance = Balance + @Amount WHERE AccountID = @ToAccountID;

      INSERT INTO dbo.Transactions (FromAccountID, ToAccountID, Amount, Kind, Ref, Status)
      VALUES (@FromAccountID, @ToAccountID, @Amount, 'T', @Ref, 1);

    COMMIT;
  END TRY
  BEGIN CATCH
    DECLARE
      @ErrNum INT = ERROR_NUMBER(), @ErrSev INT = ERROR_SEVERITY(),
      @ErrSt INT = ERROR_STATE(), @ErrLine INT = ERROR_LINE(),
      @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();

    IF @@TRANCOUNT > 0 ROLLBACK;

    INSERT INTO dbo.ErrorLog(ProcName, ErrorNumber, ErrorSeverity, ErrorState, ErrorLine, ErrorMessage, Context)
    VALUES('usp_TransferFunds', @ErrNum, @ErrSev, @ErrSt, @ErrLine, @ErrMsg,
           CONCAT('From=', @FromAccountID, '; To=', @ToAccountID, '; Amount=', @Amount, '; Ref=', @Ref));

    THROW;
  END CATCH
END
GO
