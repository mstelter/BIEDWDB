/*************************************************************
USE EDW
GO 

Description: Populate EDW Audit table with Parent/Child start time, end time, record counts & errors

Revision History
Date      Author	Revision Description
7/19/2011	VV		Created
7/21/2011	MS		Modified to add usage

USAGE:
------At the start of the Parent (or orphan)------

DECLARE	@ParentAuditID int
EXEC	@ParentAuditID = [dbo].[usp_Audit] 'I',NULL,'Name of Procedure'

------At the start of an orphan------

DECLARE @AuditID int, @ExtractRowCount int, @InsertRowCount int, @UpdateRowCount int, @ErrorRowCnt int, @ErrorCode int
EXEC	@AuditID = [dbo].[usp_Audit] 'I',NULL,'Name of Procedure'

------At the start of the child------

DECLARE @AuditID int, @ExtractRowCount int, @InsertRowCount int, @UpdateRowCount int, @ErrorRowCnt int, @ErrorCode int
EXEC	@AuditID = [dbo].[usp_Audit] 'I',@ParentAuditID,'Name of Procedure'

------At the end of the Parent------

EXEC [dbo].[usp_Audit] 'U',NULL,NULL,@ParentAuditID --@ParentAuditID value is from the start

------At the end of the child (or orphan)------

***These values are declared at the start and populated as the procedure runs
***NULL can be passed for non applicable values
***@AuditID value is from the start

SET	@ExtractRowCount = 99  --Count that we expect to update or insert
SET	@InsertRowCount = 99  --Actual insert count
SET	@UpdateRowCount = 100  --Actual update amount
SET	@ErrorRowCnt = 0  --Count of rows that did not get inserted/updated due to error
SET	@ErrorCode = NULL  --Error code if there is an error

EXEC	@return_value = [dbo].[usp_Audit] 'U',NULL,NULL,@AuditID,@ExtractRowCount,@InsertRowCount,@UpdateRowCount,@ErrorRowCnt,	@ErrorCode
*************************************************************/

CREATE PROCEDURE [dbo].[usp_Audit] 
	@InsertUpdate CHAR(1),
	@ParentAuditID INT = NULL,
	@PackageName VARCHAR(30) = NULL,
	@AuditID INT = NULL,
	@ExtractRowCount int = NULL,
	@InsertRowCount int = NULL,
	@UpdateRowCount int = NULL,
	@ErrorRowCnt int = NULL,
	@ErrorCode int = NULL

AS
BEGIN
SET NOCOUNT ON;

	IF (@InsertUpdate = 'I')
	BEGIN
		DECLARE @AuditScopeID int
		DECLARE @ExecuteStopDate Datetime
		SET  @ExecuteStopDate = '2999/12/31'
		
		INSERT INTO [dbo].[DimAudit] (ParentAuditID,PackageName,ExecuteStartDate,ExecuteStopDate) 
		VALUES (@ParentAuditID,@PackageName,GETDATE(),@ExecuteStopDate)
		SET @AuditScopeID = SCOPE_IDENTITY()
		RETURN @AuditScopeID
		
	END
	ELSE IF (@InsertUpdate = 'U') 
	BEGIN
			UPDATE [dbo].[DimAudit]
			SET ExecuteStopDate = GETDATE(),
				ExtractRowCount = @ExtractRowCount,
				InsertRowCount = @InsertRowCount,
				UpdateRowCount = @UpdateRowCount,
				ErrorRowCnt = @ErrorRowCnt,
				ErrorCode = @ErrorCode  
			WHERE AuditID = @AuditID 
		
	END

END;
