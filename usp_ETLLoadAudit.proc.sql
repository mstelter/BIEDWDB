CREATE PROCEDURE [dbo].[usp_ETLLoadAudit] 
	@StartEnd CHAR(1),
	@ParentETLLoadID INT = NULL,
	@PackageName VARCHAR(50) = NULL,
	@ETLLoadID INT = NULL,
	@ExtractRowCount int = NULL,
	@InsertRowCount int = NULL,
	@UpdateRowCount int = NULL,
	@DeleteRowCount int = NULL,
	@ErrorRowCnt int = NULL,
	@ErrorCode int = NULL

/*************************************************************
USE EDW
GO 
Description: Populate EDW ETLLoadAudit table with Parent/Child start time, end time, record counts & errors

Revision History
Date      Author	Revision Description
7/19/2011	VV		Created
7/21/2011	MS		Modified to add usage, Change Insert to Start and Update to end, rename fields from AuditID to EDWLoadID
8/10/2011   VV      Added DeleteRowCount
USAGE:  Variables can be declared and populated and used for the parameters to call the SP 
		or the System Variable (@@ROWCOUNT, @@ERRORCODE) can be used directly.
		If the Parameters are not specified in the call you must insert NULL in the place of any skipped parameter.

--------At the start of a Parent------

DECLARE	@ParentETLLoadID int
EXEC	@ParentETLLoadID = [dbo].[usp_ETLLoadAudit] @StartEnd = 'S', @PackageName = 'Name of Parent Procedure'

--------At the start of an orphan------

DECLARE @ETLLoadID int, @ExtractRowCount int, @InsertRowCount int, @UpdateRowCount int, @ErrorRowCnt int, @ErrorCode int
EXEC	@ETLLoadID = [dbo].[usp_ETLLoadAudit] @StartEnd = 'S', @PackageName = 'Name of Procedure'

------At the start of the child------

DECLARE @ETLLoadID int, @ExtractRowCount int, @InsertRowCount int, @UpdateRowCount int, @ErrorRowCnt int, @ErrorCode int
EXEC	@ETLLoadID = [dbo].[usp_ETLLoadAudit] @StartEnd = 'S', @ParentETLLoadID, @PackageName = 'Name of Procedure'

------At the end of the child (or orphan)------

/***These values are declared at the start and populated as the procedure runs***/
/***NULL can be passed for non applicable values***/
/***@ETLLoadID value is from the start***/

SET	@ExtractRowCount = 99  --Use SQL to get the count that we expect to update or insert
SET	@InsertRowCount =  @@Rowcount  --Actual insert count
SET	@UpdateRowCount =  @@Rowcount  --Actual update amount
SET	@ErrorRowCnt = 0  --Count of rows that did not get inserted/updated due to error
SET	@ErrorCode = NULL  --Error code if there is an error

EXEC [dbo].[usp_ETLLoadAudit] @StartEnd = 'E', @ETLLoadID = @ETLLoadID, @ExtractRowCount = @ExtractRowCount, @InsertRowCount = @InsertRowCount, @UpdateRowCount = @UpdateRowCount,@ErrorRowCnt = @ErrorRowCnt, @ErrorCode = @ErrorCode

------At the end of the Parent------

EXEC [dbo].[usp_ETLLoadAudit] 'E',NULL,NULL,@ParentETLLoadID,0,0,0,0 --@ParentETLLoadID value is from the start
*************************************************************/

AS
BEGIN
SET NOCOUNT ON;

	IF (@StartEnd = 'S')
	BEGIN
		DECLARE @AuditScopeID int
		DECLARE @ExecuteStopDate Datetime
		SET  @ExecuteStopDate = '2999/12/31'
		
		INSERT INTO [dbo].[ETLLoadAudit] (ParentETLLoadID,PackageName,ExecuteStartDate,ExecuteStopDate) 
		VALUES (@ParentETLLoadID,@PackageName,GETDATE(),@ExecuteStopDate)
		SET @AuditScopeID = SCOPE_IDENTITY()
		RETURN @AuditScopeID
		
	END
	ELSE IF (@StartEnd = 'E') 
	BEGIN
			UPDATE [dbo].[ETLLoadAudit]
			SET ExecuteStopDate = GETDATE(),
				ExtractRowCount = @ExtractRowCount,
				InsertRowCount = @InsertRowCount,
				UpdateRowCount = @UpdateRowCount,
				DeleteRowCount = @DeleteRowCount,
				ErrorRowCnt = @ErrorRowCnt,
				ErrorCode = @ErrorCode  
			WHERE ETLLoadID = @ETLLoadID 
		
	END

END;
