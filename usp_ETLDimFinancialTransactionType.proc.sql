
/* 
USE EDW
GO 
==============================================================================
Author		: Venkata Vempali
Create date	: 08/18/2011
Description	: Populate the EDW DimFinancialTransactionType table from v_DimFinancialTransactionType	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
09/15/2011	svelupalli	      Changing the proc to use DimFinancialTransactionType
10/28/2011	Juan X. Jacome		-- Added code to drop temp Tables (DBA Code Review)
==============================================================================
Usage Example:
Exec usp_ETLDimFinancialTransactionType
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimFinancialTransactionType]
AS     
BEGIN


DECLARE @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		
SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLDimFinancialTransactionType'

/*********************************************************************************************
MERGE the data from the view into the EDW DimStockEventType table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

IF OBJECT_ID('tempdb..#temp_DSTTRecordCount') IS NOT NULL DROP TABLE #temp_DSTTRecordCount 
CREATE TABLE #temp_DSTTRecordCount (ChangeType VARCHAR(10)) 

BEGIN TRANSACTION  

	MERGE [dbo].[DimFinancialTransactionType] AS DST
	USING [dbo].[v_DimFinancialTransactionType] AS SRC
		   ON SRC.Charge_Type_ID = DST.FinancialTransactionTypeID
		 
	WHEN MATCHED 
		 AND (Charge_ID <> TransactionID OR Charge_Description <> TransactionDescription
			 OR Charge_Category_ID <> TransactionCategoryID OR Charge_Category_Description <> TransactionCategoryDescription 
			 OR Charge_Class_ID <> TransactionClassID OR Charge_Class_Description <> TransactionClassDescription
			 OR Charge_Customer_ID <> TransactionCustomerID OR Charge_Customer_Description <> TransactionCustomerDescription
			 OR Expense_Entry_Ind <> IsTransactionCategoryExpenseEntry)
	THEN
	UPDATE SET 
		   TransactionID =  Charge_ID
		   , TransactionDescription = Charge_Description 
		   , TransactionCategoryID = Charge_Category_ID  
		   , TransactionCategoryDescription = Charge_Category_Description   
		   , TransactionClassID = Charge_Class_ID 
		   , TransactionClassDescription = Charge_Class_Description 
		   , TransactionCustomerID = Charge_Customer_ID
		   , TransactionCustomerDescription = Charge_Customer_Description 
		   , IsTransactionCategoryExpenseEntry = Expense_Entry_Ind  
		   , ETLLoadUpdateID = @ETLLoadID_sp
	     
	WHEN NOT MATCHED  THEN
	INSERT (FinancialTransactionTypeID
      ,[TransactionID]
      ,[TransactionDescription]
      ,[TransactionCategoryID]
      ,[TransactionCategoryDescription]
      ,[TransactionClassID]
      ,[TransactionClassDescription]
      ,[TransactionCustomerID]
      ,[TransactionCustomerDescription]
      ,[IsTransactionCategoryExpenseEntry]
      ,[ETLLoadID],[ETLLoadUpdateID])
	VALUES (Charge_Type_ID, Charge_ID, Charge_Description, Charge_Category_ID, Charge_Category_Description   
		   , Charge_Class_ID, Charge_Class_Description, Charge_Customer_ID, Charge_Customer_Description 
		   , Expense_Entry_Ind,@ETLLoadID_sp,@ETLLoadID_sp)  
	
	
	OUTPUT $action INTO #temp_DSTTRecordCount;
 

COMMIT TRAN

SELECT @InsertRowCount_sp = COUNT(*)  FROM #temp_DSTTRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount_sp = COUNT(*)  FROM #temp_DSTTRecordCount WHERE ChangeType = 'UPDATE'


EXEC [dbo].[usp_ETLLoadAudit]
	@StartEnd = 'E',
	@ETLLoadID = @ETLLoadID_sp,
	@InsertRowCount = @InsertRowCount_sp,
	@UpdateRowCount = @UpdateRowCount_sp
	
	IF OBJECT_ID('tempdb..#temp_DSTTRecordCount') IS NOT NULL DROP TABLE #temp_DSTTRecordCount
		
	RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID_sp,
		@InsertRowCount = @InsertRowCount_sp, 
		@UpdateRowCount = @UpdateRowCount_sp,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		
		IF OBJECT_ID('tempdb..#temp_DSTTRecordCount') IS NOT NULL DROP TABLE #temp_DSTTRecordCount
		
		RETURN -1
	

END CATCH

END

