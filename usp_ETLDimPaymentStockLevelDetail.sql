/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 10/24/2013
Description	: Populate the EDW DimPaymentStockLevelDetail table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	


==============================================================================
Usage Example:
Exec usp_ETLDimPaymentStockLevelDetail
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimPaymentStockLevelDetail]
AS     
BEGIN

DECLARE @ETLLoadID        INT,
		@ExtractRowCount  INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
SET NOCOUNT ON;

	BEGIN TRY
            
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimPaymentStockLevelDetail'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table to consolidate the Current and Static Storage Locations
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_DimPaymentLevelDetail') IS NOT NULL DROP TABLE #TMP_DimPaymentLevelDetail
CREATE TABLE #TMP_DimPaymentLevelDetail ([CashReportableTransactionId] INT NULL)

INSERT INTO #TMP_DimPaymentLevelDetail
SELECT [CashReportableTransactionId]
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_CashReportableCharges] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 

SELECT @ExtractRowCount = COUNT(*) FROM #TMP_DimPaymentLevelDetail

/*********************************************************************************************
1.  Create a temp table #DimPaymentLevelDetail and populate it with your SQL inner joined to temp table above
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_DimPaymentLevelStockDetail_WK') IS NOT NULL DROP TABLE #TMP_DimPaymentLevelStockDetail_WK
CREATE TABLE #TMP_DimPaymentLevelStockDetail_WK	(
	[ASAPCashReportableTransactionId] [int] NULL,
	[StockID] [int] NULL,
	[SalvageSaleDate] [datetime] NULL,
	[AdministratorBranchNumber] [int] NULL)

INSERT INTO #TMP_DimPaymentLevelStockDetail_WK	(
	[ASAPCashReportableTransactionId],
	[StockID],
	[SalvageSaleDate],
	[AdministratorBranchNumber])
SELECT DISTINCT crt.CashReportableTransactionId,
DS.StockID,
crc.SalvageSaleDate,
crc.AdministratorBranchNumber
FROM BI_ASAP_Rep.dbo.CashReportableCharges AS crc
INNER JOIN BI_ASAP_Rep.dbo.CashReportableTransaction AS crt 
	ON crc.CashReportableTransactionId = crt.CashReportableTransactionId
INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
	ON crc.SalvageID = DS.ASAPSalvageID
INNER JOIN #TMP_DimPaymentLevelDetail TDPLD  WITH (NOLOCK) 
	ON crt.CashReportableTransactionId = TDPLD.CashReportableTransactionId

/*********************************************************************************************
2.  MERGE the data from the temp table #DimPaymentLevelDetail into the EDW DimPaymentLevelDetail table
Replace all the fields below for the Update and Insert with the correct ones
This does Update and Insert only.  No Deletes
*********************************************************************************************/
--SELECT COUNT(*),[ASAPCashReportablePaymentID] FROM #TMP_DimPaymentLevelDetail_WK GROUP BY [ASAPCashReportablePaymentID] order by 1 desc
--SELECT COUNT(*) FROM #TMP_DimPaymentLevelDetail_WK
--SELECT * FROM #TMP_DimPaymentLevelDetail

IF OBJECT_ID('tempdb..#temp_DimPaymentLevelDetailRecordCount') IS NOT NULL DROP TABLE #temp_DimPaymentLevelDetailRecordCount
CREATE TABLE #temp_DimPaymentLevelDetailRecordCount(ChangeType VARCHAR(10)) 

BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.DimPaymentLevelStockDetail		AS pld
	USING #TMP_DimPaymentLevelStockDetail_WK AS tpld
		ON pld.ASAPCashReportableTransactionId=tpld.ASAPCashReportableTransactionId AND
			pld.StockID=tpld.StockID AND
			pld.SalvageSaleDate=tpld.SalvageSaleDate AND
			pld.AdministratorBranchNumber=tpld.AdministratorBranchNumber

WHEN MATCHED  THEN
      --Row exists and data is different
	UPDATE SET 
			ASAPCashReportableTransactionId=tpld.ASAPCashReportableTransactionId ,
			StockID=tpld.StockID ,
			SalvageSaleDate=tpld.SalvageSaleDate ,
			AdministratorBranchNumber=tpld.AdministratorBranchNumber	
	WHEN NOT MATCHED  THEN 
	--Row exists in source but not in target
	INSERT (ASAPCashReportableTransactionId,
			StockID,
			SalvageSaleDate,
			AdministratorBranchNumber)
	VALUES (tpld.ASAPCashReportableTransactionId ,
			tpld.StockID ,
			tpld.SalvageSaleDate ,
			tpld.AdministratorBranchNumber)
  
OUTPUT $action INTO #temp_DimPaymentLevelDetailRecordCount;
COMMIT TRAN

SELECT @InsertRowCount = COUNT(*)  FROM #temp_DimPaymentLevelDetailRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #temp_DimPaymentLevelDetailRecordCount WHERE ChangeType = 'UPDATE'

SET  @UpdateRowCount = @@ROWCOUNT 	
SET @InsertRowCount = NULL


EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@UpdateRowCount = @UpdateRowCount
		
		IF OBJECT_ID('tempdb..#TMP_DimPaymentLevelStockDetail_WK') IS NOT NULL DROP TABLE #TMP_DimPaymentLevelStockDetail_WK
		IF OBJECT_ID('tempdb..#temp_DimPaymentLevelDetailRecordCount') IS NOT NULL DROP TABLE #temp_DimPaymentLevelDetailRecordCount

	RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount, 
		@UpdateRowCount = @UpdateRowCount,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		
		IF OBJECT_ID('tempdb..#TMP_DimPaymentLevelStockDetail_WK') IS NOT NULL DROP TABLE #TMP_DimPaymentLevelStockDetail_WK
		IF OBJECT_ID('tempdb..#temp_DimPaymentLevelDetailRecordCount') IS NOT NULL DROP TABLE #temp_DimPaymentLevelDetailRecordCount

		RETURN -1
	

END CATCH

END