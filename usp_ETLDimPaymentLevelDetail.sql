/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 10/21/2013
Description	: Populate the EDW DimPaymentLevelDetail table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
10/22/2013	John Stires		Adjust field length to accomodate the source fields
10/24/2013	MAtt Stetler	Re-wrote according to reporting changes
12/4/2013	MAtt Stelter	Added Payment Method Code
==============================================================================
Usage Example:
Exec usp_ETLDimPaymentLevelDetail
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimPaymentLevelDetail]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimPaymentLevelDetail'

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
UNION
SELECT [CashReportableTransactionId]
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_CashReportablePayments] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 
UNION
SELECT [CashReportableTransactionId]
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_CashReportableTransaction] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 
UNION
SELECT [CashReportableTransactionId]
FROM BI_ASAP_REP.[dbo].[CashReportableTransaction] CRT WITH (NOLOCK)
INNER JOIN [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_CashReportableForm] (@StartLSN,@EndLSN,'all with merge') AS CRF
ON CRT.[CashReportableFormId] = CRF.CashReportableFormId
   WHERE [__$operation] = 5	 

SELECT @ExtractRowCount = COUNT(*) FROM #TMP_DimPaymentLevelDetail

/*********************************************************************************************
1.  Create a temp table #DimPaymentLevelDetail and populate it with your SQL inner joined to temp table above
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_DimPaymentLevelDetail_WK') IS NOT NULL DROP TABLE #TMP_DimPaymentLevelDetail_WK
CREATE TABLE #TMP_DimPaymentLevelDetail_WK	(
	[ASAPCashReportableFormId] [int] NULL,
	[ASAPCashReportableTransactionId] [int] NULL,
	[ASAPCashReportablePaymentID] [int] NULL,
	[ASAPSalvageBuyerPaymentID] [int] NULL,
	[PayerID] [int] NULL,
	[BuyerID] [int] NULL,
	[PayerName] [varchar](61) NULL,
	[DOCdescription] [varchar](50) NULL,
	[DOCNumber] [varchar](80) NULL,
	[PaymentDateTime] [datetime] NULL,
	[PaymentAmount] [money] NULL,
	[Entity] [varchar](100) NULL,
	[PayerType] [varchar](15) NULL,
	[TransactionReportableDate] [datetime] NULL,
	[ReportedDate] [datetime] NULL,
	[TransactionBranchNumber] [int] NULL,
	PaymentMethodCode VARCHAR(3) NULL,
	TotalPayments MONEY NULL)

INSERT INTO #TMP_DimPaymentLevelDetail_WK (
	[ASAPCashReportableFormId]
      ,[ASAPCashReportableTransactionId]
      ,[ASAPCashReportablePaymentID]
      ,[ASAPSalvageBuyerPaymentID]
	  ,[PayerType]
      ,[PayerID]
      ,[BuyerID]
      ,[DOCdescription]
      ,[DOCNumber]
      ,[PaymentDateTime]
      ,[PaymentAmount]
      ,[Entity]
      ,[TransactionReportableDate]
      ,[ReportedDate]
      ,[TransactionBranchNumber]
	  ,PaymentMethodCode
	  ,TotalPayments
	)
SELECT crf.CashReportableFormId
	,crt.CashReportableTransactionId 
	,crp.CashReportablePaymentId
	,crt.BuyerPaymentID
	,CASE 
		WHEN ISNULL(crt.EmployeeId, 0) = 0
			THEN 'Payer'
		ELSE 'Buyer Employee'
		END AS PayerType
	,CASE 
		WHEN ISNULL(crt.EmployeeId, 0) = 0
			THEN crt.PayerId
		ELSE crt.EmployeeId
		END AS PayerID
	,crt.BuyerId
	,CASE WHEN crp.PaymentMethodCode = 'cu' THEN 'Cash' ELSE crp.BankName END AS DOCdescription
	,CASE WHEN crp.PaymentMethodCode = 'cu' THEN CAST(crp.NumberOf100Bills AS VARCHAR(10)) ELSE crp.Reference END AS DOCNumber
	,crt.TransactionDateTime AS PaymentDateTime
	,crp.Amount AS PaymentAmount
	,crf.LegalEntityName AS Entity
	,crf.First_Payment_Date AS [TransactionReportableDate]
    ,CAST(SUBSTRING(crf.FormPAth, LEN(crf.FormPAth) - CHARINDEX('\', REVERSE(crf.FormPAth)) - 9, 10) AS DATETIME) AS ReportedDate
    ,crt.TransactionBranchNumber AS TransactionBranchNumber
	,crp.PaymentMethodCode AS PaymentMethodCode
	,crt.TotalPayments
FROM BI_ASAP_Rep.dbo.CashReportableForm AS crf WITH (NOLOCK)
INNER JOIN BI_ASAP_Rep.dbo.CashReportableTransaction AS crt  WITH (NOLOCK) 
	ON crf.CashReportableFormId = crt.CashReportableFormId
INNER JOIN BI_ASAP_Rep.dbo.CashReportablePayments AS crp  WITH (NOLOCK) 
	ON crt.CashReportableTransactionId = crp.CashReportableTransactionId
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
	MERGE dbo.DimPaymentLevelDetail		AS pld
	USING #TMP_DimPaymentLevelDetail_WK AS tpld
		ON pld.ASAPCashReportablePaymentID = tpld.ASAPCashReportablePaymentID

WHEN MATCHED  THEN
      --Row exists and data is different
	UPDATE SET 
		[ASAPCashReportableFormId] = tpld.[ASAPCashReportableFormId],
		[ASAPCashReportableTransactionId] = tpld.[ASAPCashReportableTransactionId],
		[ASAPCashReportablePaymentID] = tpld.[ASAPCashReportablePaymentID],
		[ASAPSalvageBuyerPaymentID] = tpld.[ASAPSalvageBuyerPaymentID],
		[PayerID] = tpld.[PayerID],
		[BuyerID] = tpld.[BuyerID],
		[PayerName] = tpld.[PayerName],
		[DOCdescription] = tpld.[DOCdescription],
		[DOCNumber] = tpld.[DOCNumber],
		[PaymentDateTime] = tpld.[PaymentDateTime],
		[PaymentAmount] = tpld.[PaymentAmount],
		[Entity] = tpld.[Entity],
		[PayerType] = tpld.[PayerType],
		[TransactionReportableDate] = tpld.[TransactionReportableDate],
		[ReportedDate] = tpld.[ReportedDate],
		[TransactionBranchNumber] = tpld.[TransactionBranchNumber],
		PaymentMethodCode = tpld.PaymentMethodCode,
		TotalPayments = tpld.TotalPayments
	WHEN NOT MATCHED  THEN 
	--Row exists in source but not in target
	INSERT ([ASAPCashReportableFormId]
      ,[ASAPCashReportableTransactionId]
      ,[ASAPCashReportablePaymentID]
      ,[ASAPSalvageBuyerPaymentID]
      ,[PayerID]
      ,[BuyerID]
      ,[PayerName]
      ,[DOCdescription]
      ,[DOCNumber]
      ,[PaymentDateTime]
      ,[PaymentAmount]
      ,[Entity]
      ,[PayerType]
      ,[TransactionReportableDate]
      ,[ReportedDate]
      ,[TransactionBranchNumber]
	  ,PaymentMethodCode
	  ,TotalPayments)
	VALUES (ISNULL([ASAPCashReportableFormId], 0)
				, ISNULL([ASAPCashReportableTransactionId], 0)
				, ISNULL([ASAPCashReportablePaymentID], 0)
				, ISNULL([ASAPSalvageBuyerPaymentID], 0)
				, ISNULL([PayerID], 0)
				, ISNULL([BuyerID], 0)
				, ISNULL([PayerName], '')
				, ISNULL([DOCdescription], '')
				, ISNULL([DOCNumber], '')
				, ISNULL([PaymentDateTime], '12/31/2999')
				, ISNULL([PaymentAmount], 0)
				, ISNULL([Entity], '')
				, ISNULL([PayerType], '')
				, ISNULL([TransactionReportableDate], '12/31/2999')
				, ISNULL([ReportedDate], '12/31/2999')
				, ISNULL([TransactionBranchNumber], 0)
				, ISNULL(PaymentMethodCode, '')
				, ISNULL(TotalPayments,0))
  
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
		
		IF OBJECT_ID('tempdb..#TMP_DimPaymentLevelDetail') IS NOT NULL DROP TABLE #TMP_DimPaymentLevelDetail
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
		
		IF OBJECT_ID('tempdb..#TMPDimPaymentLevelDetail') IS NOT NULL DROP TABLE #TMP_DimPaymentLevelDetail
		IF OBJECT_ID('tempdb..#temp_DimPaymentLevelDetailRecordCount') IS NOT NULL DROP TABLE #temp_DimPaymentLevelDetailRecordCount

		RETURN -1
	

END CATCH

END
