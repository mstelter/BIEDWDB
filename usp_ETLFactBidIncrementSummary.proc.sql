/*
USE EDW
GO 
==============================================================================
Author		: Somesh Velupalli 
Create date	: 09/17/2011
Description	: Populate the EDW ETLFactBidIncrementSummary table from DimBidIncrementDetail tables.
==============================================================================
REVISION History
ChangeDate	     Developer	       Release/Problem Number
09/27/2012       Somesh V          Removed AuctionID, BranchNumber,StockID,ProviderID 
                                   and Added AuctionItemID
                                   
                                   

==============================================================================
Usage Example:
Exec usp_ETLFactBidIncrementSummary
==============================================================================
*/


CREATE PROCEDURE [dbo].[usp_ETLFactBidIncrementSummary]

AS

DECLARE  @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		,@CurrentDatetime Datetime
		,@CurrentDayID int 

SET NOCOUNT ON;
BEGIN TRY
 --Run the Audit Begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLFactBidIncrementSummary'

----Get the Min and Max LSN's
--SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'    

SET @CurrentDatetime = (SELECT  ER.MaxDatetime  FROM dbo.ETLRun ER WITH (NOLOCK) where ETLJobName = 'DailyETLJob')

SET @CurrentDayID = CONVERT(VARCHAR(8), @CurrentDateTime,112)



   /****************************************************************************************************************************************
	GET RUNNER UP BID INFORMATION
	****************************************************************************************************************************************/
	IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Information_Source') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Information_Source
	CREATE TABLE #TMP_Runner_Up_Bid_Information_Source
	(Auction_Item_ID	INT, 
	 Winning_Bid_Event_Datetime	DATETIME)
	INSERT INTO #TMP_Runner_Up_Bid_Information_Source
	(Auction_Item_ID, Winning_Bid_Event_Datetime)
	SELECT 
	AuctionItemID,
	MAX(BidEventDatetime) AS Winning_Bid_Event_Datetime
	FROM dbo.DimBidIncrementDetail AS BID WITH (NOLOCK)
	WHERE BID.BidEventDatetime >= DATEADD(D,-3,@CurrentDatetime)
	GROUP BY AuctionItemID

	IF OBJECT_ID('tempdb..#TMP_Winning_Bid_Detail') IS NOT NULL DROP TABLE #TMP_Winning_Bid_Detail
	CREATE TABLE #TMP_Winning_Bid_Detail
	(Auction_Item_ID	INT,
	 Winning_Bid_Event_Datetime	DATETIME,
	 Winning_Bid_Amount MONEY,
	 Winning_Bid_Sort_Order	INT)
	INSERT INTO #TMP_Winning_Bid_Detail
	(Auction_Item_ID,
	 Winning_Bid_Event_Datetime,
	 Winning_Bid_Amount, 
	 Winning_Bid_Sort_Order)
	SELECT BID.AuctionItemID 
	       ,MAX(BID.BidEventDatetime) AS Winning_Bid_Event_Datetime
		   ,MAX(BID.CurrentBidAmount) AS Winning_Bid_Amount, 
		    MAX(BID.SortOrder) AS Winning_Bid_Sort_Order
	FROM dbo.DimBidIncrementDetail AS BID WITH (NOLOCK)
		INNER JOIN #TMP_Runner_Up_Bid_Information_Source AS TRUBIS
			ON BID.AuctionItemID = TRUBIS.Auction_Item_ID
    WHERE BID.BidEventDatetime = Winning_Bid_Event_Datetime
	GROUP BY BID.AuctionItemID

	IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Datetime') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Datetime
	CREATE TABLE #TMP_Runner_Up_Bid_Datetime
	(Auction_Item_ID	    INT, 
	 Runner_Up_Datetime	    DATETIME,
	 Runner_Up_Bid_Amount	MONEY,
	 Runner_Up_Sort_Order	INT)
	INSERT INTO #TMP_Runner_Up_Bid_Datetime
	(Auction_Item_ID,
	 Runner_Up_Datetime,
	 Runner_Up_Bid_Amount,
	 Runner_Up_Sort_Order)
	SELECT BID.AuctionItemID, MAX(BID.BidEventDatetime) AS Runner_Up_Datetime
		, MAX(BID.CurrentBidAmount) AS Runner_Up_Bid_Amount, MAX(BID.SortOrder) AS Runner_Up_Sort_Order
	FROM dbo.DimBidIncrementDetail AS BID WITH (NOLOCK)
		INNER JOIN #TMP_Winning_Bid_Detail AS TWBD
			ON BID.AuctionItemID = TWBD.Auction_Item_ID
				AND BID.BidEventDatetime <= TWBD.Winning_Bid_Event_Datetime
				AND BID.CurrentBidAmount < TWBD.Winning_Bid_Amount
				AND BID.SortOrder < TWBD.Winning_Bid_Sort_Order
	GROUP BY BID.AuctionItemID
    
    
    IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Results') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Results
	CREATE TABLE #TMP_Runner_Up_Bid_Results(
			  [Bid_Increment_Detail_ID] [int]
			, [Auction_Item_ID] [int]
			, [Runner_Up_Bid_Amount] [money]
			, [Bid_Status_Code] [nchar] (1)
			, [Bid_Status_Description] [nvarchar] (100)
			, [Runner_Up_Bid_Type] [nchar] (1)
			, [Bid_Event_Datetime] [datetime]
			, [Bid_Event_Day_ID] [int]
			, [Sequence_Number] [int]
			, [Sort_Order] [int]
			, [Occurence_Count] [int]
			, [Bid_Increment_Amount] [money]
			, [New_Bid_Amount] [money]
		)
	INSERT INTO #TMP_Runner_Up_Bid_Results(Bid_Increment_Detail_ID, Auction_Item_ID,Runner_Up_Bid_Amount, Bid_Status_Code
		, Bid_Status_Description, Runner_Up_Bid_Type, Bid_Event_Datetime, Bid_Event_Day_ID, Sequence_Number, Sort_Order, Occurence_Count, Bid_Increment_Amount, New_Bid_Amount)
	SELECT BID.BidIncrementDetailID, BID.AuctionItemID,BID.CurrentBidAmount AS Runner_Up_Bid_Amount
		, BID.BidStatusCode, BID.BidStatusDescription, BID.BidderType AS Runner_Up_Bid_Type, BID.BidEventDatetime, BID.BidEventDayID, BID.SequenceNumber, BID.SortOrder
		, BID.OccurenceCount, BID.BidIncrementAmount, BID.NewBidAmount
	FROM dbo.DimBidIncrementDetail AS BID WITH (NOLOCK)
		INNER JOIN #TMP_Runner_Up_Bid_Datetime  AS TRUBD
			ON  BID.AuctionItemID = TRUBD.Auction_Item_ID
			AND BID.SortOrder = TRUBD.Runner_Up_Sort_Order



	BEGIN TRAN
	
    CREATE TABLE #temp_RecordCount (ChangeType VARCHAR(10))
	
	UPDATE BD
		SET RunnerUpBidInd = 1
	FROM dbo.DimBidDetail AS BD
		INNER JOIN #TMP_Runner_Up_Bid_Results AS TRUBR
			ON BD.AuctionItemID = TRUBR.Auction_Item_ID
				AND BD.SortOrder = TRUBR.Sort_Order
				AND BD.SequenceNumber = TRUBR.Sequence_Number
				AND BD.BidEventDatetime = TRUBR.Bid_Event_Datetime
	
		
	INSERT INTO dbo.FactBidIncrementSummary(AuctionItemID,BidIncrementAmount, TotalBidIncrementAmount, IncrementCount
		, RunnerUpBidAmount, RunnerUpBidType,ETLLoadID,ETLLoadUpdateID)
	SELECT BID.AuctionItemID, BID.BidIncrementAmount
		,SUM(BID.BidIncrementAmount) AS Total_Bid_Increment_Amount
		,COUNT(*) AS Increment_Count
		,Runner_Up_Bid.Runner_Up_Bid_Amount,Runner_Up_Bid.Runner_Up_Bid_Type
		,@ETLLoadID_sp,@ETLLoadID_sp
	FROM dbo.DimBidIncrementDetail BID WITH (NOLOCK)
		LEFT OUTER JOIN dbo.FactBidIncrementSummary AS BIS
			ON BID.AuctionItemID = BIS.AuctionItemID
		LEFT OUTER JOIN #TMP_Runner_Up_Bid_Results AS Runner_Up_Bid
			ON BID.AuctionItemID = Runner_Up_Bid.Auction_Item_ID
	WHERE BID.NewBidAmount IS NOT NULL
		AND BIS.BidIncrementSummaryID IS NULL
		AND BID.BidEventDatetime >= DATEADD(D,-3,@CurrentDatetime)
	GROUP BY BID.AuctionItemID, BID.BidIncrementAmount, Runner_Up_Bid.Runner_Up_Bid_Amount, Runner_Up_Bid.Runner_Up_Bid_Type
	ORDER BY BID.AuctionItemID
	

	
	SELECT @InsertRowCount_sp = COUNT(*)  FROM #temp_RecordCount WHERE ChangeType = 'INSERT'	
	
	
COMMIT TRAN 

EXEC [dbo].[usp_ETLLoadAudit]
	@StartEnd = 'E',
	@ETLLoadID = @ETLLoadID_sp,
	@ExtractRowCount = @ExtractRowCount_sp,
	@InsertRowCount = @InsertRowCount_sp,
	@UpdateRowCount = @UpdateRowCount_sp		
	
	
	IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Datetime') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Datetime
	IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Information_Source') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Information_Source
	IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Results') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Results
	IF OBJECT_ID('tempdb..#TMP_Winning_Bid_Detail') IS NOT NULL DROP TABLE #TMP_Winning_Bid_Detail
	IF OBJECT_ID('tempdb..#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount
	
	
	RETURN 0 

END TRY 

BEGIN CATCH 
    
    SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID_sp,
		@ExtractRowCount = @ExtractRowCount_sp,
		@InsertRowCount = @InsertRowCount_sp, 
		@UpdateRowCount = @UpdateRowCount_sp,
		@ErrorCode = @@ERROR
		
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);   
		
			IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Datetime') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Datetime
			IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Information_Source') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Information_Source
			IF OBJECT_ID('tempdb..#TMP_Runner_Up_Bid_Results') IS NOT NULL DROP TABLE #TMP_Runner_Up_Bid_Results
			IF OBJECT_ID('tempdb..#TMP_Winning_Bid_Detail') IS NOT NULL DROP TABLE #TMP_Winning_Bid_Detail
			IF OBJECT_ID('tempdb..#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount
		
		RETURN -1
	

END CATCH	
GO