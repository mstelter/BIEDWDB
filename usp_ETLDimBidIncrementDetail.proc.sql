/*
USE EDW
GO 
=======================================================================================================================
Author		: Matt Stelter	 
Create date	: 09/17/2011
Description	: Populate the EDW DimBidIncrementDetail table 
========================================================================================================================
REVISION History
ChangeDate	     Developer	    	Release/Problem Number
09/27/2012       Somesh V           Added AuctionItemID and Removed AuctionID,StockID,ProviderID,BranchNumber




=======================================================================================================================
Usage Example:
Exec usp_ETLDimBidIncrementDetail
=======================================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimBidIncrementDetail]
AS
  DECLARE @ExtractRowCount_sp  INT,
          @ETLLoadID_sp        INT,
          @InsertRowCount_sp   INT,
          @UpdateRowCount_sp   INT,
          @CurrentDatetime     DATETIME


  SET NOCOUNT ON;

  BEGIN TRY
      --Run the Audit Begin
      EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimBidIncrementDetail'

      SET @CurrentDatetime = (SELECT ER.MaxDatetime FROM   dbo.ETLRun ER WITH (NOLOCK) WHERE  ETLJobName='DailyETLJob')

	SET NOCOUNT ON;

	CREATE TABLE #TMPBidIncrementFilter(
			  [AuctionItemID] [int]
			, [BuyerID]	[int]
			, [BuyerEmployeeID] [int]
		    , [BidLogItem] [int]
			, [BidAmount] [money]
			, [BidStatusCode] [nchar] (1)
			, [BidStatusDescription] [nvarchar] (100)
			, [BidderType] [nchar] (1)
			, [BidEventDatetime] [datetime]
			, [BidEventDayID] [int]
			, [SequenceNumber] [int]
			, [SortOrder] [int]
		)

	CREATE TABLE #TMPFinalBidOffsetResult(
			  [AuctionItemID] [int]
			, [BidAmount] [money]
			, [BidStatusCode] [nchar] (1)
			, [BidStatusDescription] [nvarchar] (100)
			, [BidderType] [nchar] (1)
			, [BidEventDatetime] [datetime]
			, [SequenceNumber] [int]
			, [SortOrder] [int]
			, [OccurenceCount] [int]
		)

	CREATE TABLE #TMPFinalBidOffset(
			  [AuctionItemID] [int]
			, [BuyerID] [int]
			, [BuyerEmployeeID] [int]
			, [BidAmount] [money]
			, [PreviousBidAmount] [money]
			, [BidStatusCode] [nchar] (1)
			, [BidStatusDescription] [nvarchar] (100)
			, [BidderType] [nchar] (1)
			, [PreviousBidderType] [nchar] (1)
			, [BidEventDatetime] [datetime]
			, [SequenceNumber] [int]
			, [SortOrder] [int]
			, [OffsetBidAmount] [money]
			, [OffsetBidStatusCode] [nchar] (1)
			, [OffsetPreviousBidStatusCode] [nchar] (1)
			, [OffsetBidStatusDescription] [nvarchar] (100)
			, [OffsetBidderType] [nchar] (1)
			, [OffsetBidEventDatetime] [datetime]
			, [OffsetSequenceNumber] [int]
			, [OffsetSortOrder] [int]
		)

	CREATE TABLE #TMPOverrideUndoBidStatus(
			  [AuctionItemID] [int]
			, [BuyerID] [int]
			, [BuyerEmployeeID] [int]
			, [BidAmount] [money]
			, [BidStatusCode] [nchar] (1)
			, [PreviousBidStatusCode] [nchar] (1)
			, [BidStatusDescription] [nvarchar] (100)
			, [BidderType] [nchar] (1)
			, [PreviousBidderType] [nchar] (1)
			, [BidEventDatetime] [datetime]
			, [SequenceNumber] [int]
			, [SortOrder] [int]
		)

	CREATE TABLE #TMPReceivedBidStatus(
			  [AuctionItemID] [int]
			, [BuyerID] [int]
			, [BuyerEmployeeID] [int]
			, [BidAmount] [money]
			, [PreviousBidAmount] [money]
			, [BidStatusCode] [nchar] (1)
			, [BidStatusDescription] [nvarchar] (100)
			, [BidderType] [nchar] (1)
			, [PreviousBidderType] [nchar] (1)
			, [BidEventDatetime] [datetime]
			, [PreviousBidEventDatetime] [datetime]
			, [SequenceNumber] [int]
			, [PreviousSequenceNumber] [int]
			, [SortOrder] [int]
			, [PreviousSortOrder] [int]
		)
		
      CREATE TABLE #temp_RecordCount
        (ChangeType VARCHAR(10))
	/****************************************************************************************************************************************************************

	BUILD LIST OF FILTERED BID INCREMENTS FOR ONLY 'K', 'O', 'U' STATUS

	****************************************************************************************************************************************************************/

	INSERT INTO #TMPBidIncrementFilter(AuctionItemID, BuyerID, BuyerEmployeeID, BidLogItem, BidAmount, BidStatusCode, BidStatusDescription
			, BidderType, BidEventDatetime, BidEventDayID, SequenceNumber, SortOrder)
	SELECT 
	  BDF.AuctionItemID
	, BDF.BuyerID
	, BDF.BuyerEmployeeID
	, BDF.BidLogItem
	, BDF.BidAmount
	, BDF.BidStatusCode
	, BDF.BidStatusDescription
	, BDF.BidderType
	, BDF.BidEventDatetime
	, BDF.BidEventDayID
	, BDF.SequenceNumber
	, BDF.SortOrder
	FROM dbo.v_DimBidDetailFiltered AS BDF WITH (NOLOCK)
	WHERE BDF.BidStatusCode IN ('K','U','O')
		 AND BDF.BidEventDatetime >= DATEADD(D,-3,@CurrentDatetime)
	ORDER BY BDF.AuctionItemID, BDF.BidEventDatetime

	CREATE INDEX TMPIDXBidIncrement ON #TMPBidIncrementFilter(AuctionItemID,BidStatusCode,SortOrder)

	INSERT INTO #TMPReceivedBidStatus(AuctionItemID, BuyerID, BuyerEmployeeID, BidAmount, PreviousBidAmount, BidStatusCode, BidStatusDescription, BidderType
			, PreviousBidderType, BidEventDatetime, PreviousBidEventDatetime, SequenceNumber, PreviousSequenceNumber, SortOrder, PreviousSortOrder)
	SELECT TBIS.AuctionItemID
		, TBIS.BuyerID
		, TBIS.BuyerEmployeeID
		, TBIS.BidAmount
		, TBISPreviousRecord.BidAmount AS PreviousBidAmount
		, TBIS.BidStatusCode
		, TBIS.BidStatusDescription
		, TBIS.BidderType
		, TBISPreviousRecord.BidderType AS PreviousBidderType
		, TBIS.BidEventDatetime
		, TBISPreviousRecord.BidEventDatetime AS PreviousBidEventDatetime
		, TBIS.SequenceNumber
		, TBISPreviousRecord.SequenceNumber AS PreviousSequenceNumber
		, TBIS.SortOrder
		, TBISPreviousRecord.SortOrder AS PreviousSortOrder
	FROM #TMPBidIncrementFilter AS TBIS
		LEFT OUTER JOIN #TMPBidIncrementFilter AS TBISPreviousRecord
			ON TBIS.AuctionItemID = TBISPreviousRecord.AuctionItemID
				AND TBISPreviousRecord.SortOrder = (
						SELECT MAX(SortOrder)
						FROM #TMPBidIncrementFilter
						WHERE SortOrder < TBIS.SortOrder
							AND AuctionItemID = TBIS.AuctionItemID
														AND BidStatusCode = 'K'
					)
	WHERE TBIS.BidStatusCode = 'K'
		 AND TBIS.BidEventDatetime >= DATEADD(D,-3,@CurrentDatetime)
	ORDER BY TBIS.AuctionItemID, TBIS.SortOrder


	INSERT INTO #TMPOverrideUndoBidStatus(AuctionItemID,BuyerID, BuyerEmployeeID,  BidAmount, BidStatusCode, PreviousBidStatusCode, BidStatusDescription
			, BidderType, PreviousBidderType, BidEventDatetime, SequenceNumber, SortOrder)
	SELECT TBIS.AuctionItemID
		, TBIS.BuyerID
		, TBIS.BuyerEmployeeID
		, TBIS.BidAmount
		, TBIS.BidStatusCode
		, TBISPreviousRecord.BidStatusCode AS PreviousBidStatusCode
		, TBIS.BidStatusDescription
		, TBIS.BidderType
		, TBISPreviousRecord.BidderType AS PreviousBidderType
		, TBIS.BidEventDatetime
		, TBIS.SequenceNumber
		, TBIS.SortOrder
	FROM #TMPBidIncrementFilter AS TBIS
		LEFT OUTER JOIN #TMPBidIncrementFilter AS TBISPreviousRecord
			ON TBIS.AuctionItemID = TBISPreviousRecord.AuctionItemID
			  AND TBISPreviousRecord.SortOrder = (
						SELECT MAX(SortOrder)
						FROM #TMPBidIncrementFilter
						WHERE SortOrder < TBIS.SortOrder
							AND AuctionItemID = TBIS.AuctionItemID
							AND BidStatusCode IN ('O','U')
					)
	WHERE TBIS.BidStatusCode IN ('O','U')
	ORDER BY TBIS.AuctionItemID, TBIS.SortOrder


	INSERT INTO #TMPFinalBidOffset(AuctionItemID, BuyerID, BuyerEmployeeID
		, BidAmount, PreviousBidAmount, BidStatusCode, BidStatusDescription, BidderType, PreviousBidderType
		, BidEventDatetime, SequenceNumber, SortOrder, OffsetBidAmount, OffsetBidStatusCode, OffsetPreviousBidStatusCode, OffsetBidStatusDescription, OffsetBidderType
		, OffsetBidEventDatetime, OffsetSequenceNumber, OffsetSortOrder)
	SELECT TRBS.AuctionItemID
		, TRBS.BuyerID
		, TRBS.BuyerEmployeeID
		, TRBS.BidAmount
		, TRBS.PreviousBidAmount
		, TRBS.BidStatusCode
		, TRBS.BidStatusDescription
		, TRBS.BidderType
		, TRBS.PreviousBidderType
		, TRBS.BidEventDatetime
		, TRBS.SequenceNumber
		, TRBS.SortOrder
		, TOUBS.BidAmount AS OffsetBidAmount
		, TOUBS.BidStatusCode AS OffsetBidStatusCode
		, TOUBS.PreviousBidStatusCode AS OffsetPreviousBidStatusCode
		, TOUBS.BidStatusDescription AS OffsetBidStatusDescription
		, TOUBS.BidderType AS OffsetBidderType
		, TOUBS.BidEventDatetime AS OffsetBidEventDatetime
		, TOUBS.SequenceNumber AS OffsetSequenceNumber
		, TOUBS.SortOrder AS OffsetSortOrder
	FROM #TMPReceivedBidStatus AS TRBS
		LEFT OUTER JOIN #TMPOverrideUndoBidStatus AS TOUBS
			ON TRBS.AuctionItemID = TOUBS.AuctionItemID
				AND TOUBS.SortOrder = 
					(
						SELECT MIN(SortOrder)
						FROM #TMPOverrideUndoBidStatus
						WHERE AuctionItemID = TRBS.AuctionItemID
							AND SortOrder > TRBS.SortOrder
							AND BidAmount BETWEEN TRBS.PreviousBidAmount AND TRBS.BidAmount
					)
				AND (CASE WHEN TRBS.PreviousBidderType = 'I'
						AND TOUBS.PreviousBidStatusCode = 'O'
					THEN 0
					ELSE 1
					END) = 1


	INSERT INTO #TMPFinalBidOffsetResult(AuctionItemID,
			 BidAmount, BidStatusCode, BidStatusDescription, BidderType
			, BidEventDatetime, SequenceNumber, SortOrder, OccurenceCount)
	SELECT AuctionItemID, BidAmount, BidStatusCode, BidStatusDescription, BidderType
			, MAX(BidEventDatetime) AS BidEventDatetime, MAX(SequenceNumber) AS SequenceNumber, MAX(SortOrder) AS SortOrder
			, Count(*) AS OccurenceCount
	FROM #TMPFinalBidOffset
	WHERE OffsetSortOrder IS NULL
	GROUP BY AuctionItemID,BidAmount, BidStatusCode, BidStatusDescription, BidderType

BEGIN TRAN
	INSERT INTO dbo.DimBidIncrementDetail(AuctionItemID, CurrentBidAmount, BidStatusCode, BidStatusDescription, BidderType
		, BidEventDatetime, BidEventDayID, SequenceNumber, SortOrder, OccurenceCount, BidIncrementAmount, NewBidAmount)
	SELECT TFBOR.AuctionItemID
		, TFBOR.BidAmount AS CurrentBidAmount
		, TFBOR.BidStatusCode
		, TFBOR.BidStatusDescription
		, TFBOR.BidderType
		, TFBOR.BidEventDatetime
		, CONVERT(CHAR(8),TFBOR.BidEventDatetime,112) AS BidEventDayID
		, TFBOR.SequenceNumber
		, TFBOR.SortOrder
		, TFBOR.OccurenceCount
	, CASE WHEN TFBORIncrement.BidAmount IS NOT NULL
		THEN TFBORIncrement.BidAmount - TFBOR.BidAmount
		ELSE 0
		END AS BidIncrementAmount
	, TFBORIncrement.BidAmount AS NewBidAmount
	FROM #TMPFinalBidOffsetResult AS TFBOR
		LEFT OUTER JOIN #TMPFinalBidOffsetResult AS TFBORIncrement
			ON TFBOR.AuctionItemID = TFBORIncrement.AuctionItemID
				AND TFBORIncrement.SortOrder =
					(
						SELECT MIN(SortOrder)
						FROM #TMPFinalBidOffsetResult
						WHERE AuctionItemID = TFBOR.AuctionItemID
						AND SortOrder > TFBOR.SortOrder
					)
		LEFT OUTER JOIN dbo.DimBidIncrementDetail AS BIDTEMP
			ON TFBOR.AuctionItemID = BIDTEMP.AuctionItemID
				AND TFBOR.BidEventDatetime = BIDTEMP.BidEventDatetime
				AND TFBOR.SortOrder = BIDTEMP.SortOrder
	WHERE BIDTEMP.BidIncrementDetailID IS NULL
	ORDER BY TFBOR.AuctionItemID,
		 TFBOR.BidEventDatetime

      SELECT @InsertRowCount_sp=Count(*)
      FROM   #temp_RecordCount
      WHERE  ChangeType='INSERT'

      COMMIT TRAN

	IF OBJECT_ID('tempdb..#TMPFinalBidOffsetResult') IS NOT NULL DROP TABLE #TMPFinalBidOffsetResult
	IF OBJECT_ID('tempdb..#TMPFinalBidOffset') IS NOT NULL DROP TABLE #TMPFinalBidOffset
	IF OBJECT_ID('tempdb..#TMPOverrideUndoBidStatus') IS NOT NULL DROP TABLE #TMPOverrideUndoBidStatus
	IF OBJECT_ID('tempdb..#TMPReceivedBidStatus') IS NOT NULL DROP TABLE #TMPReceivedBidStatus
	IF OBJECT_ID('tempdb..#TMPBidIncrementFilter') IS NOT NULL DROP TABLE #TMPBidIncrementFilter

      RETURN 0
  END TRY

  BEGIN CATCH
      SET NOCOUNT OFF

      IF @@TRANCOUNT>0
        ROLLBACK TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp,@ErrorCode = @@ERROR

      DECLARE @ErrorMessage  NVARCHAR(4000),
              @ErrorSeverity INT,
              @ErrorState    INT;

      SELECT @ErrorMessage=Error_message(),
             @ErrorSeverity=Error_severity(),
             @ErrorState=Error_state();

      RAISERROR (@ErrorMessage,
                 @ErrorSeverity,
                 @ErrorState);

	IF OBJECT_ID('tempdb..#TMPFinalBidOffsetResult') IS NOT NULL DROP TABLE #TMPFinalBidOffsetResult
	IF OBJECT_ID('tempdb..#TMPFinalBidOffset') IS NOT NULL DROP TABLE #TMPFinalBidOffset
	IF OBJECT_ID('tempdb..#TMPOverrideUndoBidStatus') IS NOT NULL DROP TABLE #TMPOverrideUndoBidStatus
	IF OBJECT_ID('tempdb..#TMPReceivedBidStatus') IS NOT NULL DROP TABLE #TMPReceivedBidStatus
	IF OBJECT_ID('tempdb..#TMPBidIncrementFilter') IS NOT NULL DROP TABLE #TMPBidIncrementFilter


      RETURN -1
  END CATCH
GO


