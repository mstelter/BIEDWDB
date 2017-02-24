/*
USE EDW
GO 
============================================================================================================
Author		: Somesh Velupalli 
Create date	: 09/17/2011
Description	: Populate the EDW DimBidBetail table.
============================================================================================================
REVISION History
ChangeDate	     Developer	    	Release/Problem Number
9/25/2012		Matt Stelter		Added Auction Lane
9/26/2012       Somesh V            Removed AuctionID,AuctionLane,StockID,ProviderID,BranchNumber
                                    and Added Auction_tem_ID
10/30/2012		Matt Stelter		Runner UP, Display Ind
11/7/2012		MAtt Stelter		Commented out UpdateDatetime filter
11/20/2012		Matt Stelter		Added update for DisplayOrder
2/8/2013		Matt Stelter		Added AuctionLane for join
09/25/2013		John Stires			Added a check for  AuctionItemID to the INSERT into DIMBidDetail
										to prevent inserting duplicates
4/17/2014		Matt Stelter		Added new Status Codes
5/15/2014		Matt Stelter		Added Live Rep Update
9/10/2014		Matt Stelter		Update the status to Accepted Counter Offer if the counter offer was accepted.	
10/21/2015		Matt Stelter		Changed logic for Buyer and Buyer Employee IDs
6/8/2014		Matt Stelter		Added IPAddress
============================================================================================================
Usage Example:
Exec usp_ETLDimBidDetail
=============================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimBidDetail]
AS
  DECLARE @ExtractRowCount_sp INT,
          @ETLLoadID_sp       INT,
          @InsertRowCount_sp  INT,
          @UpdateRowCount_sp  INT,
          @CurrentDatetime    DATETIME

  SET NOCOUNT ON;

  BEGIN TRY
      --Run the Audit Begin
      EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimBidDetail'

      SET @CurrentDatetime = (SELECT ER.MaxDatetime
                              FROM   dbo.ETLRun ER WITH (NOLOCK)
                              WHERE  ETLJobName='DailyETLJob')

      --Create TEMP Tables
      CREATE TABLE #TMP_Bidder_List
        (TMP_Bidder_List_ID     INT IDENTITY(1, 1) NOT FOR REPLICATION NOT NULL,
         Buyer_Employee_ID      INT,
         Buyer_ID               INT,
         Bidder_Display_Name    VARCHAR(100),
         ASAP_Buyer_Employee_ID INT,
         Job_Title_Code         VARCHAR(4))

      CREATE TABLE #TMP_Auction_Schedule_Source
        ([Auction_Item_ID]              [INT],
         [Stock_ID]                     [INT],
         [Auction_Item_Sequence_Number] [VARCHAR] (80),
         [Branch_Number]                [INT],
         [Auction_Date_Time]            [DATETIME],
         [Auction_Day_ID]               [CHAR] (8),
		 [AuctionLane]					[varchar](1))

      CREATE TABLE #TMP_Bid_Detail_Source
        ([Bid_Log_Item]           [INT],
         [Ask_Amount]             [MONEY],
         [Auction_Item_ID]        [INT],
         [Bid_Amount]             [MONEY],
         [Bid_Event_Datetime]     [DATETIME],
         [Bid_Event_Day_ID]       [INT],
         [Bid_Status_Code]        [NCHAR] (1),
         [Bid_Status_Description] [NVARCHAR] (100),
         [Bid_Type]               [NCHAR] (1),
         [Bidder_Type]            [NCHAR] (1),
         [Branch_Number]          [INT],
         [Buyer_ID]               [INT],
         [Provider_ID]            [INT],
         [Stock_ID]               [INT],
         [Displayname]            [NVARCHAR] (50),
         [Sequence_Number]        [INT],
         [SSM_Number]             [INT],
         [Device_Type]            NVARCHAR(50) NULL)

      CREATE TABLE #TMP_Display_Location_Patch
        (Display_Name      [NVARCHAR] (50),
         Display_Location  [NVARCHAR] (50),
         Buyer_ID          INT,
         Buyer_Employee_ID INT)

      CREATE TABLE #TMP_Floor_Location_Patch
        (Display_Location [NVARCHAR] (50),
         Buyer_ID         INT)

      CREATE TABLE #TMP_Bid_Detail
        ([Bid_Log_Item]           [INT],
         [Ask_Amount]             [MONEY],
         [Auction_Item_ID]        [INT],
         [Bid_Amount]             [MONEY],
         [Bid_Event_Datetime]     [DATETIME],
         [Bid_Event_Day_ID]       [INT],
         [Bid_Status_Code]        [NCHAR] (1),
         [Bid_Status_Description] [NVARCHAR] (100),
         [Buyer_employee_ID]      [INT],
         [Bid_Type]               [NCHAR] (1),
         [Bidder_Type]            [NCHAR] (1),
         [Branch_Number]          [INT],
         [Buyer_ID]               [INT],
         [Provider_ID]            [INT],
         [Stock_ID]               [INT],
         [Displayname]            [NVARCHAR] (50),
         [Displaylocation]        [NVARCHAR] (50),
         [Sequence_Number]        [INT],
         [SSM_Number]             [INT],
         [Sort_Order]             [INT],
         [Group_Number]           [INT],
         [Device_Type]            NVARCHAR(50) NULL,
         IsBidDetailShow		[TinyInt] NULL)

UPDATE EDW.dbo.BidLog SET BranchNumber = 116 where BranchNumber = 115 and TimeStamp > GETDATE()-3
UPDATE EDW.dbo.BidLog SET BranchNumber = 761 where BranchNumber = 717 and TimeStamp > GETDATE()-3

      CREATE TABLE #temp_RecordCount
        (ChangeType VARCHAR(10))

      --Insert Data into #TMP_tables
      INSERT INTO #TMP_Bidder_List
                  (Buyer_Employee_ID,Buyer_ID,Bidder_Display_Name)
      SELECT BuyerEmployeeID,
             BuyerID,
             BidderDisplayName
      FROM   dbo.DimBuyerEmployee AS B WITH (NOLOCK)
      WHERE  CurrentStatusCode='ACT'

      INSERT INTO #TMP_Auction_Schedule_Source
                  (Auction_Item_ID,Stock_ID,Auction_Item_Sequence_Number,Branch_Number,Auction_Date_Time,Auction_Day_ID,AuctionLane)
      SELECT DAI.AuctionItemID,
             DS.StockID,
             DAI.AuctionItemSequenceNumber,
             DB.BranchNumber,
             DA.AuctionDateTime,
             DA.AuctionDayID,
			 DAI.AuctionLane
      FROM   dbo.DimAuction DA WITH (NOLOCK)
             INNER JOIN dbo.DimBranch DB WITH (NOLOCK)
               ON DA.BranchNumber=DB.BranchNumber
             INNER JOIN dbo.DimAuctionItem DAI WITH (NOLOCK)
               ON DA.AuctionID=DAI.AuctionID
             INNER JOIN dbo.DimStock DS WITH (NOLOCK)
               ON DAI.StockID=DS.StockID
      WHERE  DA.AuctionDateTime>Dateadd(D, -3, @CurrentDatetime)

      INSERT INTO #TMP_Bid_Detail_Source
                  (Bid_Log_Item,Ask_Amount,Auction_Item_ID,Bid_Amount,Bid_Event_Datetime,Bid_Event_Day_ID,Bid_Status_Code,Bid_Status_Description,
                   Bid_Type,Bidder_Type,
                   Branch_Number,Buyer_ID,Stock_ID,Displayname,Sequence_Number,SSM_Number,Device_Type)
      SELECT BL.Item AS Bid_Log_Item,
             BL.Ask AS Ask_Amount,
             TASS.Auction_Item_ID,
             BL.Amount AS Bid_Amount,
             Dateadd(hh, -1, BL.TimeStamp) AS BID_Event_Datetime,
             CONVERT(CHAR(8), Dateadd(hh, -1, BL.TimeStamp), 112) AS Bid_Event_Day_ID,
             BL.Code AS Bid_Status_Code,
             BL.Action AS Bid_Status_Description,
             BL.BidderType AS Bid_Type,
             CASE BL.BidderType
               WHEN 'F' THEN 'F'
               WHEN 'I' THEN 'I'
               WHEN 'R' THEN 'I'
               WHEN 'P' THEN 'P'
             END AS Bidder_Type,
             TASS.Branch_Number,
             0 AS Buyer_ID, --STK.CurrentBuyerID,
             STK.StockID,
             BL.Name AS DisplayName,
             BL.SeqNumber AS Sequence_Number,
             BL.SSM AS SSM_Number,
             BL.DeviceType
      FROM   dbo.BidLog AS BL WITH (NOLOCK)
             INNER JOIN #TMP_Auction_Schedule_Source AS TASS
               ON CONVERT(CHAR(8), BL.TimeStamp, 112)=TASS.Auction_Day_ID
                  AND BL.BranchNumber=TASS.Branch_Number
                  AND BL.Item=TASS.Auction_Item_Sequence_Number
				  AND BL.AuctionLane = TASS.AuctionLane
             INNER JOIN dbo.DimStock AS STK WITH (NOLOCK)
               ON TASS.Stock_ID=STK.StockId
      WHERE  Isnumeric(Isnull(BL.Item, 0))=1
         --AND BL.UpdateDatetime>=Dateadd(D, -3, @CurrentDatetime)

      INSERT INTO #TMP_Display_Location_Patch
                  (Display_Name,Display_Location,Buyer_ID,Buyer_Employee_ID)
      SELECT BE.BidderDisplayName AS DisplayName,
             Isnull(B.CityName, '')+'-'+Isnull(B.StateAbbreviation, 'XX') AS DisplayLocation,
             B.BuyerID,
             BE.BuyerEmployeeID
      FROM   dbo.DimBuyer AS B WITH (NOLOCK)
             INNER JOIN dbo.DimBuyerEmployee AS BE WITH (NOLOCK)
               ON B.BuyerID=BE.BuyerID

      INSERT INTO #TMP_Floor_Location_Patch
                  (Display_Location,Buyer_ID)
      SELECT DISTINCT Isnull(B.CityName, '')+'-'+Isnull(B.StateAbbreviation, 'XX') AS DisplayLocation,
                      B.BuyerID
      FROM   dbo.DimBuyer AS B WITH (NOLOCK)

--New Buyer/Bidder Update Logic

--This UPDATES all Non Matching Buyers
UPDATE #TMP_Bid_Detail_Source	
SET Buyer_ID = LEFT(Displayname,(CHARINDEX ( '_' ,Displayname)-1))
WHERE CHARINDEX ( '_' ,Displayname)>0

--This UPDATES all Non Matching Buyers where the DisplayName is not a Buyer_Bidder
UPDATE #TMP_Bid_Detail_Source	
SET Buyer_ID=Displayname
WHERE CHARINDEX ( '_' ,Displayname)=0
AND ISNUMERIC(Displayname)=1 AND LEN(Displayname) < 10 

--Update Final Bid for Floor Buyers
UPDATE DBD SET dbd.buyer_ID = F.Buyer_ID  
--SELECT dbd.buyerID, F.BuyerID,* 
from #TMP_Bid_Detail_Source dbd 
INNER JOIN (select dbd.Auction_Item_ID, DBD.Bid_Amount,DBD.Bid_Event_Day_ID,DBD.Buyer_ID
			from #TMP_Bid_Detail_Source dbd
			where Bid_Status_Code IN ('B','C') and Bidder_Type = 'F') F
	ON dbd.Auction_Item_ID = F.Auction_Item_ID AND DBD.Bid_Amount = F.Bid_Amount and DBD.Bid_Event_Day_ID = F.Bid_Event_Day_ID
INNER JOIN (select dbd.Auction_Item_ID,DBD.Bid_Event_Day_ID,DBD.Sequence_Number AS SortOrder
			from #TMP_Bid_Detail_Source dbd
			inner join edw.dbo.dimauctionitem dai
				on dbd.Auction_Item_ID = dai.AuctionItemID
			INNER JOIN (select dbd.AuctionItemID, DBD.BidAmount,DBD.BidEventDayID,DBD.BuyerID
						from edw.dbo.dimbiddetail dbd
						where BidStatusCode IN ('B','C') and BidderType = 'F') F
				ON dbd.Auction_Item_ID = F.AuctionItemID AND DBD.Bid_Amount = F.BidAmount and DBD.Bid_Event_Day_ID = F.BidEventDayID
			where DBD.Bid_Status_Code IN ('K') and DBD.Bidder_Type = 'F') SO
	ON dbd.Auction_Item_ID = SO.Auction_Item_ID and DBD.Bid_Event_Day_ID = SO.Bid_Event_Day_ID AND DBD.Sequence_Number = SO.SortOrder


      INSERT INTO #TMP_Bid_Detail
                  (Bid_Log_Item,Ask_Amount,Auction_Item_ID,Bid_Amount,Bid_Event_Datetime,Bid_Event_Day_ID,Bid_Status_Code,Bid_Status_Description,
                   Buyer_employee_ID,
                   Bid_Type,Bidder_Type,Branch_Number,Buyer_ID,Stock_ID,Displayname,Displaylocation,Sequence_Number,SSM_Number,Sort_Order,Group_Number
                   ,
                   Device_Type,IsBidDetailShow)
      SELECT BDS.Bid_Log_Item,
             BDS.Ask_Amount,
             BDS.Auction_Item_ID,
             BDS.Bid_Amount,
             BDS.BID_Event_Datetime,
             BDS.Bid_Event_Day_ID,
             BDS.Bid_Status_Code,
             BDS.Bid_Status_Description,
             0 AS Buyer_Employee_ID, --Bidder.Buyer_Employee_ID,
             BDS.Bid_Type,
             BDS.Bidder_Type,
             BDS.Branch_Number,
             BDS.Buyer_ID, --Isnull(Bidder.Buyer_ID, BDS.Buyer_ID) AS Buyer_ID,
             BDS.Stock_ID,
             BDS.DisplayName
             /***************************************************************************************************************************************/
             ,
             CASE
               WHEN BDS.DisplayName='OnSite' THEN 'OnSite'
               WHEN BDS.Bid_Status_Code='^' THEN NULL
               ELSE COALESCE(AD.DisplayLocation, TDLP.Display_Location, TFLP.Display_Location, 'Not Available')
             END AS Displaylocation
             /***************************************************************************************************************************************/
             ,
             BDS.Sequence_Number,
             BDS.SSM_Number,
             Row_number() OVER ( PARTITION BY BDS.Auction_ITEM_ID, BDS.Bid_Event_Day_ID, BDS.Stock_ID ORDER BY BDS.Auction_ITEM_ID, BDS.Stock_ID,
             BDS.BID_Event_Datetime, BDS.Sequence_Number) AS Sort_Order,
             Row_number() OVER ( PARTITION BY BDS.Auction_ITEM_ID, BDS.Bid_Event_Day_ID, BDS.Stock_ID, BDS.Bid_Amount, BDS.Bid_Status_Code,
             BDS.Bid_Type,
             BDS.DisplayName ORDER BY BDS.Auction_ITEM_ID, BDS.Stock_ID, BDS.Bid_Event_Day_ID, BDS.BID_Event_Datetime, BDS.Sequence_Number) AS
             Group_Number,
             BDS.[Device_Type],
             CASE WHEN BDS.Bid_Status_Code IN ('K', 'U', 'A') OR BDS.Bid_Status_Code IS NULL THEN 0 ELSE 1 END AS IsBidDetailShow
      FROM   #TMP_Bid_Detail_Source AS BDS WITH (NOLOCK)
			INNER JOIN dbo.DimAuctionItem DAI WITH (NOLOCK)
				ON BDS.Auction_Item_ID = DAI.AuctionItemID
             LEFT OUTER JOIN #TMP_Bidder_List AS Bidder WITH (NOLOCK)
               ON Bidder.Bidder_Display_Name=BDS.DisplayName
             LEFT OUTER JOIN (SELECT Distinct DisplayLocation,BuyerEmployeeID,LoginDatetime,LogoutDatetime,BranchNumber,AuctionID,AuctionLane FROM dbo.DimAttendeeDetail WITH (NOLOCK)) AD
               ON Bidder.Buyer_Employee_ID=AD.BuyerEmployeeID
                  AND BDS.Bid_Event_Datetime BETWEEN AD.LoginDatetime AND AD.LogoutDatetime
                  AND BDS.Branch_Number=AD.BranchNumber
                  AND DAI.AuctionID = AD.AuctionID
                  AND DAI.AuctionLane = AD.AuctionLane
             LEFT OUTER JOIN #TMP_Display_Location_Patch AS TDLP WITH (NOLOCK)
               ON Cast(BDS.DisplayName AS VARCHAR(50))=TDLP.Display_Name
             LEFT OUTER JOIN #TMP_Floor_Location_Patch AS TFLP WITH (NOLOCK)
               ON Cast(BDS.DisplayName AS VARCHAR(50))=Cast(TFLP.Buyer_ID AS VARCHAR(50))
             LEFT OUTER JOIN dbo.DimBidDetail AS BD WITH (NOLOCK)
               ON BDS.Auction_Item_ID=BD.AuctionItemID
                  AND BDS.BID_Event_Datetime=BD.BidEventDatetime
                  AND Isnull(BDS.Bid_Status_Code, '')=Isnull(BD.BidStatusCode, '')
                  AND Isnull(BDS.Bid_Type, 0)=Isnull(BD.BidType, 0)
      WHERE  BD.BidDetailID IS NULL

--This UPDATES all Bidders
UPDATE #TMP_Bid_Detail
SET Buyer_employee_ID = Right(Displayname,LEN(Displayname)-(CHARINDEX ( '_' ,Displayname)))
WHERE CHARINDEX ( '_' ,Displayname)>0

      BEGIN TRAN

      INSERT INTO dbo.DimBidDetail
                  (BidLogItem,AskAmount,BidAmount,BidEventDatetime,BidEventDayID,BidStatusCode,BidStatusDescription,BuyerEmployeeID,BidderType,BuyerID
                   ,Displayname,Displaylocation,SequenceNumber,SSMNumber,SortOrder,GroupNumber,BidType,DeviceType,ETLLoadID,ETLLoadUpdateID,AuctionItemID,IsBidDetailShow,RunnerUpBidInd)
      SELECT T.Bid_Log_Item,
             T.Ask_Amount,
             T.Bid_Amount,
             T.Bid_Event_Datetime,
             T.Bid_Event_Day_ID,
             T.Bid_Status_Code,
             T.Bid_Status_Description,
             T.Buyer_employee_ID,
             T.Bidder_Type,
             T.Buyer_ID,
             T.Displayname,
             T.Displaylocation,
             T.Sequence_Number,
             T.SSM_Number,
             T.Sort_Order,
             T.Group_Number,
             T.Bid_Type,
             T.Device_Type,
             @ETLLoadID_sp,
             @ETLLoadID_sp,
             T.Auction_Item_ID,T.IsBidDetailShow,0
      FROM   #TMP_Bid_Detail AS T WITH (NOLOCK)
             LEFT OUTER JOIN dbo.DimBidDetail AS BD WITH (NOLOCK)
               ON T.BID_Event_Datetime=BD.BidEventDatetime
                  AND Isnull(T.Bid_Status_Code, '')=Isnull(BD.BidStatusCode, '')
                  AND Isnull(T.Bid_Type, 0)=Isnull(BD.BidType, 0)
				  AND   T.Auction_Item_ID=BD.AuctionItemID
      WHERE  BD.BidDetailID IS NULL

      SELECT @InsertRowCount_sp=Count(*)
      FROM   #temp_RecordCount
      WHERE  ChangeType='INSERT'

/************************Update the Bid Detail Show Ind for duplicate bids******************************/
		UPDATE DBD
		SET    IsBidDetailShow=1
		FROM   dbo.DimBidDetail DBD 
			   INNER JOIN (SELECT Max(BD.BidDetailID) AS BidDetailID
								FROM   dbo.DimBidDetail AS BD WITH (NOLOCK)
								WHERE  BD.BidStatusCode IN ('K', 'U')
								AND BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)
								GROUP  BY BD.BidLogItem,
										  BD.AuctionItemID,
										  BD.BidAmount,
										  BD.BidStatusCode,
										  BD.BidStatusDescription,
										  BD.BuyerEmployeeID,
										  BD.BidderType,
										  BD.BuyerID,
										  BD.Displayname,
										  BD.Displaylocation,
										  BD.BidType,
										  BD.RunnerUpBidInd,
										  BD.DeviceType) T
				 ON DBD.BidDetailID=T.BidDetailID

		UPDATE BD
		SET    DisplayOrder=DipOrd
		FROM   EDW.dbo.DimBidDetail BD
			   INNER JOIN (SELECT BD.AuctionItemID,
								  BD.BidEventDatetime,
								  BD.SequenceNumber,
								  Row_number() OVER ( PARTITION BY BD.AuctionItemID ORDER BY BD.AuctionItemID, BD.BidEventDatetime, BD.SequenceNumber) DipOrd
						   FROM   EDW.dbo.DimBidDetail BD WITH (NOLOCK)
						   WHERE  IsBidDetailShow=1 AND BD.BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)) BDOrd
				 ON BD.AuctionItemID=BDOrd.AuctionItemID
					AND BD.BidEventDatetime=BDOrd.BidEventDatetime
					AND BD.SequenceNumber=BDOrd.SequenceNumber
		WHERE  IsBidDetailShow=1 AND BD.BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)

		/************************Runner Up Bid Detail Record******************************/
		CREATE TABLE #TMP_Winning_Bid(AuctionItemID INT,MAXBidEventDatetime Datetime NULL)
		CREATE TABLE #TMP_Winning_Bid_Detail(AuctionItemID INT,Winning_Bid_Event_Datetime Datetime NULL,Winning_Bid_Amount Money NULL,Winning_Bid_Sort_Order INT NULL)
		CREATE TABLE #TMP_Runner_Up_Bid_Datetime(AuctionItemID INT,Runner_Up_Datetime Datetime NULL,Runner_Up_Bid_Amount Money NULL,Runner_Up_Sort_Order INT NULL,SequenceNumber INT NULL)

		INSERT INTO #TMP_Winning_Bid(AuctionItemID,MAXBidEventDatetime)
		SELECT AuctionItemID,
			   Max(BidEventDatetime) AS MAXBidEventDatetime
		FROM   [EDW].dbo.DimBidDetail AS BID WITH (NOLOCK)
		WHERE  BidStatusCode='K'
		   AND BID.IsBidDetailShow=1 AND BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)
		GROUP  BY BID.AuctionItemID

		INSERT INTO #TMP_Winning_Bid_Detail(AuctionItemID,Winning_Bid_Event_Datetime,Winning_Bid_Amount,Winning_Bid_Sort_Order)
		SELECT BID.AuctionItemID,
			   Max(BID.BidEventDatetime) AS Winning_Bid_Event_Datetime,
			   Max(BID.BidAmount) AS Winning_Bid_Amount,
			   Max(BID.SortOrder) AS Winning_Bid_Sort_Order
		FROM   [EDW].dbo.DimBidDetail AS BID WITH (NOLOCK)
			   INNER JOIN #TMP_Winning_Bid AS TWBD WITH (NOLOCK)
				 ON BID.AuctionItemID=TWBD.AuctionItemID
					AND BID.BidEventDatetime=TWBD.MAXBidEventDatetime
		WHERE  BidStatusCode='K'
		   AND BID.IsBidDetailShow=1
		GROUP  BY BID.AuctionItemID

		INSERT INTO #TMP_Runner_Up_Bid_Datetime(AuctionItemID,Runner_Up_Datetime,Runner_Up_Bid_Amount,Runner_Up_Sort_Order,SequenceNumber)
		SELECT BID.AuctionItemID,
			   Max(BID.BidEventDatetime) AS Runner_Up_Datetime,
			   Max(BID.BidAmount) AS Runner_Up_Bid_Amount,
			   Max(BID.SortOrder) AS Runner_Up_Sort_Order,
			   Max(SequenceNumber) AS SequenceNumber
		FROM   [EDW].dbo.DimBidDetail AS BID WITH (NOLOCK)
			   INNER JOIN #TMP_Winning_Bid_Detail AS TWBD WITH (NOLOCK)
				 ON BID.AuctionItemID=TWBD.AuctionItemID
					AND BID.BidEventDatetime<=TWBD.Winning_Bid_Event_Datetime
					AND BID.BidAmount<TWBD.Winning_Bid_Amount
					AND BID.SortOrder<TWBD.Winning_Bid_Sort_Order
		WHERE  BidStatusCode='K'
		   AND BID.IsBidDetailShow=1
		GROUP  BY BID.AuctionItemID

		UPDATE BD
		SET    RunnerUpBidInd=1
		FROM   [EDW].dbo.DimBidDetail AS BD
			   INNER JOIN #TMP_Runner_Up_Bid_Datetime AS TRUBR WITH (NOLOCK)
				 ON BD.AuctionItemID=TRUBR.AuctionItemID
					AND BD.SortOrder=TRUBR.Runner_Up_Sort_Order
					AND BD.SequenceNumber=TRUBR.SequenceNumber
					AND BD.BidEventDatetime=TRUBR.Runner_Up_Datetime
		WHERE  BidStatusCode='K'
		   AND BD.IsBidDetailShow=1


UPDATE DBD SET BidStatusDescription = 'Accept Counter Offer',IsBidDetailShow = 1 
			FROM EDW.dbo.DimBidDetail dbd
			INNER JOIN (SELECT DISTINCT DBD.BidDetailID FROM EDW.dbo.DimBidDetail dbd WITH (NOLOCK)
						inner join EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
							ON dbd.AuctionItemID = DAI.AuctionItemID
						INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
							ON DAI.StockID = DS.StockID
						INNER JOIN (SELECT dbd.* 
									FROM EDW.dbo.DimBidDetail dbd WITH (NOLOCK)
									inner join EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
										ON dbd.AuctionItemID = DAI.AuctionItemID
									INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
										ON DAI.StockID = DS.StockID
									WHERE BidStatusCode = 'E' and BidStatusDescription = 'Counter Offer' AND BidEventDatetime > GETDATE() - 3) CO 
							ON DBD.AuctionItemID = CO.AuctionItemID
						WHERE  DBD.BidStatusCode = 'A' AND DBD.SortOrder > CO.SortOrder) AA
				ON DBD.BidDetailID = AA.BidDetailID
				WHERE BidStatusDescription <> 'Accept Counter Offer'
      COMMIT TRAN

/***********Update DimStock and DimAuctionITem Live Rep Fields*****************/
      BEGIN TRAN
		UPDATE DAI SET IsHadLiveRepQualified = 1
		  --SELECT * 
		  FROM EDW.dbo.DimStock DS
		  INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
			ON DS.StockID = DAI.StockID
		  INNER JOIN (SELECT AuctionItemID,BidAmount,BidEventDatetime 
					  FROM EDW.dbo.DimBidDetail DBD 
					  WHERE BidStatusCode = 'E' and (BidStatusDescription = 'Counter Offer' OR BidStatusDescription = 'Accept Offer' OR BidStatusDescription <> 'Accept Counter Offer') AND BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)) COF
			ON DAI.AuctionItemID = COF.AuctionItemID

		UPDATE DS SET IsHadLiveRepQualified = 1
		  --SELECT * 
		  FROM EDW.dbo.DimStock DS
		  INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
			ON DS.StockID = DAI.StockID
		  INNER JOIN (SELECT AuctionItemID,BidAmount,BidEventDatetime 
					  FROM EDW.dbo.DimBidDetail DBD 
					  WHERE BidStatusCode = 'E' and (BidStatusDescription = 'Counter Offer' OR BidStatusDescription = 'Accept Offer' OR BidStatusDescription <> 'Accept Counter Offer') AND BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)) COF
			ON DAI.AuctionItemID = COF.AuctionItemID

		UPDATE DS SET IsHadLiveRepCounterOffer = 1
		  --SELECT * 
		  FROM EDW.dbo.DimStock DS
		  INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
			ON DS.StockID = DAI.StockID
		  INNER JOIN (SELECT AuctionItemID,BidAmount,BidEventDatetime 
					  FROM EDW.dbo.DimBidDetail DBD 
					  WHERE BidStatusCode = 'E' and BidStatusDescription = 'Counter Offer' AND BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)) COF
			ON DAI.AuctionItemID = COF.AuctionItemID

		UPDATE DS SET IsLiveRepOnTheBlockSold = 1
		  --SELECT * 
		  FROM EDW.dbo.DimStock DS
		  INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
			ON DS.StockID = DAI.StockID
		  INNER JOIN (SELECT Distinct DBD.AuctionItemID 
						FROM EDW.dbo.DimBidDetail DBD 
						INNER JOIN (SELECT AuctionItemID,BidAmount,BidEventDatetime 
									FROM EDW.dbo.DimBidDetail DBD 
									WHERE BidStatusCode = 'E' and (BidStatusDescription = 'Counter Offer' OR BidStatusDescription = 'Accept Offer' OR BidStatusDescription <> 'Accept Counter Offer') AND BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)) CO
										ON DBD.AuctionItemID = CO.AuctionItemID AND 
										DBD.BidAmount = CO.BidAmount AND 
										DBD.BidEventDatetime > CO.BidEventDatetime AND 
										DBD.BidStatusCode = 'A' ) COF
			ON DAI.AuctionItemID = COF.AuctionItemID

		UPDATE DS SET IsLiveRepSellIt = 1,IsLiveRepOnTheBlockSold = 1
		  --SELECT * 
		  FROM EDW.dbo.DimStock DS
		  INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
			ON DS.StockID = DAI.StockID
		  INNER JOIN (SELECT AuctionItemID,BidAmount,BidEventDatetime 
					  FROM EDW.dbo.DimBidDetail DBD 
					  WHERE BidStatusCode = 'E' and (BidStatusDescription = 'Accept Offer' OR BidStatusDescription <> 'Accept Counter Offer') AND BidEventDatetime>Dateadd(D, -3, @CurrentDatetime)) COF
			ON DAI.AuctionItemID = COF.AuctionItemID

		UPDATE EDW.dbo.DimStock SET 
		IsLiveRepOnTheBlockSold = 0
		,IsLiveRepOffTheBlockSold = 0
		where 
		(ISNULL(IsLiveRepOnTheBlockSold,1) = 1 OR 
		ISNULL(IsLiveRepOffTheBlockSold,1) = 1)
		AND IsStockSold = 0

      COMMIT TRAN
/************************Update the Bid Detail IP Address******************************/
UPDATE DBD
SET    IPAddress=LL.Location
		--SELECT * 
FROM EDW.dbo.DimBidDetail dbd WITH (NOLOCK)
INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK) 
	ON dbd.[AuctionItemID] = DAI.[AuctionItemID]
INNER JOIN EDW.dbo.DimAuction DA WITH (NOLOCK)
	ON DAI.AuctionID = DA.AuctionID
INNER JOIN EDW.dbo.LoginLog LL WITH (NOLOCK)
	ON dbd.DisplayName = LL.Name
	AND DA.BranchNumber = LL.BranchNumber
	AND dbd.BidEventDatetime BETWEEN LL.LoginTime AND LL.LogoutTime
WHERE dbd.BidEventDatetime>Dateadd(D, -3, GETDATE())

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp

      IF Object_id('tempdb..#TMP_Bidder_List') IS NOT NULL
        DROP TABLE #TMP_Bidder_List

      IF Object_id('tempdb..#TMP_Auction_Schedule_Source') IS NOT NULL
        DROP TABLE #TMP_Auction_Schedule_Source

      IF Object_id('tempdb..#TMP_Display_Location_Patch') IS NOT NULL
        DROP TABLE #TMP_Display_Location_Patch

      IF Object_id('tempdb..#TMP_Floor_Location_Patch') IS NOT NULL
        DROP TABLE #TMP_Floor_Location_Patch

      IF Object_id('tempdb..#TMP_Bid_Detail_Source') IS NOT NULL
        DROP TABLE #TMP_Bid_Detail_Source

      IF Object_id('tempdb..#TMP_Bid_Detail') IS NOT NULL
        DROP TABLE #TMP_Bid_Detail

      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL
        DROP TABLE #temp_RecordCount

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

      IF Object_id('tempdb..#TMP_Bidder_List') IS NOT NULL
        DROP TABLE #TMP_Bidder_List

      IF Object_id('tempdb..#TMP_Auction_Schedule_Source') IS NOT NULL
        DROP TABLE #TMP_Auction_Schedule_Source

      IF Object_id('tempdb..#TMP_Display_Location_Patch') IS NOT NULL
        DROP TABLE #TMP_Display_Location_Patch

      IF Object_id('tempdb..#TMP_Floor_Location_Patch') IS NOT NULL
        DROP TABLE #TMP_Floor_Location_Patch

      IF Object_id('tempdb..#TMP_Bid_Detail_Source') IS NOT NULL
        DROP TABLE #TMP_Bid_Detail_Source

      IF Object_id('tempdb..#TMP_Bid_Detail') IS NOT NULL
        DROP TABLE #TMP_Bid_Detail

      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL
        DROP TABLE #temp_RecordCount

      RETURN -1
  END CATCH
