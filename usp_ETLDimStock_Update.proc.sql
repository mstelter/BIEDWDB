/*
==============================================================================
Author		: Venkata Vempali
Create date	: 05/08/2012
Description	: Update the EDW DimStock table from CDC tables.
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
05/09/2011    VV                Updated the Logic for SaleRunCount
05/10/2011    VV                Added cdc tables to capture Salvage Id's
05/14/2011  Somesh Velupalli    Added update for Title Problem Ind
05/18/2011  Somesh Velupalli    Commented code for Title Problem Ind(Delete code after testing)
05/29/2012  Preetham Duvva      Added code to update ReleaseProblemCount, IsReleaseProblem
								and IsAllReleaseProblemResolved
05/30/2012  Preetham Duvva      Updated the logic for ReleaseProblemCount and modified the update 
								for DimStock
06/04/2012	Juan X. Jacome		Added logic to update Minimum Bid Fields
06/12/2012  Preetham Duvva      Added logic for IsHadReleaseProblem
06/13/2012  Preetham Duvva      Updated the logic for IsReleaseProblem and IsAllReleaseProblem
6/28/2012	Matt Stelter		Changed logic for updating Bid Indicators
06/29/2012	Juan X. Jacome		Added logic to update IBFRenegeInd
07/10/2012  Preetham Duvva      Added logic to include cancelled assignments for ReleaseProblem indicators
7/12/2012	Matt Stelter		Changed logic for updating Bid Indicators
7/13/2012   Preetham Duvva      Added a check for release problem indicators to check for release problem
7/13/2012	Matt Stelter		Added Update for all Sale Indicators
7/16/2012   Preetham Duvva      Added Buyer Attendance stock objects
7/20/2012   Somesh Velupalli	Removed code for IBF renege and putting it in DimStock
7/24/2012   Preetham Duvva      Removed UnitsSoldOutOfCountryBuyers and UnitsSoldOutOfStateBuyers
7/13/2012	Matt Stelter		Added Update for IsIBFDisplay
8/13/2012   Somesh Velupalli    Added update for IS IBF Qualified Indicator
8/28/2012	Matt Stelter		Changed Bid Inds
9/6/2012    Sidd MUrao          Added Logic to Update IsResidence
09/17/2012  Venkata Vempali     Added logic to Update MiscAssignStatus & StockStatus
09/18/2012  Preetham Duvva      Added code IsQualifiedSecondOffer,IsSoldSecondOffer and SecondOfferResult
09/19/2012  Preetham Duvva      Updated the logic for 2nd offer ind if Buyer Reneges/ cancels
10/22/2012  Somesh Velupalli    Added logic for Advance Charge Indicators
10/31/2012  MAtt Stelter		Update for NULL Stalls
11/15/2012  Preetham Duvva      Added Tower Kiosk Ind
12/7/2012   Preetham Duvva      Updated the source for TowerKioskInd
12/10/2012	Juan X. Jacome		Added IAAAuthorizedToPayPrepaidAdvanceChargeInd, IAAMaximumAuthorizedAmount
12/17/2012  Venkata Vempali     Update the logic for OriginalBidAmount ,OriginalBidAmountExcludingCancel ,PreviousHistoricalBid ,PreviousHistoricalBidProvider
									NextHighestHistoricalBid ,NextHighestHistoricalBidProvider  R5.10.02 S30
12/18/2012  Matt Stelter		Update for Inside Sale Indicator
12/20/2012  Venkata Vempali     Added update stmt for Provider Bids. TFS Bug 19270 
1/15/2013	Matt Stelter		Fix for IsResidence
2/4/2013	Juan X. Jacome		Added logic for IsCAT 
2/7/2013	Matt Stelter		Update for Inside Sale Indicator
2/8/2013	Juan X. Jacome		Update logic for IsCAT 
4/12/2013   Venkata Vempali     Commented code for IsMinimumBidOutlier and add in usp_ETLDimStock
4/25/2013	Juan X. Jacome		Update logic for IsCAT
5/3/2013	Matt Stelter		Added Provider Actual Cash Value and ProviderIsACVOutlier
5/28/2013	Matt Stelter		Added Provider Min Bid Amount
5/29/2013	Juan X. Jacome		Added logic to prevent dividing by 0
7/18/2013	Johh Stires			Update for Inside Sale Indicator for STR
7/23/2013	MAtt Stelter		Added Logic for ProviderSoldModelYearAge and Band
8/6/2013	Juan X. Jacome		Added logic for ProviderMinimumBidOutlier
8/20/2013   Juan X. Jacome		Updated logic for Inside Sales Ind
10/8/2013	Matt Stelter		Added code for ProviderSoldRunAndDrive
10/9/2013	John Stires			Modified the ProviderSoldModelBand logic to include updating
									Stocks where the SoldDayID = ProviderSoldDayID
10/15/2013	Juan X. Jacome		Added logic for StateAutoEmployeeID		
10/29/2013  Matt Stelter		Changed Provider Actual Cash Value and ACV Outlier		
11/11/2013	Matt Stelter		Moved Min Bid User from usp_DimStock	
02/26/2014	Matt Stelter		Fixed Provider Sold Model Year Band and Provider Sold Model Year Age	
3/3/2014	Juan X. Jacome	    Added logic for State Auto UserID
6/10/2014	Matt Stelter		Added VehicleNumber		
6/18/2014	Juan X. Jacome		Added AssignmentBranchNumber, AssignmentBranchName	
7/25/2014   Bruce Kutnick		Added code to handle new field in DimStock (IsHadPartialPayment)
7/30/2014   Bruce Kutnick		Modified code for IsHadPartialPayment that was added on 7-25-14.  We are no longer grouping
								and we are now simply changing the flag based upon what the value is in CDC for this field
								coming out of Salvage Sale table.
9/02/2014   Bruce Kutnick		Revised code to insure we get the Minimum Bid Caller Name from FactMinimumBidDetail when possible
9/10/2014	Matt Stelter		Added HD Image
9/16/2014	Matt Stelter		Min Bid User removed stipulation that there is a MinBidCallerID in SalvageInfo table
11/18/2014  Bruce Kutnick		Modified StorageLocationID update.
12/01/2014	Bruce Kutnick		Reversal of StorageLocationID changes.   REM out temp tables created and loaded (#TmpStorageLocationIDUpdate 
								and #TmpStorageLocationID) and REM out update to DimStock.StorageLocationID attribute 
12/30/2014	Matt Stelter		Fix for ProviderSoldModelYearAge & ProviderSoldModelYearBand
6/1/2015	Juan X.Jacome		Added IsASAPCAT
6/29/2015	Pratyusha Koduru	Added IsInspectionServiceCancelled
07/07/2015  Pratyusha Koduru    Added CATEventName & ManufacturerOrigin
7/29/2015	Juan X.Jacome		Added WITH(NOLOCK) to Update IsRunAndDriveAuctionProvider 
8/10/2015   Pratyusha Koduru    Updated Next Highest Historical Bid logic to show Null if there is no Rerun - I98
9/1/2015	Matt Stelter		Updated for Minimum Bid
9/16/2015   Pratyusha Koduru    Updated logic for IsInspectionServiceCancelled to look on Charge_Type_ID - I101
9/24/2015	Pratyusha Koduru	Added IsInspectionServiceReportsCancelled
10/20/2015	Juan X.Jacome		Added table alias for Run_And_Drive_Ind for --Update IsRunAndDriveAuctionProvider
12/15/2015  Pratyusha Koduru    Uncommented CATEVENTName Code
12/15/2015  Pratyusha Koduru    Uncommented IsMinimumBidOutlier (IsMinimumBidOutlier logic is also in usp_ETLDimStock now,Test it and comment it there in the next release)
1/12/2016	Matt Stelter		ProviderSoldModelYearBand2
4/22/2016	Matt Stelter		Inspection Services Cancelled Fix
5/12/2016  Yogitha Alwarsetty   Commented IsInspectionServiceCancelled,IsInspectionServiceReportsCancelled 
7/1/2016   Pratyusha Koduru     Updated logic for ProviderMinimumBidAmount and ProviderMinimumBidOutlier
8/29/2016  Pratyusha Koduru     Update IsCAT to include LA CATStocks (422,760,427,759)
10/19/2016 Yogitha Alwarsetty   Updated ISCAT to include Hurricane Matthew Stocks
10/19/2016 Yogitha Alwarsetty   Added CATIndEventDescription 
11/2/2016 Yogitha Alwarsetty    Modified CATIndEventDescription Logic
==============================================================================
Usage Example:
Exec usp_ETLDimStock_Update
==============================================================================
*/
Create  PROCEDURE [dbo].[usp_ETLDimStock_Update]

AS     

DECLARE @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		,@ProviderSoldDayID INT
		,@LastRunDate DATETIME
SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLDimStock_Update'
		
--Get the Min and Max LSN's
SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN, @LastRunDate = MinDatetime FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'
  


CREATE TABLE #TMP_Provider_Run_Salvage_ID(Salvage_ID INT NOT NULL)
CREATE CLUSTERED INDEX IDX_Provider_Run_Salvage_ID ON #TMP_Provider_Run_Salvage_ID(Salvage_ID)

CREATE TABLE #TMP_Provider_Auction_Item(
	Salvage_ID [int] NOT NULL
	,StockID [int] NOT NULL
	,AuctionDateTime [datetime] NULL
	,FinalBid [money] NULL
	,IsProviderAuction [tinyint] NOT NULL)

CREATE TABLE #TMP_Provider_Sale_Run_Counts(
	 [Salvage_ID] [int] NOT NULL
	,[Sale_Run_Count_Excluding_Cancel] [int] 
	,[Sale_Run_Count_ExCancel_ExNoBid] [int]) 
CREATE CLUSTERED INDEX IDX_Provider_Sale_Run_Counts ON #TMP_Provider_Sale_Run_Counts(Salvage_ID)   

CREATE TABLE #TMPAuctionStockID (StockID INT)	

CREATE TABLE #TMP_TitleProblemInd (Salvage_ID INT)

CREATE TABLE #TMP_ReleaseProblem (StockID INT, ReleaseProblemStartDate DateTime)

CREATE TABLE #Temp_ReleaseProblemCount(StockID Int, ReleaseProblemCount Int DEFAULT (0) NOT NULL)

CREATE TABLE #Temp_IsReleaseProblem(StockID Int)

CREATE TABLE #Temp_AllReleaseProblem(StockID Int)

CREATE TABLE #TMP_IsHadReleaseProblem (StockID Int)

CREATE TABLE #TMP_ReleaseProblemSalvage (SalvageID Int)


CREATE TABLE #TMP_IsResidence (	  [Salvage_ID] [int] NOT NULL , [Pickup_Location_ID] int )

CREATE TABLE #TMP_BRG  ( Salvage_ID INT )


CREATE TABLE #TMP_Sndoffer_Event
(	[Salvage_ID] [int] NOT NULL,
	[Salvage_Event_Code] nvarchar(10) NULL,
	[UpdateDateTime] [datetime] NULL,
	[Trank] [tinyint] NULL
)

CREATE TABLE #TMP_Sndoffer
(	[Salvage_ID] [int] NOT NULL,
	[Salvage_Event_Code] nvarchar(10) NULL,
	[QualifiedSecondOfferInd] [tinyint] NULL,
	[SoldSecondOfferInd] [tinyint] NULL,
	[SecondOfferResult] nvarchar(40) NULL,
	[UpdateDateTime] [datetime] NULL
	
)

CREATE TABLE #TMP_FactMinBidDetail
(	[StockID] [int] NOT NULL,
	[MinimumBidPreviousAmount] [money] NULL,
	[MinimumBidCurrentAmount] [money] NULL,
	[MinimumBidOriginalAmount] [money] NULL,
	MinimumBidSource nvarchar(20) NULL,
	MinimumBidAmount [money] NULL,
	MinimumBidCallerName nvarchar(70) NULL)

CREATE TABLE #AuctionEntryDeduped(Auction_Schedule_ID INT,Auction_Item_ID INT,BuyerID INT,MaximumBidAMT MONEY)
CREATE UNIQUE CLUSTERED INDEX AuctionEntryNDX
  ON #AuctionEntryDeduped ( Auction_Schedule_ID ASC, Auction_Item_ID ASC, BuyerID ASC )
CREATE TABLE #TMPIBIDInfoFact (StockID INT,AuctionID INT,MaxBidAmount MONEY,MaxIBidAmount MONEY,NumAcceptedProxyBids INT,NumAcceptedInternetBids INT,
NumAcceptedFloorBids INT,IBIDWonIND INT,ProxyPush Decimal(18,9),IBRProxyPush Decimal(18,9),LivePush Decimal(18,9))
CREATE UNIQUE CLUSTERED INDEX IXTMP6
  ON #TMPIBIDInfoFact ( AuctionID ASC, StockID ASC )
Create Table #TMPProxyBidInfoFact(StockID INT,AuctionID INT,MaxBidAmount MONEY,MaxIBidAmount MONEY,NumAcceptedProxyBids INT,
									NumAcceptedInternetBids INT,NumAcceptedFloorBids INT,ProxyBidWonIND INT,IBIDPush Decimal(18,9),LivePush Decimal(18,9))
CREATE UNIQUE CLUSTERED INDEX IXTMP7
  ON #TMPProxyBidInfoFact ( AuctionID ASC, StockID ASC )
CREATE TABLE #TMPSealedBidInfoFact(StockID INT,AuctionID INT,MaxBidAmount MONEY,MaxSaleAmount MONEY)
CREATE UNIQUE CLUSTERED INDEX IXTMP8
  ON #TMPSealedBidInfoFact ( AuctionID ASC, StockID ASC )
CREATE TABLE #TMPLiveBidWonFact(StockID INT,AuctionID INT,MaxBidAmount MONEY,MaxInternetBidAmount MONEY,MaxSaleAmount MONEY,
BuyerType CHAR(1),NumAcceptedProxyBids INT,NumAcceptedInternetBids INT,NumAcceptedFloorBids INT,IBIDPush Decimal(18,9),ProxyPush Decimal(18,9),IBRProxyPush Decimal(18,9))
CREATE TABLE #TMPBidInds(StockID INT,AuctionID INT,IBIDPushIND INT,IBIDBidReceivedIND INT,IBIDBidWonIND INT,IBIDHighBidAmount money,ProxyBidPushIND INT,
ProxyBidReceivedIND INT,ProxyBidWonIND INT,ProxyHighBidAmount money,SealedBidWonIND INT,LiveBidWonIND INT,LiveBidPushIND INT,LiveBidReceivedIND INT)
CREATE NONCLUSTERED INDEX IXTMP11 ON #TMPBidInds ( StockID ASC)

CREATE TABLE #TMP_StockStatus_Salvage_ID(Salvage_ID INT NOT NULL)

CREATE TABLE #Temp_StockAuthorizedPay(
StockID INT NOT NULL,
IAAAuthorizedToPayPrepaidAdvanceChargeInd INT NULL,
IAAMaximumAuthorizedAmount MONEY NULL)

CREATE TABLE #INSStocks(Salvage_ID INT NULL)


CREATE TABLE #TMP_IsCAT
(StockID INT NULL,
CATIndEventDescription Varchar(40) NULL)

CREATE TABLE #TMP_SPI_Data(
	  Salvage_ID [int] NOT NULL
	, Minimum_Bid_Caller_ID [int] NULL
	, Minimum_Bid_Caller_Name [varchar](70) NULL)

CREATE  TABLE #IsHadPartialPayment
		(
		Salvage_ID int,
		IsHadPartialPayment tinyint
		)

CREATE TABLE #TMP_InspectionServices_Enhancement_Cancelled(
	  [Salvage_ID] [int] NOT NULL
     , [Status_Code] [char](3) NULL
	  ,Charge_Type_ID [int] NOT NULL
	 , [Max_Work_Request_date] [datetime] NULL)

CREATE TABLE #TMP_InspectionServices_Enhancement_Completed(
	  [Salvage_ID] [int] NOT NULL
     , [Status_Code] [char](3) NULL
	  ,Charge_Type_ID [int] NOT NULL
	 , [Max_Work_Request_date1] [datetime] NULL)

CREATE TABLE #TMP_CATEventName
      (
      [CATEventName] VARCHAR(255) NULL
     ,[SalvageID] [INT] NOT NULL
      )

	  
CREATE TABLE #TMP_InspectionServices_Reports_Cancelled (
	  [Salvage_ID] [int] NOT NULL
     , [Status_Code] [char](3) NULL
	  ,Charge_Type_ID [int] NOT NULL
	 , [Max_Work_Request_date] [datetime] NULL)

CREATE TABLE #TMP_InspectionServices_Reports_Completed(
	  [Salvage_ID] [int] NOT NULL
     , [Status_Code] [char](3) NULL
	  ,Charge_Type_ID [int] NOT NULL
	 , [Max_Work_Request_date1] [datetime] NULL)	


CREATE TABLE #TMP_IsLACAT
(StockID INT NULL,
CATIndEventDescription Varchar(40) NULL)



Create TABLE #TMP_IsHurricaneCAT
(StockID INT NULL,
CATIndEventDescription Varchar(40) NULL)

CREATE TABLE #TMP_CATStocks
(StockID INT NULL,
CATIndEventDescription Varchar(40) NULL)


----CREATE	   TABLE #TmpStorageLocationID
----		(
----		Salvage_ID int,
----		TowOut_Storage_LOcation_ID int,
----		Create_DateTime datetime,
----		[Tow_Type_Code] char(2)
----		)


----CREATE  TABLE #TmpStorageLocationIDUpdate
----		(
----		Salvage_ID int,
----		TowOut_Storage_LOcation_ID int,
----		Create_DateTime datetime,
----		StockID int
----		)

----/***************************************************************************************
----Create all temp tables and Insert the values from the CDC Current into the temp tables.
----***************************************************************************************/

----/*****    Storage Location ID Change For DW Merge *****************/
----INSERT	INTO #TmpStorageLocationID
----		(
----		Salvage_Id,
----		TowOut_Storage_Location_ID,
----		Create_DateTime,
----		[Tow_Type_Code]
----		)

----SELECT	
----		[Salvage_ID],
----		[Towout_Storage_Location_ID],
----		Create_DateTime,
----		[Tow_Type_Code]
----FROM	[BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Tow](@MinLSN,@MaxLSN,'all with merge') 
----WHERE	[__$operation] = 5
----AND		Tow_Type_code = 'PK'

----INSERT INTO #TmpStorageLocationIDUpdate
----		(
----		Salvage_Id,
----		TowOut_Storage_Location_ID,
----		Create_DateTime,
----		StockId
----		)

----SELECT	t.Salvage_ID,
----		t.[Towout_Storage_Location_ID],
----		t.Create_DateTime,
----		ds.StockID
----FROM	#TmpStorageLocationID t WITH (NOLOCK)
----		INNER JOIN
----				(
----				select	Salvage_ID,
----						MAX(Create_DateTime) Create_DateTime
----				from  #TmpStorageLocationID t2 WITH (NOLOCK)  
----				GROUP BY
----						Salvage_ID
----				) AS [lasttow]
----			ON	lasttow.Salvage_ID = t.Salvage_ID
----			AND	lasttow.Create_DateTime = t.Create_DateTime
----		INNER JOIN EDW.dbo.DimStock ds WITH (NOLOCK)
----			ON ds.ASAPSalvageID = t.Salvage_ID





/*	------------	SaleRunCount Objects(Provider)	-----------------------
	This Should run after the dbo.FactionItem table is Populated/Updated */

INSERT INTO #TMP_Provider_Run_Salvage_ID (Salvage_ID)
SELECT DISTINCT Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge')
WHERE Salvage_Event_Code IN('FSEN','BYRE','BYCN','SA13','SAL4','SAL9','STLV') AND [__$operation] = 5
UNION 
SELECT DISTINCT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Auction_Item](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5
UNION
SELECT DISTINCT AI.Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Entry](@MinLSN, @MaxLSN, 'all with merge') AE
INNER JOIN [BI_ASAP_Rep].[dbo].Auction_Item AI WITH (NOLOCK)
	ON AI.Auction_Item_ID = AE.Auction_Item_ID
WHERE [__$operation] = 5
UNION
SELECT DISTINCT AI.Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Schedule](@MinLSN, @MaxLSN, 'all with merge') ASched
INNER JOIN [BI_ASAP_Rep].[dbo].Auction_Item AI WITH (NOLOCK)
	ON AI.Auction_Schedule_ID = ASched.Auction_Schedule_ID
WHERE [__$operation] = 5

INSERT INTO #TMP_Provider_Auction_Item(Salvage_ID ,StockID ,AuctionDateTime ,FinalBid ,IsProviderAuction)    
SELECT S.Salvage_ID ,DS.StockID ,AI.AuctionDateTime ,AI.FinalBid ,AI.IsProviderAuction
FROM #TMP_Provider_Run_Salvage_ID S
	INNER JOIN dbo.DimStock DS WITH(NOLOCK)
		ON S.Salvage_ID = DS.ASAPSalvageID
	INNER JOIN dbo.DimAuctionItem AI WITH(NOLOCK)
		ON DS.StockID = AI.StockID
WHERE AI.IsProviderAuction = 1 AND AI.AuctionDateTime <= GETDATE()
 
INSERT INTO #TMP_Provider_Sale_Run_Counts(Salvage_ID ,Sale_Run_Count_Excluding_Cancel ,Sale_Run_Count_ExCancel_ExNoBid)	
SELECT Salvage_ID, 
	SUM (CASE WHEN ISNUll(AuctionDateTime, 0) = 0 THEN 0 ELSE 1 END) AS Sale_Run_Count_Excluding_Cancel,
	SUM (CASE WHEN ISNUll(AuctionDateTime, 0) = 0 OR ISNULL(FinalBid,0) = 0 THEN 0 ELSE 1 END) AS Sale_Run_Count_ExCancel_ExNoBid
FROM #TMP_Provider_Auction_Item	
	GROUP BY Salvage_ID 

/*Get Auction Item Stocks to Update */
--2.  Changes to Auction_Item,Changes to Auction_Entry,Changes to Auction_Schedule, Get the StockIDs that have the event code 'BYRE', 'BYCN','FSEN','STLV' */
INSERT INTO #TMPAuctionStockID
			(StockID)
SELECT DISTINCT DS.StockID
FROM #TMP_Provider_Run_Salvage_ID tprs WITH (NOLOCK)
inner join dbo.dimstock ds WITH (NOLOCK)
on tprs.Salvage_ID = ds.ASAPSalvageID
   
---- Release Problem Stock Level Objects

INSERT INTO #TMP_ReleaseProblem
(StockID ,
 ReleaseProblemStartDate )
SELECT DISTINCT DS.StockID ,
		Isnull(SRP.Release_Problem_DateTime, '2999-12-31') As ReleaseProblemStartDate
FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Release_Problem](@MinLSN, @MaxLSN, 'all with merge') SRP
INNER JOIN DimStock DS WITH(NOLOCK)
	ON SRP.Salvage_ID = DS.ASAPSalvageID
WHERE [__$operation] = 5


----
INSERT #TMP_ReleaseProblemSalvage
(SalvageID )

SELECT DISTINCT
Salvage_ID
FROM BI_ASAP_Rep.cdc.fn_cdc_get_net_changes_dbo_Salvage_Release_Problem (@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5
UNION
SELECT DISTINCT
SRP.Salvage_ID
From BI_ASAP_Rep.cdc.fn_cdc_get_net_changes_dbo_Salvage_Event_History (@MinLSN, @MaxLSN, 'all with merge') SEH
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Release_Problem SRP WITH(NOLOCK)
	ON SEH.Salvage_ID = SRP.Salvage_ID
WHERE [__$operation] = 5
UNION
SELECT DISTINCT
SRP.Salvage_ID
FROM BI_ASAP_Rep.cdc.fn_cdc_get_net_changes_dbo_Salvage_Info (@MinLSN, @MaxLSN, 'all with merge') SI
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Release_Problem SRP WITH(NOLOCK)
	ON SI.Salvage_ID = SRP.Salvage_ID
WHERE [__$operation] = 5

--- Insert ReleaseProblemCount, IsReleaseProblem and IsAllReleaseProblemsResolved 

INSERT INTO #Temp_ReleaseProblemCount
(StockID ,
 ReleaseProblemCount )
SELECT 
FRP.StockID,
Count(FRP.ReleaseProblemStartDate)AS CountofReleaseProblems
FROM FactReleaseProblem FRP WITH(NOLOCK)
INNER JOIN  
(SELECT DISTINCT StockID FROM #TMP_ReleaseProblem) AS T  ON T.StockID   = FRP.StockID 
GROUP BY 
FRP.StockID
ORDER BY FRP.StockID

--Insert records into temp table for IsReleaseProblem
INSERT INTO #Temp_IsReleaseProblem
(StockID)
SELECT
DISTINCT
ds.StockID
FROM #TMP_ReleaseProblemSalvage TRP WITH (NOLOCK)
INNER JOIN DimStock DS WITH (NOLOCK)
	ON  TRP.SalvageID = DS.ASAPSalvageID 
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Event_History SEH WITH (NOLOCK)
	ON DS.ASAPSalvageID = SEH.Salvage_ID
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Info SI WITH (NOLOCK)
	ON SEH.Salvage_ID = SI.Salvage_ID
WHERE (SI.Release_DateTime IS NULL OR SEH.Salvage_Event_Code <> 'CLOS' OR SEH.Salvage_Event_Code <> 'ASCN')
ORDER BY StockID

--Insert records into temp table for IsAllReleaseProblemsResolved
INSERT INTO #Temp_AllReleaseProblem
(StockID)
SELECT
DISTINCT
DS.StockID
FROM #TMP_ReleaseProblemSalvage TRP WITH (NOLOCK)
INNER JOIN DimStock DS WITH (NOLOCK)
	ON  TRP.SalvageID = DS.ASAPSalvageID 
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Event_History SEH WITH (NOLOCK)
	ON DS.ASAPSalvageID = SEH.Salvage_ID
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Info SI WITH (NOLOCK)
	ON SEH.Salvage_ID = SI.Salvage_ID
WHERE (SI.Release_DateTime IS NOT NULL OR SEH.Salvage_Event_Code = 'CLOS' OR SEH.Salvage_Event_Code = 'ASCN')
ORDER BY StockID



--IsHadReleaseProblem

INSERT INTO #TMP_IsHadReleaseProblem
(StockID  )
SELECT DISTINCT DS.StockID 
FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Release_Problem](@MinLSN, @MaxLSN, 'all with merge') SRP
INNER JOIN DimStock DS WITH(NOLOCK)
	ON SRP.Salvage_ID = DS.ASAPSalvageID
WHERE SRP.Release_Problem_DateTime IS NOT NULL AND [__$operation]  = 5 

--INSERT INTO #TMP_TitleProblemInd   
--(Salvage_ID)
--SELECT DISTINCT Salvage_ID 
--FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge')
--WHERE Salvage_Event_Code IN('TPRG','CTRC','CLOS','ADRC','TPRG','TPRS') AND [__$operation] = 5
--UNION   
--SELECT DISTINCT Salvage_ID 
--FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Info](@MinLSN, @MaxLSN, 'all with merge')
--WHERE  [__$operation] = 5     
 

 --- IsResidence
INSERT INTO  #TMP_IsResidence (	  [Salvage_ID] , [Pickup_Location_ID] )
Select Salvage_ID, Storage_Location_id
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Pickup](
			    @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5

-----------------------------------------------------------------------------------------
------Gather data for Bid Indicators-----------------------------------------------------
-----------------------------------------------------------------------------------------
CREATE TABLE #TMPAuctionStockID2 (StockID INT)

INSERT INTO #TMPAuctionStockID2 (StockID)
SELECT DISTINCT StockID
FROM   #TMPAuctionStockID WITH (NOLOCK)

--DimStock - Don't need
INSERT INTO #TMPAuctionStockID2 (StockID)
SELECT DISTINCT StockID
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_internet_auction_bid](@MinLSN, @MaxLSN, 'all with merge') iab
       INNER JOIN [edw].[dbo].DimStock DS WITH (NOLOCK)
         ON iab.Salvage_ID=DS.ASAPSalvageID
WHERE  [__$operation]=5
   AND DS.StockID NOT IN (SELECT StockID
                          FROM   #TMPAuctionStockID2)

INSERT INTO #TMPAuctionStockID2 (StockID)
SELECT DISTINCT StockID
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_sale](@MinLSN, @MaxLSN, 'all with merge') ss
       INNER JOIN [edw].[dbo].DimStock DS WITH (NOLOCK)
         ON ss.Salvage_ID=DS.ASAPSalvageID
WHERE  [__$operation]=5
   AND DS.StockID NOT IN (SELECT StockID
                          FROM   #TMPAuctionStockID2)

INSERT INTO #TMPAuctionStockID2 (StockID)
SELECT DISTINCT StockID
FROM   DimPostAuctionChannel WITH(NOLOCK)
WHERE  UpdateDateTime>=Getdate()-7
   AND StockID NOT IN (SELECT StockID
                       FROM   #TMPAuctionStockID2)

INSERT INTO #TMPAuctionStockID2 (StockID)
SELECT DISTINCT StockID
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_sale](@MinLSN, @MaxLSN, 'all with merge') ss
       INNER JOIN [edw].[dbo].DimStock DS WITH (NOLOCK)
         ON ss.Salvage_ID=DS.ASAPSalvageID
WHERE  [__$operation]=5
   AND DS.StockID NOT IN (SELECT StockID
                          FROM   #TMPAuctionStockID2)

INSERT INTO #TMPAuctionStockID2 (StockID)
SELECT DISTINCT StockID
FROM   BI_Auction_Rep.dbo.IBR_Results IBRR WITH (NOLOCK)
       INNER JOIN [edw].[dbo].DimStock DS WITH (NOLOCK)
         ON IBRR.Stock_No=DS.StockNumber
WHERE  mddate>=Getdate()-7
   AND StockID NOT IN (SELECT StockID
                       FROM   #TMPAuctionStockID2)

INSERT INTO #AuctionEntryDeduped (Auction_Schedule_ID,Auction_Item_ID,BuyerID,MaximumBidAMT)
SELECT AE.Auction_Schedule_ID,
       AE.Auction_Item_ID,
       Max(AE.Buyer_ID) AS BuyerID,
       Max(AE.Bid_Amount) AS MaximumBidAMT
FROM   [BI_ASAP_REP].dbo.Auction_Entry AS AE WITH (NOLOCK)
       INNER JOIN [BI_ASAP_REP].dbo.Auction_Schedule AS ASched WITH (NOLOCK)
         ON AE.Auction_Schedule_ID=ASched.Auction_Schedule_ID
       INNER JOIN dbo.DimAuctionItem AS ai WITH (NOLOCK)
         ON ae.Auction_Item_ID=ai.AuctionItemID
            AND ae.Auction_Schedule_ID=ai.AuctionID
       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON ai.StockID=DS.StockID
       INNER JOIN #TMPAuctionStockID2 TASID2 WITH (NOLOCK)
         ON DS.StockID=TASID2.StockID
		INNER JOIN (SELECT StockID, MAX(AuctionDateTime) as MaxAuctionDateTime from DimAuctionItem WHERE AuctionDateTime < Getdate() Group by StockID) MaxAuction
			ON AI.StockID = MaxAuction.StockID and AI.AuctionDateTime = MaxAuction.MaxAuctionDateTime         
GROUP  BY AE.Auction_Schedule_ID,
          AE.Auction_Item_ID
ORDER  BY AE.Auction_Schedule_ID,
          AE.Auction_Item_ID 
-----------------------------------------------------------------------------------------
INSERT INTO #TMPIBIDInfoFact (StockID,AuctionID,MaxBidAmount,MaxIBidAmount,NumAcceptedProxyBids,NumAcceptedInternetBids,
NumAcceptedFloorBids,IBIDWonIND,ProxyPush,IBRProxyPush,LivePush)
SELECT ai.StockID,
	   ai.AuctionID,
	   ae.MaximumBidAMT AS MaxBidAmount,
	   Max(iab.Max_Amount) AS MaxIBidAmount,
	   Isnull(pac.NumAcceptedProxyBids, 0) AS NumAcceptedProxyBids,
	   Isnull(pac.NumAcceptedInternetBids, 0) AS NumAcceptedInternetBids,
	   Isnull(pac.NumOfAcceptedFloorBids, 0) AS NumAcceptedFloorBids,
	   Max(CASE
						  WHEN (won.Salvage_ID IS NOT NULL)
							   AND (pac.BuyerType='R'
									 OR pac.BuyerType='I') THEN 1
						  ELSE 0
						END) AS IBIDWonIND,
	   CASE
		 WHEN Isnull(pac.RunnerUpBidderType, 'X')=('P') AND won.Sale_Amount <> 0 THEN Isnull((pac.RunnerUpBid)/(won.Sale_Amount), 0)
		 ELSE 0
	   END AS ProxyPush,
	   CASE
		 WHEN Isnull(COALESCE(Max_bid, ae.MaximumBidAMT), 0)<Isnull(won.Sale_Amount, 0) AND won.Sale_Amount <> 0 THEN Isnull(
		 COALESCE(Max_bid, ae.MaximumBidAMT)/(won.Sale_Amount), 0)
		 ELSE 0
	   END AS IBRProxyPush,
	   Max(CASE
			 WHEN Isnull(pac.RunnerUpBidderType, 'X')='F' AND won.Sale_Amount <> 0 THEN Isnull((pac.RunnerUpBid)/(won.Sale_Amount), 0)
			 ELSE 0
		   END) AS LivePush
--INTO   #TMPIBIDInfoFact
FROM   #AuctionEntryDeduped AS ae WITH (NOLOCK)
	   INNER JOIN dbo.DimAuctionItem AS ai WITH (NOLOCK)
		 ON ae.Auction_Item_ID=ai.AuctionItemID
			AND ae.Auction_Schedule_ID=ai.AuctionID
	   INNER JOIN dbo.DimStock DS WITH (NOLOCK)
		 ON ai.StockID=DS.StockID
	   INNER JOIN [BI_ASAP_REP].dbo.internet_auction_bid AS iab WITH (NOLOCK)
		 ON DS.ASAPSalvageID=iab.Salvage_ID
			AND ai.AuctionID=iab.Auction_Schedule_ID
			AND iab.bid_source='INT'
	   LEFT OUTER JOIN [BI_ASAP_REP].dbo.Salvage_Sale AS won WITH (NOLOCK)
		 ON iab.Salvage_ID=won.Salvage_ID
			AND iab.Auction_Schedule_ID=won.Auction_Schedule_ID
			AND iab.Buyer_ID=won.Buyer_ID
			AND iab.Entry_Amount>=won.Sale_Amount
	   LEFT OUTER JOIN dbo.DimPostAuctionChannel AS pac WITH (NOLOCK)
		 ON ai.StockID=pac.StockID
			AND ai.AuctionID=pac.AuctionScheduleID
			AND pac.BuyerType IN ('I', 'R')
	   LEFT OUTER JOIN [BI_Auction_Rep].dbo.IBR_Results AS ibr WITH (NOLOCK)
		 ON Isnull(Cast(ibr.Stock_No AS INT), 1000000)=DS.ASAPSalvageID
			AND ai.AuctionDateTime=ibr.Auction_Date
			AND pac.BuyerID<>Isnull(Cast(ibr.Buyer_Id AS INT), 1000000)
GROUP  BY ai.StockID,
		  ai.AuctionID,
		  ae.MaximumBidAMT,
		  Isnull(pac.NumAcceptedProxyBids, 0),
		  Isnull(pac.NumAcceptedInternetBids, 0),
		  Isnull(pac.NumOfAcceptedFloorBids, 0),
		  CASE
			WHEN Isnull(pac.RunnerUpBidderType, 'X')=('P') AND won.Sale_Amount <> 0 THEN Isnull((pac.RunnerUpBid)/(won.Sale_Amount), 0)
			ELSE 0
		  END,
		  CASE
			WHEN Isnull(COALESCE(Max_bid, ae.MaximumBidAMT), 0)<Isnull(won.Sale_Amount, 0) AND won.Sale_Amount <> 0 THEN Isnull(
			COALESCE(Max_bid, ae.MaximumBidAMT)/(won.Sale_Amount), 0)
			ELSE 0
		  END
ORDER  BY ai.StockID,
		  ai.AuctionID
-----------------------------------------------------------------------------------------
INSERT INTO #TMPProxyBidInfoFact(StockID,AuctionID,MaxBidAmount,MaxIBidAmount,NumAcceptedProxyBids,
									NumAcceptedInternetBids,NumAcceptedFloorBids,ProxyBidWonIND,IBIDPush,LivePush)
SELECT ai.StockID,
	   ai.AuctionID,
	   MaxBidAmount = Max(ae.MaximumBidAMT),
	   MaxIBidAmount = Max(iab.Max_Amount),
	   Isnull(pac.NumAcceptedProxyBids, 0) AS NumAcceptedProxyBids,
	   Isnull(pac.NumAcceptedInternetBids, 0) AS NumAcceptedInternetBids,
	   Isnull(pac.NumOfAcceptedFloorBids, 0) AS NumAcceptedFloorBids,
	   ProxyBidWonIND = CASE
						  WHEN Max(won.Salvage_ID) IS NOT NULL --Gonzalo
						THEN 1
						  ELSE 0
						END,
	   CASE
		 WHEN Max(Isnull(pac.RunnerUpBidderType, 'X')) IN ('I', 'R') THEN Isnull(Max(pac.RunnerUpBid)/Max(won.Sale_Amount), 0)
		 ELSE 0
	   END IBIDPush,
	   CASE
		 WHEN Max(Isnull(pac.RunnerUpBidderType, 'X'))='F' THEN Isnull(Max(pac.RunnerUpBid)/Max(won.Sale_Amount), 0)
		 ELSE 0
	   END LivePush
--INTO   #TMPProxyBidInfoFact
FROM   #AuctionEntryDeduped AS ae
	   INNER JOIN dbo.DimAuctionItem AS ai WITH (NOLOCK)
		 ON ae.Auction_Item_ID=ai.AuctionItemID
			AND ae.Auction_Schedule_ID=ai.AuctionID
	   INNER JOIN dbo.DimStock DS WITH (NOLOCK)
		 ON ai.StockID=DS.StockID
	   INNER JOIN [BI_ASAP_Rep].dbo.internet_auction_bid AS iab WITH (NOLOCK)
		 ON DS.ASAPSalvageID=iab.Salvage_ID
			AND ai.AuctionID=iab.Auction_Schedule_ID
	   LEFT OUTER JOIN [BI_ASAP_REP].dbo.Salvage_Sale AS won WITH (NOLOCK)
		 ON iab.Salvage_ID=won.Salvage_ID
			AND iab.Auction_Schedule_ID=won.Auction_Schedule_ID
			AND iab.Buyer_ID=won.Buyer_ID
			AND iab.Entry_Amount<=won.Sale_Amount
	   INNER JOIN dbo.DimPostAuctionChannel AS pac WITH (NOLOCK)
		 ON ai.StockID=pac.StockID
			AND ai.AuctionID=pac.AuctionScheduleID
			AND ae.BuyerID=pac.BuyerID
			AND pac.BuyerType='P'
	   LEFT OUTER JOIN [BI_Auction_Rep].dbo.IBR_Results AS ibr WITH (NOLOCK)
		 ON won.Auction_Schedule_ID=ibr.AuctionID
			AND won.Auction_Item_ID=ibr.ItemID
WHERE  Isnull(iab.bid_source, 'NUL')<>'INT'
   AND Isnull(ai.IsSealedBid, 0)=0
   AND Isnull(iab.Sealed_Bid_Ind, 0)=0
GROUP  BY ai.StockID,
		  ai.AuctionID,
		  pac.NumAcceptedProxyBids,
		  pac.NumAcceptedInternetBids,
		  pac.NumOfAcceptedFloorBids
ORDER  BY ai.StockID,
		  ai.AuctionID;
-----------------------------------------------------------------------------------------
INSERT INTO #TMPSealedBidInfoFact(StockID,AuctionID,MaxBidAmount,MaxSaleAmount)
SELECT ai.StockID,
	   ai.AuctionID,
	   MaxBidAmount = Max(ae.MaximumBidAMT),
	   MaxSaleAmount = Max(ss.Sale_Amount)
--INTO   #TMPSealedBidInfoFact
FROM   #AuctionEntryDeduped AS ae
	   INNER JOIN dbo.DimAuctionItem AS ai WITH (NOLOCK)
		 ON ae.Auction_Item_ID=ai.AuctionItemID
			AND ae.Auction_Schedule_ID=ai.AuctionID
			AND Isnull(ai.IsSealedBid, 0)=1
	   INNER JOIN dbo.DimStock DS WITH (NOLOCK)
		 ON ai.StockID=DS.StockID
	   INNER JOIN [BI_ASAP_REP].dbo.Salvage_Sale AS ss WITH (NOLOCK)
		 ON DS.ASAPSalvageID=ss.Salvage_ID
			AND ai.AuctionID=ss.Auction_Schedule_ID
			AND ai.AuctionItemID=ss.Auction_Item_ID
GROUP  BY ai.StockID,
		  ai.AuctionID
ORDER  BY ai.AuctionID,
		  ai.StockID;
-----------------------------------------------------------------------------------------
INSERT INTO #TMPLiveBidWonFact(StockID,AuctionID,MaxBidAmount,MaxInternetBidAmount,MaxSaleAmount,
BuyerType,NumAcceptedProxyBids,NumAcceptedInternetBids,NumAcceptedFloorBids,IBIDPush,ProxyPush,IBRProxyPush)
SELECT ai.StockID,
	   ai.AuctionID,
	   Max(ae.MaximumBidAMT) AS MaxBidAmount,
	   Max(iab.Max_Amount) AS MaxInternetBidAmount,
	   Max(ss.Sale_Amount) AS MaxSaleAmount,
	   pac.BuyerType,
	   Isnull(pac.NumAcceptedProxyBids, 0) AS NumAcceptedProxyBids,
	   Isnull(pac.NumAcceptedInternetBids, 0) AS NumAcceptedInternetBids,
	   Isnull(pac.NumOfAcceptedFloorBids, 0) AS NumAcceptedFloorBids,
	   CASE
		 WHEN Max(Isnull(pac.HighNonOnsiteType, 'X')) IN ('I', 'R') THEN Max(pac.HighNonOnsiteAmount)/Max(ss.Sale_Amount)
		 ELSE 0
	   END IBIDPush,
	   CASE
		 WHEN Max(Isnull(pac.HighNonOnsiteType, 'X'))=('P') THEN Max(pac.HighNonOnsiteAmount)/Max(ss.Sale_Amount)
		 ELSE 0
	   END ProxyPush,
	   CASE
		 WHEN Max(Isnull(COALESCE(ae.MaximumBidAMT, Bid_Amt), 0))<Max(Isnull(ss.Sale_Amount, 0)) THEN Isnull(
		 Max(COALESCE(ae.MaximumBidAMT, Bid_Amt))/Max(ss.Sale_Amount), 0)
		 ELSE 0
	   END IBRProxyPush
--INTO   #TMPLiveBidWonFact
FROM   #AuctionEntryDeduped AS ae
	   INNER JOIN dbo.DimAuctionItem AS ai WITH (NOLOCK)
		 ON ae.Auction_Item_ID=ai.AuctionItemID
			AND ae.Auction_Schedule_ID=ai.AuctionID
			AND Isnull(ai.IsSealedBid, 0)=0
	   INNER JOIN dbo.DimStock DS WITH (NOLOCK)
		 ON ai.StockID=DS.StockID
	   INNER JOIN [BI_ASAP_REP].dbo.Salvage_Sale AS ss WITH (NOLOCK)
		 ON DS.ASAPSalvageID=ss.Salvage_ID
			AND ai.AuctionID=ss.Auction_Schedule_ID
			AND ai.AuctionItemID=ss.Auction_Item_ID
	   LEFT OUTER JOIN [BI_ASAP_Rep].dbo.internet_auction_bid AS iab WITH (NOLOCK)
		 ON DS.ASAPSalvageID=iab.Salvage_ID
			AND ai.AuctionID=iab.Auction_Schedule_ID
	   LEFT OUTER JOIN dbo.DimPostAuctionChannel AS pac WITH (NOLOCK)
		 ON ai.StockID=pac.StockID
			AND ai.AuctionID=pac.AuctionScheduleID
			AND pac.BuyerType='F'
	   LEFT OUTER JOIN [BI_Auction_Rep].dbo.IBR_Results AS ibr WITH (NOLOCK)
		 ON Isnull(Cast(ibr.Stock_No AS INT), 1000000)=DS.ASAPSalvageID
			AND ai.AuctionDateTime=ibr.Auction_Date
			AND pac.BuyerID<>Isnull(Cast(ibr.Buyer_Id AS INT), 1000000)
GROUP  BY ai.StockID,
		  ai.AuctionID,
		  pac.BuyerType,
		  pac.AwardedAmount,
		  pac.HighNonOnsiteType,
		  pac.HighNonOnsiteAmount,
		  pac.NumAcceptedProxyBids,
		  pac.NumAcceptedInternetBids,
		  pac.NumOfAcceptedFloorBids
HAVING ((Max(ae.MaximumBidAMT))>=Max(Isnull(iab.Max_Amount, 0))
		AND pac.BuyerType='F')
		OR ((Max(ae.MaximumBidAMT))>Max(Isnull(iab.Max_Amount, 0))
			AND (Max(ae.MaximumBidAMT))=Max(ss.Sale_Amount))
ORDER  BY ai.AuctionID,
		  ai.StockID

-----------------------------------------------------------------------------------------
INSERT INTO #TMPBidInds(StockID,AuctionID,IBIDPushIND,IBIDBidReceivedIND,IBIDBidWonIND,IBIDHighBidAmount,ProxyBidPushIND,
ProxyBidReceivedIND,ProxyBidWonIND,ProxyHighBidAmount,SealedBidWonIND,LiveBidWonIND,LiveBidPushIND,LiveBidReceivedIND)
SELECT DS.StockID, AI.AuctionID,
	   IBIDPushIND = CASE
					   WHEN lbw.IBIDPush>=.80 THEN 1
					   ELSE 0
					 END,
	   IBIDBidReceivedIND = CASE
							  WHEN Isnull(ibid.MaxIBidAmount, 0)>0
									OR proxy.NumAcceptedInternetBids>0
									OR ibid.NumAcceptedInternetBids>0
									OR lbw.NumAcceptedInternetBids>0 THEN 1
							  ELSE 0
							END,
	   IBIDBidWonIND = ibid.IBIDWonIND,
	   IBIDHighBidAmount = ibid.MaxIBidAmount,
	   ProxyBidPushIND = CASE
						   WHEN lbw.ProxyPush>=.80
								 OR lbw.IBRProxyPush>=.80 THEN 1
						   ELSE 0
						 END,
	   ProxyBidReceivedIND = CASE
							   WHEN Isnull(Proxy.MaxIBidAmount, 0)>0
									 OR Isnull(proxy.NumAcceptedProxyBids, 0)>0
									 OR Isnull(ibid.NumAcceptedProxyBids, 0)>0
									 OR Isnull(lbw.NumAcceptedProxyBids, 0)>0 THEN 1
							   ELSE 0
							 END,
	   ProxyBidWonIND = Proxy.ProxyBidWonIND,
	   ProxyHighBidAmount = Proxy.MaxIBidAmount,
	   SealedBidWonIND = CASE
						   WHEN sbi.MaxBidAmount=sbi.MaxSaleAmount THEN 1
						   ELSE 0
						 END,
	   LiveBidWonIND = CASE
						 WHEN (Isnull(lbw.MaxBidAmount, 0)=Isnull(lbw.MaxSaleAmount, 0)
							   AND Isnull(lbw.MaxBidAmount, 0)>0
							   AND lbw.MaxInternetBidAmount IS NULL)
							   OR (Isnull(lbw.BuyerType, 'X')='F') THEN 1
						 ELSE 0
					   END,
	   LiveBidPushIND = CASE
						  WHEN ibid.LivePush>=.80
								OR proxy.LivePush>=.80 THEN 1
						  ELSE 0
						END,
	   LiveBidReceivedIND = CASE
							  WHEN (Isnull(lbw.MaxBidAmount, 0)=Isnull(lbw.MaxSaleAmount, 0)
									AND Isnull(lbw.MaxBidAmount, 0)>0
									AND lbw.MaxInternetBidAmount IS NULL)
									OR (Isnull(lbw.BuyerType, 'X')='F')
									OR ibid.NumAcceptedFloorBids>0
									OR proxy.NumAcceptedFloorBids>0
									OR lbw.NumAcceptedFloorBids>0 THEN 1
							  ELSE 0
							END
--INTO   #TMPBidInds
FROM   dbo.DimStock DS WITH (NOLOCK)
	   INNER JOIN dbo.DimAuctionItem AS ai WITH (NOLOCK)
		 ON ai.StockID=DS.StockID
	   INNER JOIN #AuctionEntryDeduped AS ae WITH (NOLOCK)
		 ON ae.Auction_Item_ID=ai.AuctionItemID
			AND ae.Auction_Schedule_ID=ai.AuctionID
	   LEFT OUTER JOIN #TMPIBIDInfoFact AS ibid WITH (NOLOCK)
		 ON ae.Auction_Schedule_ID=ibid.AuctionID
			AND Ds.StockID=ibid.StockID
	   LEFT OUTER JOIN #TMPProxyBidInfoFact AS proxy WITH (NOLOCK)
		 ON ae.Auction_Schedule_ID=proxy.AuctionID
			AND Ds.StockID=proxy.StockID
	   LEFT OUTER JOIN #TMPSealedBidInfoFact AS sbi WITH (NOLOCK)
		 ON ae.Auction_Schedule_ID=sbi.AuctionID
			AND Ds.StockID=sbi.StockID
	   LEFT OUTER JOIN #TMPLiveBidWonFact AS lbw WITH (NOLOCK)
		 ON ae.Auction_Schedule_ID=lbw.AuctionID
			AND Ds.StockID=lbw.StockID

  SELECT
      DS.StockID
    , CASE WHEN PAC.BuyerType in ('I','R')          THEN 1 ELSE 0 END AS IBID_Bid_Won_IND_New
    , CASE WHEN PAC.NumAcceptedInternetBids>0     THEN 1 ELSE 0 END AS IBID_Bid_Received_IND_New
    , PAC.NumAcceptedInternetBids
    , CASE WHEN PAC.BuyerType='P'                   THEN 1 ELSE 0 END AS Proxy_Bid_Won_IND_New
    , CASE WHEN PAC.NumAcceptedProxyBids>0        THEN 1 ELSE 0 END AS Proxy_Bid_Received_IND_New
    , PAC.NumAcceptedProxyBids

    , CASE WHEN PAC.BuyerType='F'                   THEN 1 ELSE 0 END AS Live_Bid_Won_IND_New
    , CASE WHEN PAC.NumOfAcceptedFloorBids>0     THEN 1 ELSE 0 END AS Live_Bid_Received_IND_New
    , PAC.NumOfAcceptedFloorBids
INTO #taloSetsIndicators
FROM DimPostAuctionChannel PAC WITH (NOLOCK)
INNER JOIN DIMStock DS WITH (NOLOCK) 
ON  PAC.StockID = DS.StockID
			
UPDATE TBI SET
     IBIDBidWonIND         = case when ISNULL(IBIDBidWonIND,0)       <> ISNULL(t.IBID_Bid_Won_IND_New,0)       THEN ISNULL(t.IBID_Bid_Won_IND_New,0)       ELSE ISNULL(IBIDBidWonIND,0)       END
    ,IBIDBidReceivedIND    = case when ISNULL(IBIDBidReceivedIND,0)  <> ISNULL(t.IBID_Bid_Received_IND_New,0)  THEN ISNULL(t.IBID_Bid_Received_IND_New,0)  ELSE ISNULL(IBIDBidReceivedIND,0)  END
    ,ProxyBidWonIND        = case when ISNULL(ProxyBidWonIND,0)      <> ISNULL(t.Proxy_Bid_Won_IND_New,0)      THEN ISNULL(t.Proxy_Bid_Won_IND_New,0)      ELSE ISNULL(ProxyBidWonIND,0)      END
    ,ProxyBidReceivedIND   = case when ISNULL(ProxyBidReceivedIND,0) <> ISNULL(t.Proxy_Bid_Received_IND_New,0) THEN ISNULL(t.Proxy_Bid_Received_IND_New,0) ELSE ISNULL(ProxyBidReceivedIND,0) END
    ,LiveBidWonIND         = case when ISNULL(LiveBidWonIND,0)       <> ISNULL(t.Live_Bid_Won_IND_New,0)       THEN ISNULL(t.Live_Bid_Won_IND_New,0)       ELSE ISNULL(LiveBidWonIND,0)       END
    ,LiveBidReceivedIND    = case when ISNULL(LiveBidReceivedIND,0)  <> ISNULL(t.Live_Bid_Received_IND_New,0)  THEN ISNULL(t.Live_Bid_Received_IND_New,0)  ELSE ISNULL(LiveBidReceivedIND,0)  END
FROM
   #TMPBidInds TBI WITH (NOLOCK)
   --INNER JOIN dbo.DimStock DS WITH (NOLOCK)
   --    ON TBI.StockID = DS.StockID
   INNER JOIN (  SELECT
				  AuctionScheduleID,DS.StockID
				, CASE WHEN PAC.BuyerType in ('I','R')          THEN 1 ELSE 0 END AS IBID_Bid_Won_IND_New
				, CASE WHEN PAC.NumAcceptedInternetBids>0     THEN 1 ELSE 0 END AS IBID_Bid_Received_IND_New
				, PAC.NumAcceptedInternetBids
				, CASE WHEN PAC.BuyerType='P'                   THEN 1 ELSE 0 END AS Proxy_Bid_Won_IND_New
				, CASE WHEN PAC.NumAcceptedProxyBids>0        THEN 1 ELSE 0 END AS Proxy_Bid_Received_IND_New
				, PAC.NumAcceptedProxyBids

				, CASE WHEN PAC.BuyerType='F'                   THEN 1 ELSE 0 END AS Live_Bid_Won_IND_New
				, CASE WHEN PAC.NumOfAcceptedFloorBids>0     THEN 1 ELSE 0 END AS Live_Bid_Received_IND_New
				, PAC.NumOfAcceptedFloorBids
			FROM DimPostAuctionChannel PAC WITH (NOLOCK)
			INNER JOIN DIMStock DS WITH (NOLOCK) 
			ON  PAC.StockID = DS.StockID) AS t
       ON TBI.StockID = t.StockID AND TBI.AuctionID = t.AuctionScheduleID
       
       
       
 -- Getting Salvage ID's to the update MiscAssignStatus & StockStatus      
	INSERT INTO #TMP_StockStatus_Salvage_ID (Salvage_ID)
	SELECT Salvage_ID
	FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](
	   @MinLSN,@MaxLSN,'all with merge') AS S 
	   WHERE [__$operation] = 5
	UNION
	SELECT Salvage_ID
	FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Info](
			@MinLSN,@MaxLSN,'all with merge') AS SI
		 WHERE [__$operation] = 5      
  

/******2nd Offer ******/

 INSERT INTO #TMP_Sndoffer_Event
(	[Salvage_ID] ,
	[Salvage_Event_Code] ,
	[UpdateDateTime] ,
	[Trank] 
)
 
 SELECT 
      Salvage_Id
      ,Salvage_Event_Code
      ,UpdateDateTime
      ,RANK () OVER (PARTITION BY Salvage_Id ORDER BY UpdateDateTime DESC ) AS Trank
 FROM [BI_ASAP_Rep].[cdc].fn_cdc_get_net_changes_dbo_salvage_min_bid_qualifier_history (@MinLSN,@MaxLSN,'all with merge') 
 WHERE [__$operation] = 5  
  
INSERT INTO #TMP_Sndoffer
(	[Salvage_ID] ,
	[Salvage_Event_Code] ,
	[QualifiedSecondOfferInd] ,
	[SoldSecondOfferInd] ,
	[SecondOfferResult] ,
	[UpdateDateTime] 
)

 SELECT Salvage_ID,
  Salvage_Event_Code,
  CASE WHEN Salvage_Event_Code IN ('MBQI','MBOA','MBOK','MBOB','MBOE') THEN 1 ELSE 0 END AS QualifiedSecondOfferInd ,
  CASE WHEN Salvage_Event_Code = 'MBOA' THEN 1 ELSE 0 END AS SoldSecondOfferInd,
  CASE 
  WHEN Salvage_Event_Code = 'MBOA' THEN 'Sold'
  WHEN Salvage_Event_Code = 'MBOK' THEN 'Rejected' 
  WHEN Salvage_Event_Code = 'MBOB' THEN 'Negotiated' 
  WHEN Salvage_Event_Code = 'MBOE' THEN 'Expired' 
  ELSE ''  
  END AS SecondOfferResult,
  UpdateDateTime
  FROM #TMP_Sndoffer_Event  
  WHERE Trank = 1
	 
--Insert into table if buyer reneges OR cancels after the occurence of the event codes MBQI and MBOA
  INSERT INTO #TMP_BRG
  SELECT TS.Salvage_Id
  FROM #TMP_Sndoffer TS
  INNER JOIN BI_ASAP_Rep.dbo.Salvage_Event_History SEH ON SEH.Salvage_ID = TS.Salvage_Id
  WHERE  SEH.Salvage_Event_Code IN ('BYRE','BYCN') AND seh.event_datetime > TS.UpdateDatetime
  UNION
  SELECT DISTINCT SEH.Salvage_Id
  FROM  [BI_ASAP_Rep].[cdc].fn_cdc_get_net_changes_dbo_Salvage_Event_History (@MinLSN,@MaxLSN,'all with merge') SEH
  INNER JOIN BI_ASAP_Rep.dbo.Salvage_Min_Bid_Qualifier_History SQH ON SEH.Salvage_ID = SQH.Salvage_Id
  WHERE [__$operation] = 5 AND SEH.Salvage_Event_Code IN ('BYRE','BYCN') AND seh.event_datetime > SQH.UpdateDatetime
  
 
  	 
 ----------------------------------------------------------------------------------------------------   

--Advance Charge Indicators

CREATE TABLE #TMP_Salvage_Charge_ID (Salvage_ID INT NOT NULL)
INSERT INTO #TMP_Salvage_Charge_ID (Salvage_ID)
SELECT Salvage_ID
  FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider_Charge
  ( @MinLSN,@MaxLSN,'all with merge') AS SPC
  WHERE [__$operation] = 5
  UNION 
  SELECT DISTINCT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Sale](@MinLSN,@MaxLSN,'all with merge')
WHERE [__$operation] = 5



CREATE TABLE #TMP_Charge_Indicators
(Salvage_ID INT NOT NULL, 
 StockID INT,
 IsHadAdvanceCharge  INT,
 IsHadAdvanceStorage INT,
 IsHadAdvanceTow INT,
 IsHadAdvanceOther INT,
 IsSalePriceGreaterthan150 INT)  
INSERT INTO #TMP_Charge_Indicators
(Salvage_ID,
 StockID,
 IsHadAdvanceCharge,
 IsHadAdvanceStorage,
 IsHadAdvanceTow,
 IsHadAdvanceOther,
 IsSalePriceGreaterthan150) 
SELECT ASAPSalvageID,DS.StockID,
IsHadAdvanceCharge = CASE WHEN FSCE.AdvanceTotalCharge > 0 THEN 1 ELSE 0 END,
IsHadAdvanceStorage = CASE WHEN FSCE.AdvanceStorageCharge > 0 THEN 1 ELSE 0 END,
IsHadAdvanceTow = CASE WHEN FSCE.AdvanceTowCharge > 0 THEN 1 ELSE 0 END,
IsHadAdvanceOther = CASE WHEN FSCE.AdvanceOtherCharge > 0 THEN 1 ELSE 0 END,
IsSalePriceGreaterthan150 = CASE WHEN DS.SalePrice > 150 THEN 1 ELSE 0 END
FROM 
dbo.DimStock DS WITH (NOLOCK)
INNER JOIN dbo.FactStockChargeExpense FSCE WITH (NOLOCK)
ON DS.StockID = FSCE.StockID  
INNER JOIN #TMP_Salvage_Charge_ID TSC 
ON DS.ASAPSalvageID = TSC.Salvage_ID


--Tower Kiosk Ind

CREATE TABLE #TMP_ST
(StockID INT,
Event_DateTime DateTime)
INSERT INTO #TMP_ST
SELECT StockID
      ,MAX([Event_DateTime]) AS Event_DateTime
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History] (@MinLSN,@MaxLSN,'all with merge') ST 
INNER JOIN [BI_ASAP_Rep].dbo.USER_INFO UI with (nolock) ON ST.Update_user_ID = UI.User_ID AND UI.user_Name = 'Tower_Kiosk'
INNER JOIN DimStock DS ON ST.Salvage_ID = DS.ASAPSalvageID
 WHERE [__$operation] = 5   
 GROUP BY  StockID

 -- Stock IAAAuthorizedToPayPrepaidAdvanceChargeInd, IAAMaximumAuthorizedAmount
 INSERT INTO #Temp_StockAuthorizedPay(
StockID,
IAAAuthorizedToPayPrepaidAdvanceChargeInd,
IAAMaximumAuthorizedAmount
)
SELECT DS.StockID,
	   SA.IAA_Authorized_To_Pay,
	   ISNULL(SA.Max_Authorized_Pay_Amount,0)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Authorized_Amount_Info](
	   @MinLSN,@MaxLSN,'all with merge') AS SA 
INNER JOIN DimStock DS 
 ON SA.Salvage_ID = DS.ASAPSalvageID
	   WHERE [__$operation] = 5


			
/*****************************************************************************************
Updating the data from all Temp tables into the DimStock table. No Inserts & deletes.
******************************************************************************************/
BEGIN TRAN 



-- Updating data in DimStock from #TMP_Provider_Sale_Run_Counts 

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Provider_Sale_Run_Counts

----/*************Updating Storage Location **************/
----Update DS
----SET 
----	StorageLocationID = ISNULL(t.TowOut_Storage_Location_ID, 0)
----FROM dbo.DimStock DS WITH (NOLOCK)
----INNER JOIN #TmpStorageLocationIDUpdate t WITH (NOLOCK)
----	ON DS.StockID = t.StockID


----SET @UpdateRowCount_sp += @@ROWCOUNT

UPDATE DS SET
--SELECT Salvage_ID ,
	 SaleRunCountExcludingCancel = Sale_Run_Count_Excluding_Cancel
	,SaleRunCountExCancelExNoBid = Sale_Run_Count_ExCancel_ExNoBid
	,ETLLoadUpdateID		   = @ETLLoadID_sp
FROM #TMP_Provider_Sale_Run_Counts AS SRC
	INNER JOIN dbo.DimStock AS DS 
		ON DS.ASAPSalvageID = SRC.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT


/*************Updating Bid Amounts**************/
Update DS
SET 
	OriginalBidAmount = AI.OriginalBid, 
	PreviousHistoricalBid = AI.PreviousHistoricalBid,
	NextHighestHistoricalBid = CASE  WHEN (DS.SaleRunCount < 2
									 AND DS.IsIbfSold = 0)
									 --AI.IsNoValidBidAmount = 0 
									 THEN NULL
							   ELSE AI.NextHighestHistoricalBid
							   END, 
	ETLLoadUpdateID=@ETLLoadID_sp
FROM dbo.DimStock DS
INNER JOIN dbo.DimAuctionItem AI WITH (NOLOCK)
	ON DS.StockID = AI.StockID
INNER JOIN #TMPAuctionStockID ASID
	ON AI.StockID = ASID.StockID		
WHERE AI.IsMostRecentAuction = 1






SET @UpdateRowCount_sp += @@ROWCOUNT

/* First update the Provider bid's to Null and then update the bid values. TFS Bug: 19270 */ 

Update DS
SET 
	OriginalBidAmountExcludingCancel = NULL, 
	PreviousHistoricalBidProvider = NULL,
	NextHighestHistoricalBidProvider =  NULL,
	ETLLoadUpdateID=@ETLLoadID_sp
FROM dbo.DimStock DS WITH (NOLOCK)
INNER JOIN #TMPAuctionStockID ASID
	ON DS.StockID = ASID.StockID	


Update DS
SET 
	OriginalBidAmountExcludingCancel = AI.OriginalBidProvider, 
	PreviousHistoricalBidProvider = AI.PreviousHistoricalBidProvider,
	NextHighestHistoricalBidProvider =  AI.NextHighestHistoricalBidProvider,
	ETLLoadUpdateID=@ETLLoadID_sp
FROM dbo.DimStock DS WITH (NOLOCK)
INNER JOIN dbo.DimAuctionItem AI WITH (NOLOCK)
	ON DS.StockID = AI.StockID
INNER JOIN #TMPAuctionStockID ASID
	ON AI.StockID = ASID.StockID	
WHERE AI.IsMostRecentAuctionProvider = 1

---Update ReleaseProblemCount, IsHadReleaseProblem ,IsReleaseProblem, IsAllReleaseProblemsResolved
UPDATE DS
SET ReleaseProblemCount = ISNULL(TRC.ReleaseProblemCount,0),
ETLLoadUpdateID=@ETLLoadID_sp
FROM DimStock DS WITH(NOLOCK)
INNER JOIN #Temp_ReleaseProblemCount TRC On DS.StockID = TRC.StockID

UPDATE DS
SET IsHadReleaseProblem = 1,
ETLLoadUpdateID=@ETLLoadID_sp
FROM [EDW].[dbo].[DimStock] DS WITH (NOLOCK)
INNER JOIN #TMP_IsHadReleaseProblem THR On DS.StockID = THR.StockID

UPDATE DS
SET IsReleaseProblem = 1,
IsAllReleaseProblemsResolved =0,
ETLLoadUpdateID=@ETLLoadID_sp
FROM DimStock DS WITH(NOLOCK)
INNER JOIN #Temp_IsReleaseProblem TIR On DS.StockID = TIR.StockID
WHERE IsHadReleaseProblem = 1

UPDATE DS
SET IsAllReleaseProblemsResolved = 1,
IsReleaseProblem = 0,
ETLLoadUpdateID=@ETLLoadID_sp
FROM DimStock DS WITH(NOLOCK)
INNER JOIN #Temp_AllReleaseProblem TAR On DS.StockID = TAR.StockID
WHERE IsHadReleaseProblem = 1

-- Update IsMinimumBidOutlier, PreviousMinimumBidAmount,OriginalMinimumBidAmount 
INSERT INTO #TMP_FactMinBidDetail
(	StockID,
	MinimumBidPreviousAmount,
	MinimumBidCurrentAmount,
	MinimumBidOriginalAmount,
	MinimumBidSource,
	MinimumBidAmount,
	MinimumBidCallerName
)
SELECT	FMBD.StockID,
		MinimumBidPreviousAmount,
		MinimumBidCurrentAmount,
		MinimumBidOriginalAmount,

		MinimumBidSource,
		CASE WHEN MinimumBidSource = 'call' and MinimumBidCurrentAmount = 0 THEN NULL ELSE MinimumBidCurrentAmount END AS MinimumBidAmount,
     	--CASE WHEN CallerName = '' and [MinimumBidCallerName] <> '' THEN [MinimumBidCallerName] ELSE CallerName END AS MinimumBidCallerName
		CallerName AS MinimumBidCallerName
FROM dbo.FactMinimumBidDetail FMBD WITH (NOLOCK) 
INNER JOIN ( SELECT StockID,MAX(ASAPUpdateDate) as  MAXASAPUpdateDate
							FROM dbo.FactMinimumBidDetail FMBD WITH (NOLOCK)
							WHERE ETLLoadID IN (SELECT TOP 2 ETLLoadID   -- SET to TOP 2 in PEDWDB
												FROM ETLLoadAudit WITH (NOLOCK)
												WHERE PackageName = 'usp_ETLFactMinimumBidDetail'
												AND ETLLoadID <> 1
												ORDER BY ETLLoadID DESC
							)	
							GROUP BY StockID
							) 	LastMBD
ON 	FMBD.StockID = 	LastMBD.StockID AND FMBD.ASAPUpdateDate = 	LastMBD.MAXASAPUpdateDate 
order by FMBD.StockID	

--UPDATE DS 
--SET 
--   IsTitleProblem = CASE WHEN TitleProblemResolvedDate = '2999-12-31' THEN 1 ELSE 0  END 
--FROM dbo.DimStock DS 
--INNER JOIN #TMP_TitleProblemInd TT 
--ON TT.Salvage_ID = DS.ASAPSalvageID
--INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
--ON DS.StockID = FSCT.StockID 

SET @UpdateRowCount_sp += @@ROWCOUNT

/******Update Bid Indicators*************/
UPDATE DS
SET    IsIBIDPush=Isnull(IBIDPushIND, 0),
	   IsIBIDReceived=Isnull(IBIDBidReceivedIND, 0),
	   IsIBIDWon=Isnull(IBIDBidWonIND, 0),
	   IsProxyBidPush=Isnull(ProxyBidPushIND, 0),
	   IsProxyBidReceived=Isnull(ProxyBidReceivedIND, 0),
	   IsProxyBidWon=Isnull(ProxyBidWonIND, 0),
	   IsLiveBidPush=Isnull(LiveBidPushIND, 0),
	   IsLiveBidReceived=Isnull(LiveBidReceivedIND, 0),
	   IsLiveBidWon=Isnull(LiveBidWonIND, 0),
	   IsSealedBidWon=Isnull(SealedBidWonIND, 0)
FROM   dbo.DimStock DS
	   INNER JOIN #TMPBidInds TBI WITH (NOLOCK)
		 ON DS.StockID=TBI.StockID

UPDATE DS SET
	IsIBIDPush=0,
	IsIBIDWon=0,
	IsProxyBidPush=0,
	IsProxyBidWon=0,
	IsLiveBidPush=0,
	IsLiveBidWon=0,
	IsSealedBidWon=0,
	IsNegotiatedSale=0,
	IsBulkSale=0,
	IsIBFSold = 0,
	SalePrice = NULL,
    SaleTaxAmount = NULL,
    IsSoldRunAndDrive = 0,
    IsCreditSale = 0
FROM dbo.DimStock DS WITH (NOLOCK)
INNER JOIN dbo.LKStockID LKSID WITH (NOLOCK)
ON DS.StockID = LKSID.stockID
WHERE ISStockSold = 0
AND (IsIBIDPush=1 OR 
	IsIBIDWon=1 OR 
	IsProxyBidPush=1 OR 
	IsProxyBidWon=1 OR 
	IsLiveBidPush=1 OR 
	IsLiveBidWon=1 OR 
	IsSealedBidWon=1 OR 
	IsNegotiatedSale=1 OR 
	IsBulkSale=1 OR 
	IsIBFSold = 1 OR 
	SalePrice IS NOT NULL OR  
    SaleTaxAmount IS NOT NULL OR  
    IsSealedBidWon = 1 OR 
    IsSoldRunAndDrive = 1 OR 
    IsCreditSale = 1)

SET @UpdateRowCount_sp += @@ROWCOUNT


UPDATE DS 
SET IsIBFDisplay = 0
FROM dbo.FactStockCycleTime FSCT 
INNER JOIN DimStock DS
	on DS.StockID = FSCT.StockID
WHERE (IsStockSold = 1 or IsIBFSold = 1) 
AND IsIBFDisplay = 1 and SoldDateTime > GETDATE() - 2


UPDATE DS 
SET IsIBFQualified = 1
FROM dbo.FactStockCycleTime FSCT 
INNER JOIN DimStock DS 
 ON DS.StockID = FSCT.StockID 
 WHERE (IsIBFSold =1 )
 AND IsIBFQualified = 0

UPDATE dbo.dimstock SET IsIBIDWon = 0,IsProxyBidWon = 0,IsLiveBidWon =0,
	IsIBIDPush = 0, IsProxyBidPush = 0, IsLiveBidPush = 0 
--select * from dbo.dimstock
where IsIBFSold = 1 and (IsIBIDWon = 1 or IsProxyBidWon = 1 or IsLiveBidWon = 1) 

UPDATE dbo.dimstock SET IsIBIDWon = 0,IsProxyBidWon = 0,IsLiveBidWon =0,
	IsIBIDPush = 0, IsProxyBidPush = 0, IsLiveBidPush = 0 
--select * from dbo.dimstock
where IsBulkSale = 1 and (IsIBIDWon = 1 or IsProxyBidWon = 1 or IsLiveBidWon = 1) 

UPDATE dbo.DimStock SET IsProxyBidWon = 0  
where IsLiveBidWon = 1 and IsProxyBidWon = 1 and IsLiveBidReceived = 1 and IsProxyBidReceived = 0

UPDATE dbo.DimStock SET IsIBIDWon = 0  
where IsLiveBidWon = 1 and IsIBIDWon = 1 and IsLiveBidReceived = 1 and IsIBIDReceived = 0
/*********Update IBF Renege Ind *************/


/* Update IsResidence */
Update DS
Set IsResidence =DSL.IsResidence
From dbo.DimStock DS with (nolock) inner join #TMP_IsResidence TR 
on TR.Salvage_id = DS.asapsalvageid
inner join dbo.DimStorageLocation DSL with (nolock)
on DSL.Storagelocationid = DS.Pickuplocationid
--where DSL.IsResidence = 1
WHERE DS.IsResidence <> DSL.IsResidence

DELETE FROM #TMP_IsResidence

INSERT INTO  #TMP_IsResidence (	  [Salvage_ID] , [Pickup_Location_ID] )
Select 0, Storage_Location_id
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Storage_Location_Current](
			    @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5

Update DS
Set IsResidence =DSL.IsResidence
From dbo.DimStock DS with (nolock) inner join #TMP_IsResidence TR 
on TR.Salvage_id = DS.asapsalvageid
inner join dbo.DimStorageLocation DSL with (nolock)
on DSL.Storagelocationid = DS.Pickuplocationid
--where DSL.IsResidence = 1
WHERE DS.IsResidence <> DSL.IsResidence

/* Update MiscAssignStatus */

	UPDATE DS SET
		 DS.MiscAssignStatus = 0
		,ETLLoadUpdateID=@ETLLoadID_sp
	FROM dbo.DimStock DS WITH(NOLOCK)	
			INNER JOIN #TMP_StockStatus_Salvage_ID S 
				ON DS.ASAPSalvageID = S.Salvage_ID
	WHERE DS.IsCancel = 0
			AND DS.MiscAssignStatus = -1
			AND DS.AssignmentTypeCode IN ('ttl','tow')
	 
	SET @UpdateRowCount_sp += @@ROWCOUNT
	  
	--SELECT *
	UPDATE DS SET
		 DS.MiscAssignStatus = 1
		,ETLLoadUpdateID=@ETLLoadID_sp
	FROM dbo.DimStock DS WITH(NOLOCK)
		INNER JOIN #TMP_StockStatus_Salvage_ID S
			ON DS.ASAPSalvageID = S.Salvage_ID
	WHERE DS.MiscAssignStatus = 0
		AND DS.AssignmentTypeCode IN ('ttl','tow')
		AND DS.ProcessStatusCode IN ('S40','S45','S50','S55')
		
	SET @UpdateRowCount_sp += @@ROWCOUNT	
		
/* Update StockStatus */		
	UPDATE DS SET
		StockStatus = CASE WHEN DS.IsStockSold = 1 THEN 'Sold'
						   WHEN DS.IsCancel = 1 THEN 'Cancelled'
						   WHEN DS.IsStockSold = 0 AND DS.IsCancel = 0 AND DS.MiscAssignStatus = -1 THEN 'Open'
						   WHEN DS.MiscAssignStatus = 0 THEN 'Misc. Open'
						   WHEN DS.MiscAssignStatus = 1 THEN 'Misc. Close' 
					  END
		,ETLLoadUpdateID=@ETLLoadID_sp
	FROM dbo.DimStock DS WITH(NOLOCK)
		INNER JOIN #TMP_StockStatus_Salvage_ID S 
			ON DS.ASAPSalvageID = S.Salvage_ID
	WHERE ISNULL(DS.StockStatus,'') <> CASE WHEN DS.IsStockSold = 1 THEN 'Sold'
											WHEN DS.IsCancel = 1 THEN 'Cancelled'
											WHEN DS.IsStockSold = 0 AND DS.IsCancel = 0 AND DS.MiscAssignStatus = -1 THEN 'Open'
											WHEN DS.MiscAssignStatus = 0 THEN 'Misc. Open'
											WHEN DS.MiscAssignStatus = 1 THEN 'Misc. Close' 
										END
 
/******2nd Offer******/

  UPDATE DS
  SET IsQualifiedSecondOffer = T2.QualifiedSecondOfferInd,
  IsSoldSecondOffer = T2.SoldSecondOfferInd,
  SecondOfferResult = ISNULL(T2.SecondOfferResult,''),
  ETLLoadUpdateID=@ETLLoadID_sp
  FROM DimStock DS WITH (NOLOCK)
  INNER JOIN #TMP_Sndoffer T2 ON DS.ASAPSalvageid = T2.Salvage_id  
  
  
---Update Dim Stock 2nd Offer Inds for buyer reneges
  
  UPDATE DS
  SET IsQualifiedSecondOffer = 0,
  IsSoldSecondOffer = 0,
  SecondOfferResult = '',
  ETLLoadUpdateID=@ETLLoadID_sp
  FROM DimStock DS WITH (NOLOCK)
  INNER JOIN #TMP_BRG BRG ON DS.ASAPSalvageid = BRG.Salvage_id  
  
  
		
	SET @UpdateRowCount_sp += @@ROWCOUNT
	
	
-- Update DimStock with Advcharge Indicators

   UPDATE DS 
	SET 
	IsHadAdvanceCharge  = TCI.IsHadAdvanceCharge,
	IsHadAdvanceStorage  = TCI.IsHadAdvanceStorage,
	IsHadAdvanceTow  = TCI.IsHadAdvanceTow,
	IsHadAdvanceOther = TCI.IsHadAdvanceOther,
	IsSalePriceGreaterthan150 = TCI.IsSalePriceGreaterthan150
	FROM 
	dbo.DimStock DS 
	INNER JOIN #TMP_Charge_Indicators TCI 
	ON DS.ASAPSalvageID = TCI.Salvage_ID	
	
	
--Tower Kiosk Ind

UPDATE DS
SET  IsTowerKiosk = 1
FROM dbo.DimStock DS WITH(NOLOCK)
INNER JOIN #TMP_ST TS ON DS.StockID = TS.StockID 

--- Stock IAAAuthorizedToPayPrepaidAdvanceChargeInd, IAAMaximumAuthorizedAmount
UPDATE DS
SET IAAAuthorizedToPayPrepaidAdvanceChargeInd = TS.IAAAuthorizedToPayPrepaidAdvanceChargeInd,
	IAAMaximumAuthorizedAmount = TS.IAAMaximumAuthorizedAmount
FROM DimStock DS WITH(NOLOCK)
INNER JOIN #Temp_StockAuthorizedPay TS
ON DS.StockID = TS.StockID


SET @UpdateRowCount_sp += @@ROWCOUNT

UPDATE dbo.DimStock SET stallNumber = 0 WHERE stallNumber IS NULL

/*****************Inside Sales Ind*************************/

INSERT INTO #INSStocks(Salvage_ID) SELECT Salvage_ID
FROM  [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Provider_Info](
   @MinLSN,@MaxLSN,'all with merge') AS SPI
   WHERE [__$operation] = 5

INSERT INTO #INSStocks(Salvage_ID) SELECT S.Salvage_ID
FROM  [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](
   @MinLSN,@MaxLSN,'all with merge') AS S
   LEFT OUTER JOIN #INSStocks INSS WITH (NOLOCK)
   ON S.Salvage_ID = INSS.Salvage_ID
   WHERE [__$operation] = 5   AND INSS.Salvage_ID IS NULL

UPDATE DS 
SET IsINS =
CASE WHEN ASAPsp.Salvage_Provider_Group_ID <> 608
			AND (DS.ClaimNumber IN ('CFC', 'SSP', 'OTR', 'CFC*',
                         'SSP*', 'OTR*', 'RDD', 
                         'SAF', 'UIN', 'STR')
				OR DS.ClaimNumber LIKE '%GVP-%'
				OR (DS.ClaimNumber LIKE 'AVP%' AND ASAPsp.Salvage_Provider_ID=217895 )
				OR  DS.ClaimNumber LIKE 'JAL%'
				OR  DS.ClaimNumber LIKE 'IMS%'
				)
			AND AssignmentTypeCode NOT IN ('TOW', 'TTL')
	THEN 1 ELSE 0 END 
FROM   dbo.DimStock DS
       INNER JOIN BI_ASAP_Rep.dbo.Salvage_Provider ASAPsp
         ON DS.CurrentProviderID=ASAPsp.Salvage_Provider_ID
       INNER JOIN   #INSStocks INSS WITH (NOLOCK)
		ON DS.ASAPSalvageID = INSS.Salvage_ID

-- IsCAT	
INSERT INTO #TMP_IsCAT
(StockID ,
CATIndEventDescription
)
SELECT DS.StockID,
Case when (FSCT.LossDayID BETWEEN 20121027 AND 20121231 
AND FSCT.AssignmentDayID BETWEEN 20121027 AND 20130203
AND DS.DamageCodePrimary in ('FL','FW','HA','SD','SW','UK')--('Flood', 'Fresh Water', 'Hail', 'Salt Water', 'Storm Damage', 'Unknown')
AND (DS.AdministrativeBranchNumber = 636 -- Scranton
      OR  DS.AdministrativeBranchNumber IN (SELECT BranchNumber FROM dbo.DimBranch WITH(NOLOCK) WHERE StateAbbreviation IN ('NY','NJ'))
     ) 
AND DS.LossTypeCode IN ('WA','OT') --('Water', 'Other')
AND DS.CurrentProviderID IN (SELECT ProviderID FROM dbo.DimProvider WITH(NOLOCK) WHERE ProviderTypeCode = 'INS')) then 'Super storm Sandy 2012'
when (DS.CurrentProviderID IN (318937 ,329805,319281,330596,319760,329854)) then 'Dallas Flood 2015'
End CATIndEventDescription

FROM dbo.DimStock DS WITH(NOLOCK)
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON FSCT.StockID = DS.StockID
WHERE (FSCT.LossDayID BETWEEN 20121027 AND 20121231 
AND FSCT.AssignmentDayID BETWEEN 20121027 AND 20130203
AND DS.DamageCodePrimary in ('FL','FW','HA','SD','SW','UK')--('Flood', 'Fresh Water', 'Hail', 'Salt Water', 'Storm Damage', 'Unknown')
AND (DS.AdministrativeBranchNumber = 636 -- Scranton
      OR  DS.AdministrativeBranchNumber IN (SELECT BranchNumber FROM dbo.DimBranch WITH(NOLOCK) WHERE StateAbbreviation IN ('NY','NJ'))
     ) 
AND DS.LossTypeCode IN ('WA','OT') --('Water', 'Other')
AND DS.CurrentProviderID IN (SELECT ProviderID FROM dbo.DimProvider WITH(NOLOCK) WHERE ProviderTypeCode = 'INS'))
OR DS.CurrentProviderID IN (318937 ,329805,319281,330596,319760,329854)

-------Adding LA CATStocks(422BatonRouge, 760Lafayette) to the existing CAT Stocks----------------
INSERT INTO #TMP_IsLACAT
(StockID ,
CATIndEventDescription
)
SELECT DS.StockID,
'Louisiana Flood 2016'  AS CATIndEventDescription
FROM EDW.dbo.DimStock DS WITH(NOLOCK)
INNER JOIN EDW.dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON FSCT.StockID = DS.StockID
WHERE (DS.AdministrativeBranchNumber in (422,760,427,759) AND FSCT.LossDayID BETWEEN 20160811 AND 20160819) 
            AND 
      (DS.LossTypeCode IN ('WA') OR DS.DamageCodePrimary in ('FL','FW','HA','SD','SW','UK'))

---------- Adding Hurricane Matthew CATstocks---------------------------
INSERT INTO #TMP_IsHurricaneCAT
(StockID ,
 CATIndEventDescription
)
SELECT DS.StockID,
'Hurricane Matthew 2016 ' AS CATIndEventDescription
FROM EDW.dbo.DimStock DS WITH(NOLOCK)
INNER JOIN EDW.dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON FSCT.StockID = DS.StockID
WHERE ( FSCT.LossDayID BETWEEN 20161006 AND 20161014 
AND  DS.AdministrativeBranchNumber IN (SELECT BranchNumber FROM dbo.DimBranch WITH(NOLOCK)  where branchname in ('Charleston','Savannah','Raleigh','Wilmington','Tidewater','Suffolk ','Richmond','Fredericksburg-South','Fort Pierce','Jacksonville','Miami','Miami-North', 'Orlando', 'Orlando-North')
)
AND DS.CurrentProviderID IN (SELECT ProviderID FROM dbo.DimProvider WITH(NOLOCK) WHERE ProviderTypeCode = 'INS')
AND (DS.LossTypeCode IN ('WA','OT')  OR DS.DamageCodePrimary in ('FL','FW','HA','SD','SW')))

INSERT INTO #TMP_CATStocks
SELECT StockID ,CATIndEventDescription
FROM #TMP_IsCAT WITH(NOLOCK)
UNION 
SELECT StockID ,CATIndEventDescription
FROM #TMP_IsLACAT WITH(NOLOCK)
UNION 
SELECT StockID ,CATIndEventDescription
FROM #TMP_IsHurricaneCAT WITH(NOLOCK)

UPDATE DS
SET CATIndEventDescription = ISNULL(CS.CATIndEventDescription,'')
FROM DimStock DS WITH(NOLOCK)
left outer Join #TMP_CATStocks CS WITH(NOLOCK)
ON DS.stockID=CS.StockID

UPDATE DS
SET IsCAT = 0,
CATIndEventDescription = ''
FROM DimStock DS WITH(NOLOCK)
WHERE IsCAT = 1
AND StockID NOT IN (SELECT StockID FROM #TMP_CATStocks WITH(NOLOCK))


UPDATE DS
SET IsCAT = 1,
CATIndEventDescription = ISNULL(CATIndEventDescription,'')
FROM DimStock DS WITH(NOLOCK)
WHERE IsCAT = 0
AND StockID IN (SELECT StockID FROM #TMP_CATStocks WITH(NOLOCK))



/*****Populating ProviderActualCashValue, ProviderIsACVOutlier*****/	 
--IF OBJECT_ID('tempdb..#TMP_Open_Stock_ACV') IS NOT NULL DROP TABLE #TMP_Open_Stock_ACV
CREATE TABLE #TMP_Open_Stock_ACV(Salvage_ID INT)
INSERT INTO #TMP_Open_Stock_ACV(Salvage_ID)
SELECT DISTINCT SE.Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Estimate](@MinLSN,@MaxLSN,'all with merge') SE
WHERE [__$operation] = 5

UPDATE DS SET ProviderActualCashValue=ActualCashValue, ProviderIsACVOutlier=IsACVOutlier
--SELECT ProviderActualCashValue=ActualCashValue, ProviderIsACVOutlier=IsACVOutlier
FROM dbo.DimStock  DS 
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
	ON DS.StockID = FSCT.StockID
INNER JOIN #TMP_Open_Stock_ACV	 TSSS WITH (NOLOCK)
	ON DS.ASAPSalvageID = TSSS.Salvage_ID
WHERE ISNULL(FSCT.ProviderSoldDayID,29991231) = 29991231	
	OR ISNULL(FSCT.ProviderSoldDayID,29991231) = CAST(CONVERT(CHAR(8),FSCT.ProviderSoldDayID,112) AS INT)

/**********Title Direct*********/
CREATE TABLE #TMP_IsTitleDirect ([Salvage_ID] [int], [IsTitleDirect] tinyint,Salvage_Provider_Program_ID INT )

INSERT INTO #TMP_IsTitleDirect ([Salvage_ID] , [IsTitleDirect], Salvage_Provider_Program_ID)
SELECT Salvage_ID,ISNULL(Is_Active,0) as IsStockTitleDirect,Salvage_Provider_Program_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_SP_Program_Detail](
			    @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5

UPDATE DS SET IsTitleDirect = ISNULL(sppd.IsTitleDirect,0)
FROM #TMP_IsTitleDirect sppd WITH(NOLOCK)
INNER JOIN dbo.DimStock DS WITH(NOLOCK)
ON DS.ASAPSalvageID = sppd.Salvage_ID
WHERE sppd.Salvage_Provider_Program_ID = 1

--------/**********Provider Min Bid Amt**************/
--------UPDATE DS
--------SET DS.ProviderMinimumBidAmount = SPI.Minimum_Bid_Amount,
--------    DS.ProviderMinimumBidOutlier = CASE WHEN SPI.Minimum_Bid_Amount < 10 
--------	                                     OR  SPI.Minimum_Bid_Amount >= 99999 
--------										 OR  SPI.Minimum_Bid_Amount IS NULL THEN 1 
--------								   ELSE 0
--------								   END 
--------FROM dbo.DimStock DS WITH (NOLOCK)
--------INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
--------ON DS.StockID = FSCT.StockID
--------INNER JOIN [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Provider_Info](
--------   @MinLSN,@MaxLSN,'all with merge') AS SPI
--------ON DS.ASAPSalvageID = SPI.Salvage_ID   
--------WHERE FSCT.ProviderSoldDateTime = FSCT.SoldDateTime

/**********Vehicle Number**************/
UPDATE DS
SET DS.VehicleNumber = ISNULL(SPI2.Vehicle_Number,'')
FROM dbo.DimStock DS WITH (NOLOCK)
INNER JOIN [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Provider_Info](
   @MinLSN,@MaxLSN,'all with merge') AS SPI
ON DS.ASAPSalvageID = SPI.Salvage_ID 
INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Provider_Info AS SPI2
ON SPI2.Salvage_ID = SPI.Salvage_ID  

COMMIT TRAN

EXEC [dbo].[usp_ETLLoadAudit]
	@StartEnd = 'E',
	@ETLLoadID = @ETLLoadID_sp,
	@ExtractRowCount = @ExtractRowCount_sp,
	@InsertRowCount = @InsertRowCount_sp,
	@UpdateRowCount = @UpdateRowCount_sp


--*********************** ProviderSoldModelYearAge  And ProviderSoldModelYearBand **************************************
UPDATE DS SET [ProviderSoldModelYearAge] = [SoldModelYearAge],[ProviderSoldModelYearBand]=[SoldModelYearBand],[ProviderSoldModelYearBand2]=[SoldModelYearBand2]
--SELECT top 10000 FSCT.SoldDayID,FSCT.ProviderSoldDayID,* 
FROM dbo.DimStock DS with (nolock)
inner join dbo.FactStockCycleTime FSCT with (nolock)
	on DS.stockID = FSCT.StockID
inner join BI_ASAP_REP.dbo.Salvage S with (nolock)
	on DS.ASAPSalvageID = s.Salvage_ID
WHERE (ISNULL([ProviderSoldModelYearBand],'') = '' or ProviderSoldModelYearAge IS NULL or ISNULL([ProviderSoldModelYearBand2],'') = '')
and FSCT.ProviderSoldDayID < 29991231

UPDATE DS SET
--SELECT ASAPSalvageID,--YEAR(ProviderSoldDateTime),cast(ModelYearChangeMonthNumber as varchar(2)),cast(ModelYearChangeDayNumber as varchar(2)),cast(YEAR(ProviderSoldDateTime) as varchar(4))--ProviderSoldModelYearband as ProviderSoldModelYearbandORIG,ProviderSoldModelYearAge AS ProviderSoldModelYearAgeORIG,
ProviderSoldModelYearband = CASE (CASE WHEN ProviderSoldDateTime > (convert(datetime,cast(CASE WHEN ModelYearChangeMonthNumber = 0 THEN 12 ELSE ModelYearChangeMonthNumber END as varchar(2)) +'/'+
								   cast(CASE WHEN ModelYearChangeDayNumber = 0 THEN 31 ELSE ModelYearChangeDayNumber END as varchar(2))+'/'+
								    cast(YEAR(ProviderSoldDateTime) as varchar(4)),101))
		        then YEAR(ProviderSoldDateTime) + 1  else YEAR(ProviderSoldDateTime) End - (ModelYear))
    WHEN -1 THEN '0 to 4' 
    WHEN 0  THEN '0 to 4' 
    WHEN 1  THEN '0 to 4' 
    WHEN 2  THEN '0 to 4' 
    WHEN 3  THEN '0 to 4' 
    WHEN 4  THEN '0 to 4' 
    WHEN 5  THEN '5 to 8' 
    WHEN 6  THEN '5 to 8' 
    WHEN 7  THEN '5 to 8' 
    WHEN 8  THEN '5 to 8'
    ELSE '9+' END
,ProviderSoldModelYearband2 = CASE (CASE WHEN ProviderSoldDateTime > (convert(datetime,cast(CASE WHEN ModelYearChangeMonthNumber = 0 THEN 12 ELSE ModelYearChangeMonthNumber END as varchar(2)) +'/'+
								   cast(CASE WHEN ModelYearChangeDayNumber = 0 THEN 31 ELSE ModelYearChangeDayNumber END as varchar(2))+'/'+
								    cast(YEAR(ProviderSoldDateTime) as varchar(4)),101))
		        then YEAR(ProviderSoldDateTime) + 1  else YEAR(ProviderSoldDateTime) End - (ModelYear))
    WHEN -1 THEN '0 to 3' 
    WHEN 0  THEN '0 to 3' 
    WHEN 1  THEN '0 to 3' 
    WHEN 2  THEN '0 to 3' 
    WHEN 3  THEN '0 to 3' 
    WHEN 4  THEN '4 to 6' 
    WHEN 5  THEN '4 to 6' 
    WHEN 6  THEN '4 to 6' 
    WHEN 7  THEN '7 to 10' 
    WHEN 8  THEN '7 to 10'
    WHEN 9  THEN '7 to 10'
    WHEN 10  THEN '7 to 10'
    ELSE '11+' END 	 
,ProviderSoldModelYearAge =  (CASE WHEN  ProviderSoldDateTime > (convert(datetime,cast(CASE WHEN ModelYearChangeMonthNumber = 0 THEN 12 ELSE ModelYearChangeMonthNumber END as varchar(2)) +'/'+ cast(CASE WHEN ModelYearChangeDayNumber = 0 THEN 31 ELSE ModelYearChangeDayNumber END as varchar(2))+'/'+ cast(YEAR(ProviderSoldDateTime) as varchar(4)),101))
                            THEN YEAR(ProviderSoldDateTime) + 1 ELSE YEAR(ProviderSoldDateTime) END  ) - Modelyear
FROM dbo.DimStock DS with (nolock)
inner join [dbo].[DimProviderStockContract] PSC with (nolock)
	on DS.stockID = PSC.StockID
inner join dbo.FactStockCycleTime FSCT with (nolock)
	on DS.stockID = FSCT.StockID
inner join BI_ASAP_REP.dbo.Salvage S with (nolock)
	on DS.ASAPSalvageID = s.Salvage_ID
WHERE 
--ModelYearChangeMonthNumber IS NOT NULL
--and ModelYearChangeDayNumber IS NOT NULL
--and FSCT.ProviderSoldDayID < 29991231
--and ([ProviderSoldModelYearBand] IS NULL
--OR ProviderSoldModelYearAge IS NULL) 
 [ProviderSoldModelYearAge]<>((CASE WHEN  ProviderSoldDateTime > (convert(datetime,cast(CASE WHEN ModelYearChangeMonthNumber = 0 THEN 12 ELSE ModelYearChangeMonthNumber END as varchar(2)) +'/'+ cast(CASE WHEN ModelYearChangeDayNumber = 0 THEN 31 ELSE ModelYearChangeDayNumber END as varchar(2))+'/'+ cast(YEAR(ProviderSoldDateTime) as varchar(4)),101))
                            THEN YEAR(ProviderSoldDateTime) + 1 ELSE YEAR(ProviderSoldDateTime) END  ) - Modelyear)
 AND ProviderSoldDatetime between GETDATE()-30 AND '12/30/2999'
	
-- StateAutoEmployeeID	

UPDATE DS
SET StateAutoEmployeeID = NULL,
	StateAutoUserID = NULL
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](@MinLSN,@MaxLSN,'all with merge') AS S 
INNER JOIN dbo.DimStock DS WITH(NOLOCK)
  ON S.Salvage_ID = DS.ASAPSalvageID
WHERE [__$operation] = 5
AND DS.CurrentProviderId in (select ProviderID from DimProvider where ProviderGroupName = 'State Auto Insurance')



UPDATE DS
SET StateAutoEmployeeID = lksae.EmployeeNumber,
	StateAutoUserID = lksae.UserID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](@MinLSN,@MaxLSN,'all with merge') AS S 
INNER JOIN dbo.DimStock DS WITH(NOLOCK)
  ON S.Salvage_ID = DS.ASAPSalvageID
INNER JOIN dbo.LKStateAutoEmployee lksae WITH (NOLOCK)
  ON  UPPER(DS.AdjustorFirstName) = lksae.FirstName
  AND UPPER(DS.AdjustorLastName) = lksae.LastName
WHERE [__$operation] = 5
AND DS.CurrentProviderId in (select ProviderID from DimProvider where ProviderGroupName = 'State Auto Insurance')	





--Update IsRunAndDriveAuctionProvider

SELECT @ProviderSoldDayID =  CAST(CONVERT(CHAR(8),BusinessDateTime-1,112) AS INT)  FROM dbo.BusinessDay

UPDATE DS SET DS.IsRunAndDriveAuctionProvider = Run_and_Drive_Auction_Ind
--SELECT  DS.StockID,DS.IsRunAndDriveAuctionProvider,DS.IsRunAndDriveAuction, Run_and_Drive_Auction_Ind
FROM dbo.DimStock DS
INNER JOIN (SELECT AI.Salvage_ID,Max(CAST(AE.Run_And_Drive_Ind AS INT)) AS Run_and_Drive_Auction_Ind
			FROM BI_ASAP_Rep.dbo.Auction_Entry AE WITH(NOLOCK)
			INNER JOIN BI_ASAP_Rep.dbo.Auction_Item AI WITH(NOLOCK)
				ON AE.Auction_Item_ID = AI.Auction_Item_ID
			INNER JOIN BI_ASAP_REP.dbo.Auction_Schedule ASCh WITH(NOLOCK)
				ON AI.Auction_Schedule_ID = Asch.Auction_Schedule_ID
			INNER JOIN dbo.DimStock DS WITH(NOLOCK)
				ON AI.Salvage_ID = DS.ASAPSalvageID
			INNER JOIN (SELECT DS.StockID, MAX(Auction_DateTime) AS MAXAuction_DateTime
						FROM BI_ASAP_Rep.dbo.Auction_Item AI WITH(NOLOCK)
						INNER JOIN BI_ASAP_REP.dbo.Auction_Schedule ASCh WITH(NOLOCK)
							ON AI.Auction_Schedule_ID = Asch.Auction_Schedule_ID
						INNER JOIN dbo.DimStock DS WITH(NOLOCK)
							ON AI.Salvage_ID = DS.ASAPSalvageID
						INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
							ON DS.Stockid = FSCT.StockID
						WHERE Asch.Auction_DateTime <= FSCT.ProviderSoldDateTime
						GROUP BY DS.StockID) MaxAuction
				ON ASch.Auction_DateTime = MaxAuction.MAXAuction_DateTime
					AND DS.Stockid = MaxAuction.StockID
			--WHERE Asch.Auction_DateTime > '7/1/2013'
			GROUP BY  AI.Salvage_ID) S
	ON DS.ASAPSalvageID = S.Salvage_ID
	inner join dbo.factstockcycletime  fsct WITH(NOLOCK)
on ds.stockid = fsct.stockid
where ProviderSoldDayID >= @ProviderSoldDayID
and DS.IsRunAndDriveAuctionProvider<>Run_and_Drive_Auction_Ind

/***********Update Min Bid Caller Names*****************/

INSERT INTO #TMP_SPI_Data (Salvage_ID ,Minimum_Bid_Caller_ID ,Minimum_Bid_Caller_Name)
SELECT SPI.Salvage_ID
	 ,SPI.Minimum_Bid_Caller_ID AS Minimum_Bid_Caller_ID
	 ,CASE WHEN DU3.FirstName IS NULL THEN ISNULL(DU3.LastName,'') ELSE ISNULL(DU3.FirstName + ISNULL(' '+DU3.LastName,''),'') END AS Minimum_Bid_Caller_Name
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Provider_Info](
   @MinLSN,@MaxLSN,'all with merge') AS SPI
   LEFT OUTER JOIN dbo.DimUser AS DU3 with (NOLOCK)
		ON SPI.Minimum_Bid_Caller_ID = DU3.[UserID]
   WHERE [__$operation] = 5 --AND ISNULL(SPI.Minimum_Bid_Caller_ID,0) > 0

/*****  Update Minimum Bid Caller Name Using FactMinimumBidDetail Data  *********/
UPDATE	#TMP_SPI_Data
SET		Minimum_Bid_Caller_Name =  fmb.CallerName
FROM	(
		SELECT	a.[FactMinimumBidID],
				a.StockID,
				a.CallerName,
				b.ASAPSalvageID
		FROM	EDW. [dbo].[FactMinimumBidDetail] a WITH (NOLOCK)
				INNER JOIN
						(
						SELECT	MAX(a.FactMinimumBidID) FactMinimumBidID,
								a.StockId,
								a.MinimumBidCurrentAmount,
								d.ASAPSalvageID
						from	dbo.FactMinimumBidDetail a WITH (NOLOCK)
								INNER JOIN dbo.DimStock d WITH (NOLOCK)
									ON  d.StockID = a.StockID
         							AND a.MinimumBidCurrentAmount = d.MinimumBidAmount
								INNER JOIN #TMP_SPI_Data s WITH (NOLOCK)
									ON s.Salvage_ID = d.ASAPSalvageID
						WHERE LEN(a.CallerName) > 1
						GROUP BY
							a.StockId,
							a.MinimumBidCurrentAmount,
							d.ASAPSalvageID
						) AS [b]
					ON b.FactMinimumBidID = a.FactMinimumBidID
		) AS fmb
	where fmb.ASAPSalvageID = #TMP_SPI_Data.Salvage_ID

UPDATE S SET
	MinimumBidCallerID = COALESCE(Minimum_Bid_Caller_ID,MinimumBidCallerID) 
	, MinimumBidCallerName = COALESCE(Minimum_Bid_Caller_Name,MinimumBidCallerName) 
FROM #TMP_SPI_Data AS SPID WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON SPID.Salvage_ID = S.ASAPSalvageID 

--Update MinimumBid Data
UPDATE DS
SET PreviousMinimumBidAmount = TFMD.MinimumBidPreviousAmount,
	OriginalMinimumBidAmount = TFMD.MinimumBidOriginalAmount,
	IsMinimumBidOutlier = case when  TFMD.MinimumBidCurrentAmount < 10 OR  TFMD.MinimumBidCurrentAmount >=99999 
									OR TFMD.MinimumBidCurrentAmount IS NULL then 1 else 0 
						 end,
	MinimumBidSource = TFMD.MinimumBidSource,
	[MinimumBidAmount] = TFMD.MinimumBidAmount,
	[MinimumBidCallerName] = TFMD.MinimumBidCallerName	  
FROM DimStock DS WITH (NOLOCK)
INNER JOIN #TMP_FactMinBidDetail TFMD WITH (NOLOCK)
ON DS.StockID = TFMD.StockID

SET @UpdateRowCount_sp += @@ROWCOUNT

--/**********Provider Min Bid Amt**************/
--UPDATE DS
--SET DS.ProviderMinimumBidAmount = DS.[MinimumBidAmount],
--    DS.ProviderMinimumBidOutlier = CASE WHEN DS.[MinimumBidAmount] < 10 
--	                                     OR  DS.[MinimumBidAmount] >= 99999 
--										 OR  DS.[MinimumBidAmount] IS NULL THEN 1 
--								   ELSE 0
--								   END
--FROM DimStock DS WITH (NOLOCK)
--INNER JOIN #TMP_FactMinBidDetail TFMD WITH (NOLOCK)
--ON DS.StockID = TFMD.StockID								   
--INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
--ON DS.StockID = FSCT.StockID
--WHERE FSCT.ProviderSoldDateTime = FSCT.SoldDateTime

/**********Provider Min Bid Amt New Logic**************/

SELECT FSE.ASAPSalvageID, 
       FMB.FactMinimumBidID,
       FMB.MinimumBidCurrentAmount,
	   FMB.ASAPUpdateDate,
       ISNULL(Max(CASE WHEN DSE.EventCode IN('BYCN', 'BYRE') THEN FSE.EventDateTime
                              END),'2999-12-31') Buyer_Renege_Cancel_Sale_Datetime
INTO #TMP_RowNumber
FROM EDW.dbo.FactStockEvent AS FSE WITH (NOLOCK)
            INNER JOIN EDW.dbo.DimStockEventType as DSE WITH (NOLOCK)
      ON FSE.EventTypeID = DSE.EventTypeID
	  INNER JOIN EDW.dbo.DimStock DS
	  ON DS.ASAPSalvageID = FSE.ASAPSalvageID
	  LEFT OUTER JOIN ( SELECT * FROM dbo.FactMinimumBidDetail FMBD WITH (NOLOCK)
							WHERE ETLLoadID IN (SELECT TOP 2 ETLLoadID   -- SET to TOP 2 in PEDWDB
												FROM ETLLoadAudit WITH (NOLOCK)
												WHERE PackageName = 'usp_ETLFactMinimumBidDetail'
												AND ETLLoadID <> 1
												ORDER BY ETLLoadID DESC)
							)	
							FMB 
	  ON FMB.StockID = ds.StockID
WHERE EventCode IN ('BYCN','BYRE') 
GROUP BY FSE.ASAPSalvageID,FMB.FactMinimumBidID,FMB.MinimumBidCurrentAmount,FMB.ASAPUpdateDate


SELECT TP.* 
INTO #TMP_ProviderMinBidAmount
FROM (SELECT ASAPSalvageID,FactMinimumBidID, MinimumBidCurrentAmount,ASAPUpdateDate,
           Buyer_Renege_Cancel_Sale_Datetime,
           ROW_NUMBER() OVER (PARTITION BY ASAPSalvageID Order by ASAPUpdateDate DESC)  RN
         FROM #TMP_RowNumber WITH(NoLock)
         WHERE ASAPUpdateDate < Buyer_Renege_Cancel_Sale_Datetime
		 ) TP
WHERE RN = 1

UPDATE DS
SET DS.ProviderMinimumBidAmount = CASE WHEN AgreementTermTypeDescription = 'Purchase Agreement' AND IsBuyerRenegedAfterSettlement = 0 AND IsBuyerCancelAfterSettlement = 0 THEN NULL
	                                    WHEN AgreementTermTypeDescription = 'Purchase Agreement' AND IsBuyerRenegedAfterSettlement = 1 OR IsBuyerCancelAfterSettlement = 1 THEN PM.MinimumBidCurrentAmount
										WHEN AgreementTermTypeDescription <> 'Purchase Agreement' THEN DS.MinimumBidAmount
                                   END                              
FROM EDW.dbo.DimStock DS WITH(NoLock)
INNER JOIN #TMP_FactMinBidDetail TFMD WITH (NOLOCK)
ON DS.StockID = TFMD.StockID
INNER JOIN EDW.dbo.FactStockCycleTime FS WITH(NoLock)
ON DS.StockId = FS.StockId
LEFT OUTER JOIN #TMP_ProviderMinBidAmount PM WITH(NoLock)
ON DS.ASAPSalvageID = PM.ASAPSalvageID
--WHERE FS.ProviderSoldDateTime = FS.SoldDateTime
--WHERE DS.MinimumBidAmount > 0
--AND DS.MinimumBidAmount IS NOT NULL

 UPDATE DS
 SET DS.ProviderMinimumBidOutlier = 1
--SELECT ProviderMinimumBidOutlier,ProviderMinimumBidAmount,* 
FROM EDW.dbo.DimStock DS WITH(NoLock)
INNER JOIN #TMP_FactMinBidDetail TFMD WITH (NOLOCK)
ON DS.StockID = TFMD.StockID
WHERE DS.ProviderMinimumBidOutlier <> 1
AND (DS.ProviderMinimumBidAmount < 10 OR  DS.ProviderMinimumBidAmount >= 99999 OR  DS.ProviderMinimumBidAmount IS NULL)

UPDATE DS
SET DS.ProviderMinimumBidOutlier = 0
--SELECT ProviderMinimumBidOutlier,ProviderMinimumBidAmount,* 
FROM EDW.dbo.DimStock DS WITH(NoLock)
INNER JOIN #TMP_FactMinBidDetail TFMD WITH (NOLOCK)
ON DS.StockID = TFMD.StockID
WHERE DS.ProviderMinimumBidOutlier = 1
AND DS.ProviderMinimumBidAmount >= 10 and  DS.ProviderMinimumBidAmount < 99999 
AND  DS.ProviderMinimumBidAmount IS NOT NULL

--/***********Update AdjusterPhone*****************/

--UPDATE DS SET AdjusterPhone = ISNULL(Phone_Number_1,'')
--FROM EDW.dbo.DimStock DS
--INNER JOIN (SELECT 
--				 User_ID
--				,Phone_Number_1
--			FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_User_Info] (	@MINLSN, @MaxLSN, 'all with merge'  )  
--			WHERE [__$operation] = 5) PN 
--ON DS.HandlerUserID = PN.User_ID


/**************Update AssignmentBranchNumber, AssignmentBranchName ******/


IF OBJECT_ID('tempdb..#TMP_VirtualBranch_Transfer') IS NOT NULL DROP TABLE #TMP_VirtualBranch_Transfer
CREATE TABLE #TMP_VirtualBranch_Transfer(
	Salvage_ID [int] NOT NULL,
	BranchNumber [int] NOT NULL,
	BranchName varchar(60) )


INSERT INTO #TMP_VirtualBranch_Transfer(
	Salvage_ID,
	BranchNumber,
	BranchName
)

select S.Salvage_ID, S.Administrative_Branch_Number, DB.BranchName
from [BI_ASAP_Rep].[cdc].[dbo_Salvage_CT] S
INNER JOIN (  select t1.Salvage_ID, max(t1.__$seqval) AS __$seqval
				from [BI_ASAP_Rep].[cdc].[dbo_Salvage_CT] t1
				inner join [BI_ASAP_Rep].[cdc].[dbo_Salvage_CT] t2
				on t1.__$seqval = t2.__$seqval
				AND t1.Administrative_Branch_Number <> t2.Administrative_Branch_Number
				and t1.__$operation = 3
				and t2.__$operation = 4
				INNER JOIN EDW.dbo.DimBranch DB1 WITH(NOLOCK)
				ON T1.Administrative_Branch_Number = DB1.BranchNumber
				INNER JOIN EDW.dbo.DimBranch DB2 WITH(NOLOCK)
				ON T2.Administrative_Branch_Number = DB2.BranchNumber
			WHERE DB1.BranchType <> 'VIR'
			AND DB2.BranchType = 'VIR'
			group by t1.Salvage_ID
		) S2
	ON S.__$seqval = S2.__$seqval
	AND S.__$operation = 3
INNER JOIN EDW.dbo.DimBranch DB WITH(NOLOCK) 
	ON DB.BranchNumber = S.Administrative_Branch_Number


UPDATE S SET
		AssignmentBranchNumber = VBT.BranchNumber
		,AssignmentBranchName = VBT.BranchName
FROM #TMP_VirtualBranch_Transfer AS VBT WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON VBT.Salvage_ID = S.ASAPSalvageID 


/* Update For IsHadPartialPayment Attribute */
INSERT	INTO #IsHadPartialPayment
		(
		Salvage_ID,
		IsHadPartialPayment
		)
SELECT	salvage_ID, 
		(CONVERT(tinyint, Partial_Payment_Ind)) Partial_Payment_Ind 
		--Max(CONVERT(tinyint, Partial_Payment_Ind)) Partial_Payment_Ind 
FROM    [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_sale](@MinLSN, @MaxLSN , 'all with merge')  
WHERE  [__$operation]=5
--AND	   Partial_Payment_Ind = 1
--GROUP BY
--	   salvage_ID

CREATE CLUSTERED INDEX ix1 ON #IsHadPartialPayment (Salvage_ID)

UPDATE	DS
SET		DS.[IsHadPartialPayment] = a.IsHadPartialPayment
FROM	dbo.DimStock DS WITH (NOLOCK)
		INNER JOIN #IsHadPartialPayment a
			ON a.Salvage_ID  = DS.ASAPSalvageID

--IsHDImage
UPDATE DS SET IsHDImage = 1 
FROM EDW.dbo.DimStock DS WITH (NOLOCK)
INNER JOIN (SELECT ISM.* FROM [Staging].[dbo].[ImageSalvageIdMapping] ISM WITH (NOLOCK)
			INNER JOIN (SELECT [SalvageID]
				  ,MAX([MDDate]) AS MaxMDDATE
			  FROM [Staging].[dbo].[ImageSalvageIdMapping] WITH (NOLOCK)
			  WHERE MDDate >= @LastRunDate
			  GROUP BY SalvageID) ISMM
			ON ISM.SalvageID = ISMM.SalvageID AND ISM.MDDate = ISMM.MaxMDDATE
			WHERE DZIndicator = 1 ) IND
			ON DS.ASAPSalvageID = IND.SalvageID


--IsASAPCAT
UPDATE DS
SET IsASAPCAT = ISNULL(S.CATFlag, 0)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](
   @MinLSN,@MaxLSN,'all with merge') AS CDCS 
INNER JOIN BI_ASAP_REP.dbo.Salvage S WITH(NOLOCK)
	ON CDCS.Salvage_Id = S.Salvage_Id	
INNER JOIN dbo.DimStock DS WITH(NOLOCK)
	ON DS.ASAPSalvageID = S.Salvage_ID	
WHERE [__$operation] = 5

-- IsInspectionServiceCancelled
	 

INSERT INTO #TMP_InspectionServices_Enhancement_Cancelled 
      (
       Salvage_ID
	  ,Charge_Type_ID 
      ,Max_Work_Request_date
      )

SELECT	SE.Salvage_ID
       ,CT.Charge_Type_ID
       ,MAX(SE.Work_Request_Date) AS Max_Work_Request_date
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
    ON C.Charge_ID = CT.Charge_ID
WHERE CT.Charge_ID = 281
    AND SE.Status_Code IN ('WCN','WDC')
    AND [__$operation]  = 5 
GROUP BY SE.Salvage_ID,SE.Status_Code,CT.Charge_Type_ID


INSERT INTO #TMP_InspectionServices_Enhancement_Completed 
      (
       Salvage_ID
	  ,Charge_Type_ID 
      ,Max_Work_Request_date1
      )

SELECT	SE.Salvage_ID
       ,CT.Charge_Type_ID
       ,MAX(SE.Work_Request_Date) AS  Max_Work_Request_date1
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
	ON C.Charge_ID = CT.Charge_ID
WHERE CT.Charge_ID = 281 
    AND SE.Status_Code IN ('WCP','APN','WPN')
    AND SE.Salvage_ID IN ( SELECT Salvage_id FROM #TMP_InspectionServices_Enhancement_Cancelled)
    AND [__$operation]  = 5  
GROUP BY SE.Salvage_ID,SE.Status_Code,CT.Charge_Type_ID

------- Commented IsInspectionServiceCancelled old logic----------------
--UPDATE DS
--SET IsInspectionServiceCancelled = 1
----SELECT DS.* 
--FROM EDW.dbo.DimStock DS
--INNER JOIN ( select TMP1.* FROM #TMP_InspectionServices_Enhancement_Cancelled TMP1 WITH(NOLOCK)
--                    LEFT OUTER JOIN #TMP_InspectionServices_Enhancement_Completed TMP2 WITH(NOLOCK)
--	                     ON  TMP1.Salvage_Id = TMP2.Salvage_Id   
--						 AND TMP1.Charge_Type_Id = TMP2.Charge_Type_Id 
--                    WHERE TMP1.Max_Work_Request_date > ISNULL(Max_Work_Request_date1, '2000-01-01')
--            ) TS
--    ON DS.ASAPSalvageID = TS.Salvage_ID


--UPDATE DS
--SET IsInspectionServiceCancelled = 0
----SELECT DS.*
--FROM EDW.dbo.DimStock DS
--INNER JOIN (select TMP1.* FROM #TMP_InspectionServices_Enhancement_Completed TMP1 WITH(NOLOCK)
--                   LEFT OUTER JOIN #TMP_InspectionServices_Enhancement_Cancelled TMP2 WITH(NOLOCK)
--	                    ON  TMP1.Salvage_Id = TMP2.Salvage_Id  
--						AND TMP1.Charge_Type_Id = TMP2.Charge_Type_Id  
--                   WHERE TMP1.Max_Work_Request_date1 > ISNULL(TMP2.Max_Work_Request_date,'2000-01-01')
--           ) TS1
--    ON DS.ASAPSalvageID = TS1.Salvage_ID
--AND IsInspectionServiceCancelled = 1


-- IsInspectionServiceReportsCancelled
	 

INSERT INTO #TMP_InspectionServices_Reports_Cancelled
      (
       Salvage_ID
	  ,Charge_Type_ID 
      ,Max_Work_Request_date
      )

SELECT	SE.Salvage_ID
       ,CT.Charge_Type_ID
       ,MAX(SE.Work_Request_Date) AS Max_Work_Request_date
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
    ON C.Charge_ID = CT.Charge_ID
WHERE CT.Charge_ID = 280 
    AND SE.Status_Code IN ('WCN','WDC')
    AND [__$operation]  = 5 
GROUP BY SE.Salvage_ID,SE.Status_Code,CT.Charge_Type_ID


INSERT INTO #TMP_InspectionServices_Reports_Completed 
      (
       Salvage_ID
	  ,Charge_Type_ID 
      ,Max_Work_Request_date1
      )

SELECT	SE.Salvage_ID
       ,CT.Charge_Type_ID
       ,MAX(SE.Work_Request_Date) AS  Max_Work_Request_date1
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
	ON C.Charge_ID = CT.Charge_ID
WHERE CT.Charge_ID = 280  
    AND SE.Status_Code IN ('WCP','APN','WPN')
    AND SE.Salvage_ID IN ( SELECT Salvage_id FROM #TMP_InspectionServices_Reports_Cancelled)
    AND [__$operation]  = 5  
GROUP BY SE.Salvage_ID,SE.Status_Code,CT.Charge_Type_ID

------ Commented IsInspectionServiceReportsCancelled------
--UPDATE DS
--SET IsInspectionServiceReportsCancelled = 1
----SELECT DS.* 
--FROM EDW.dbo.DimStock DS
--INNER JOIN ( select TMP1.* FROM #TMP_InspectionServices_Reports_Cancelled TMP1 WITH(NOLOCK)
--                    LEFT OUTER JOIN #TMP_InspectionServices_Reports_Completed TMP2 WITH(NOLOCK)
--	                     ON  TMP1.Salvage_Id = TMP2.Salvage_Id   
--						 AND TMP1.Charge_Type_Id = TMP2.Charge_Type_Id 
--                    WHERE TMP1.Max_Work_Request_date > ISNULL(Max_Work_Request_date1, '2000-01-01')
--            ) TS
--    ON DS.ASAPSalvageID = TS.Salvage_ID


--UPDATE DS
--SET IsInspectionServiceReportsCancelled = 0
----SELECT DS.*
--FROM EDW.dbo.DimStock DS
--INNER JOIN (select TMP1.* FROM #TMP_InspectionServices_Reports_Completed TMP1 WITH(NOLOCK)
--                   LEFT OUTER JOIN #TMP_InspectionServices_Reports_Cancelled TMP2 WITH(NOLOCK)
--	                    ON  TMP1.Salvage_Id = TMP2.Salvage_Id  
--						AND TMP1.Charge_Type_Id = TMP2.Charge_Type_Id  
--                   WHERE TMP1.Max_Work_Request_date1 > ISNULL(TMP2.Max_Work_Request_date,'2000-01-01')
--           ) TS1
--    ON DS.ASAPSalvageID = TS1.Salvage_ID

--AND IsInspectionServiceCancelled = 1
    ---Commented [IsInspectionServiceCancelled]-----
UPDATE ds set 
--[IsInspectionServiceCancelled] = 0,
InspectionServiceLevel = EL.Enhancement_Level_Desc,
EnhancementLevelID = SEL.Enhancement_Level_ID
--select * 
from edw.dbo.dimstock ds
INNER JOIN EDW.dbo.FactStockCycleTime FSCT WITH (NOLOCK)
	ON DS.StockID = FSCT.StockID
inner join BI_ASAP_REP.[dbo].[Salvage_Enhancement_Level] SEL WITH(NOLOCK)
	ON ds.ASAPSalvageID = SEL.Salvage_ID
INNER JOIN BI_ASAP_REP.[dbo].[Enhancement_Level] EL WITH(NOLOCK)
	ON SEL.[Enhancement_Level_ID] = EL.[Enhancement_Level_ID]
where [IsInspectionServiceCancelled]= 1 AND SEL.Enhancement_Level_ID > 0 AND FSCT.TimeZonedInspectionServicesFlippedDayID > 20160112

------CATEventName--------------------------

INSERT INTO #TMP_CATEventName (CATEventName, SalvageID)
SELECT 
      ISNULL(CE.[EventName],'') AS CATEventName,
      S.Salvage_Id AS SalvageID
 From [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_SalvageCATInfo](@MinLSN, @MaxLSN, 'all with merge') AS SCI  
    INNER JOIN [BI_ASAP_REP].dbo.Salvage S  WITH(NOLOCK)
 ON S.Salvage_ID = SCI.SalvageID 
    INNER JOIN [BI_ASAP_REP].dbo.CATEvent CE WITH(NOLOCK)
 ON SCI.CATEventID = CE.EventID
 WHERE [__$operation]  = 5  


UPDATE DS
SET DS.CATEventName = TMP1.CATEventName
--SELECT *
FROM [EDW].dbo.DimStock DS
   INNER JOIN #TMP_CATEventName TMP1 WITH(NOLOCK)
ON DS.ASAPSalvageID = TMP1.SalvageID

--------------------------Manufacturer Origin--------------------------

UPDATE DS 
SET DS.ManufacturerOrigin = lo.ManufacturerOrigin
FROM EDW.dbo.DimStock DS 
INNER JOIN LKManufacturerOrgin lo WITH (NOLOCK)
	ON lo.Make = DS.MakeName
WHERE lo.UpdateDateTime > @LastRunDate

IF OBJECT_ID('tempdb..#TMP_Provider_Run_Salvage_ID') IS NOT NULL DROP TABLE #TMP_Provider_Run_Salvage_ID
IF OBJECT_ID('tempdb..#TMP_Provider_Auction_Item') IS NOT NULL DROP TABLE #TMP_Provider_Auction_Item
IF OBJECT_ID('tempdb..#TMP_Provider_Sale_Run_Counts') IS NOT NULL DROP TABLE #TMP_Provider_Sale_Run_Counts
IF OBJECT_ID('tempdb..#TMPAuctionStockID') IS NOT NULL DROP TABLE #TMPAuctionStockID
IF OBJECT_ID('tempdb..#TMP_TitleProblemInd') IS NOT NULL DROP TABLE #TMP_TitleProblemInd
IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
IF OBJECT_ID('tempdb..#Temp_ReleaseProblemCount') IS NOT NULL DROP TABLE #Temp_ReleaseProblemCount
IF OBJECT_ID('tempdb..#Temp_IsReleaseProblem') IS NOT NULL DROP TABLE #Temp_IsReleaseProblem
IF OBJECT_ID('tempdb..#Temp_AllReleaseProblem') IS NOT NULL DROP TABLE #Temp_AllReleaseProblem
IF OBJECT_ID('tempdb..#TMP_FactMinBidDetail') IS NOT NULL DROP TABLE #TMP_FactMinBidDetail
IF OBJECT_ID('tempdb..#TMP_IsHadReleaseProblem') IS NOT NULL DROP TABLE #TMP_IsHadReleaseProblem
IF Object_id('tempdb..#AuctionEntryDeduped') IS NOT NULL  DROP TABLE #AuctionEntryDeduped
IF Object_id('tempdb..#TMPIBIDInfoFact') IS NOT NULL  DROP TABLE #TMPIBIDInfoFact
IF Object_id('tempdb..#TMPProxyBidInfoFact') IS NOT NULL  DROP TABLE #TMPProxyBidInfoFact
IF Object_id('tempdb..#TMPSealedBidInfoFact') IS NOT NULL  DROP TABLE #TMPSealedBidInfoFact
IF Object_id('tempdb..#TMPLiveBidWonFact') IS NOT NULL  DROP TABLE #TMPLiveBidWonFact
IF Object_id('tempdb..#TMPBidInds') IS NOT NULL  DROP TABLE #TMPBidInds
IF OBJECT_ID('tempdb..#TMP_Provider_Auction_Item2') IS NOT NULL DROP TABLE #TMP_Provider_Auction_Item2
IF OBJECT_ID('tempdb..#TMP_ReleaseProblemSalvage') IS NOT NULL DROP TABLE #TMP_ReleaseProblemSalvage
IF OBJECT_ID('tempdb..#TMP_IsResidence') IS NOT NULL DROP TABLE #TMP_IsResidence
IF OBJECT_ID('tempdb..#TMP_StockStatus_Salvage_ID') IS NOT NULL DROP TABLE #TMP_StockStatus_Salvage_ID
IF OBJECT_ID('tempdb..#TMP_Sndoffer_Event') IS NOT NULL DROP TABLE #TMP_Sndoffer_Event
IF OBJECT_ID('tempdb..#TMP_Sndoffer') IS NOT NULL DROP TABLE #TMP_Sndoffer
IF OBJECT_ID('tempdb..#TMP_BRG') IS NOT NULL DROP TABLE #TMP_BRG
IF OBJECT_ID('tempdb..#TMP_ST') IS NOT NULL DROP TABLE #TMP_ST
IF OBJECT_ID('tempdb..#Temp_StockAuthorizedPay') IS NOT NULL DROP TABLE #Temp_StockAuthorizedPay
IF OBJECT_ID('tempdb..#INSStocks') IS NOT NULL DROP TABLE #INSStocks
IF OBJECT_ID('tempdb..#TMP_FactMinBidDetail') IS NOT NULL DROP TABLE #TMP_FactMinBidDetail
IF OBJECT_ID('tempdb..#TMP_StockStatus_Salvage_ID') IS NOT NULL DROP TABLE #TMP_StockStatus_Salvage_ID
IF OBJECT_ID('tempdb..#Temp_StockAuthorizedPay') IS NOT NULL DROP TABLE #Temp_StockAuthorizedPay
IF OBJECT_ID('tempdb..#TMP_IsCAT') IS NOT NULL DROP TABLE #TMP_IsCAT
IF OBJECT_ID('tempdb..#TMP_IsTitleDirect') IS NOT NULL DROP TABLE #TMP_IsTitleDirect
IF OBJECT_ID('tempdb..#TMP_VirtualBranch_Transfer') IS NOT NULL DROP TABLE #TMP_VirtualBranch_Transfer
IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Cancelled') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Cancelled
IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Completed') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Completed
IF OBJECT_ID('tempdb..#TMP_CATEventName') IS NOT NULL DROP TABLE  #TMP_CATEventName
IF OBJECT_ID('tempdb..#TMP_RowNumber') IS NOT NULL DROP TABLE #TMP_RowNumber
IF OBJECT_ID('tempdb..#TMP_ProviderMinBidAmount') IS NOT NULL DROP TABLE #TMP_ProviderMinBidAmount

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

		IF OBJECT_ID('tempdb..#TMP_Provider_Run_Salvage_ID') IS NOT NULL DROP TABLE #TMP_Provider_Run_Salvage_ID
		IF OBJECT_ID('tempdb..#TMP_Provider_Auction_Item') IS NOT NULL DROP TABLE #TMP_Provider_Auction_Item
		IF OBJECT_ID('tempdb..#TMP_Provider_Sale_Run_Counts') IS NOT NULL DROP TABLE #TMP_Provider_Sale_Run_Counts
		IF OBJECT_ID('tempdb..#TMPAuctionStockID') IS NOT NULL DROP TABLE #TMPAuctionStockID
		IF OBJECT_ID('tempdb..#TMP_TitleProblemInd') IS NOT NULL DROP TABLE #TMP_TitleProblemInd
		IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
		IF OBJECT_ID('tempdb..#Temp_ReleaseProblemCount') IS NOT NULL DROP TABLE #Temp_ReleaseProblemCount
		IF OBJECT_ID('tempdb..#Temp_IsReleaseProblem') IS NOT NULL DROP TABLE #Temp_IsReleaseProblem
		IF OBJECT_ID('tempdb..#Temp_AllReleaseProblem') IS NOT NULL DROP TABLE #Temp_AllReleaseProblem
		IF OBJECT_ID('tempdb..#TMP_FactMinBidDetail') IS NOT NULL DROP TABLE #TMP_FactMinBidDetail
		IF OBJECT_ID('tempdb..#TMP_IsHadReleaseProblem') IS NOT NULL DROP TABLE #TMP_IsHadReleaseProblem
		IF Object_id('tempdb..#AuctionEntryDeduped') IS NOT NULL  DROP TABLE #AuctionEntryDeduped
		IF Object_id('tempdb..#TMPIBIDInfoFact') IS NOT NULL  DROP TABLE #TMPIBIDInfoFact
		IF Object_id('tempdb..#TMPProxyBidInfoFact') IS NOT NULL  DROP TABLE #TMPProxyBidInfoFact
		IF Object_id('tempdb..#TMPSealedBidInfoFact') IS NOT NULL  DROP TABLE #TMPSealedBidInfoFact
		IF Object_id('tempdb..#TMPLiveBidWonFact') IS NOT NULL  DROP TABLE #TMPLiveBidWonFact
		IF Object_id('tempdb..#TMPBidInds') IS NOT NULL  DROP TABLE #TMPBidInds
		IF OBJECT_ID('tempdb..#TMP_Provider_Auction_Item2') IS NOT NULL DROP TABLE #TMP_Provider_Auction_Item2
		IF OBJECT_ID('tempdb..#TMP_ReleaseProblemSalvage') IS NOT NULL DROP TABLE #TMP_ReleaseProblemSalvage
		IF OBJECT_ID('tempdb..#TMP_IsResidence') IS NOT NULL DROP TABLE #TMP_IsResidence
		IF OBJECT_ID('tempdb..#TMP_StockStatus_Salvage_ID') IS NOT NULL DROP TABLE #TMP_StockStatus_Salvage_ID
		IF OBJECT_ID('tempdb..#TMP_Sndoffer_Event') IS NOT NULL DROP TABLE #TMP_Sndoffer_Event
		IF OBJECT_ID('tempdb..#TMP_Sndoffer') IS NOT NULL DROP TABLE #TMP_Sndoffer
		IF OBJECT_ID('tempdb..#TMP_BRG') IS NOT NULL DROP TABLE #TMP_BRG
		IF OBJECT_ID('tempdb..#TMP_ST') IS NOT NULL DROP TABLE #TMP_ST
		IF OBJECT_ID('tempdb..#Temp_StockAuthorizedPay') IS NOT NULL DROP TABLE #Temp_StockAuthorizedPay
		IF OBJECT_ID('tempdb..#INSStocks') IS NOT NULL DROP TABLE #INSStocks
		IF OBJECT_ID('tempdb..#TMP_FactMinBidDetail') IS NOT NULL DROP TABLE #TMP_FactMinBidDetail
		IF OBJECT_ID('tempdb..#TMP_StockStatus_Salvage_ID') IS NOT NULL DROP TABLE #TMP_StockStatus_Salvage_ID
		IF OBJECT_ID('tempdb..#Temp_StockAuthorizedPay') IS NOT NULL DROP TABLE #Temp_StockAuthorizedPay      
		IF OBJECT_ID('tempdb..#TMP_IsCAT') IS NOT NULL DROP TABLE #TMP_IsCAT  
		IF OBJECT_ID('tempdb..#TMP_IsTitleDirect') IS NOT NULL DROP TABLE #TMP_IsTitleDirect
		IF OBJECT_ID('tempdb..#TMP_VirtualBranch_Transfer') IS NOT NULL DROP TABLE #TMP_VirtualBranch_Transfer
        IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Cancelled') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Cancelled
        IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Completed') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Completed
        IF OBJECT_ID('tempdb..#TMP_CATEventName') IS NOT NULL DROP TABLE  #TMP_CATEventName
		IF OBJECT_ID('tempdb..#TMP_InspectionServices_Reports_Completed') IS NOT NULL DROP TABLE #TMP_InspectionServices_Reports_Completed
        IF OBJECT_ID('tempdb..#TMP_InspectionServices_Reports_Cancelled') IS NOT NULL DROP TABLE #TMP_InspectionServices_Reports_Cancelled
		IF OBJECT_ID('tempdb..#TMP_RowNumber') IS NOT NULL DROP TABLE #TMP_RowNumber
		IF OBJECT_ID('tempdb..#TMP_ProviderMinBidAmount') IS NOT NULL DROP TABLE #TMP_ProviderMinBidAmount
		IF OBJECT_ID('tempdb..#TMP_IsCAT') IS NOT NULL DROP TABLE #TMP_IsCAT
		IF OBJECT_ID('tempdb..#TMP_IsLACAT') IS NOT NULL DROP TABLE #TMP_IsLACAT
		IF OBJECT_ID('tempdb..#TMP_IsHurricaneCAT') IS NOT NULL DROP TABLE #TMP_IsHurricaneCAT
        IF OBJECT_ID('tempdb..#TMP_CATStocks') IS NOT NULL DROP TABLE #TMP_CATStocks
		
		RETURN -1
	
END CATCH







GO


