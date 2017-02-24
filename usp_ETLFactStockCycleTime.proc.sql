

--/* 
--USE EDW
--GO 
--==============================================================================
--Author      : Venkata Vempali
--Create date : 11/14/2011
--Description : Populate the EDW FactStockCycleTime table from CDC   
--==============================================================================
--REVISION History
--ChangeDate  Developer         Release/Problem Number 
--12/1/2011	Matt Stelter	  Corrected Earliest sale date logic
--12/16/1011     VV             Added Durations. 
--12/22/2011	Matt Stelter	  Corrected Buyer Payment to Pickup
--12/22/2011     VV             Updated as per DBA's code review. WO - 22341 
--01/11/2012	Matt Stelter	  Added code for IBF Dates
--01/17/2012     VV             Added ISNULL to temp tables
--01/19/2012  somesh v		  Added Anticipated sale date
--01/19/2012     VV             Added BusinessLossToPickupTheftRule & CalendarLossToPickupTheftRule	
--01/23/2012	somesh v          Changed source for logout,login,saledoc received,certificate received,TitleIn
--01/25/2012  somesh v		  Changed calculation for CalendarAssignmenttosold
--01/31/2012  somesh v		  Changed calculation for Calendar and Business Hold Time	
--02/03/2012      VV            Dropped CurrentStockStatusStartDate ,CurrentStockStatusStartDayID
--02/05/2012      VV            --Error Fix. Alias name for #TMP_CT_PR from PR to RP.
--02/06/2012  somesh v          Updated COALESCE Stmt of BusinessHoldTime & CalendarHoldTime to Null from 0
--02/06/2012	Matt S.			  Correct SMVD to use Cert In Date
--02/06/2012  somesh v	      Update Placed on hold date to use HDCF & HLCO event code and changed HDCF for holdtime
--02/07/2012	Matt Stelter	  Added code for IBF Duration
--02/07/2012  somesh v          Update Hold release Date time to use Case instead of Coalesce
--02/08/2012     VV             Deleted/Commented Code that is not used. For Reference check TFS $/BI/EDW/Phase2/Release Objects. 
--02/13/2012	Matt S.			  Correct Cert In Date
--02/15/2012  Somesh V          Changed the Logic for Hold Durations
--02/17/2012  Somesh V          Commented Placed on Hold Date
--02/17/2012	Sidd M			  Added [DaysInPhysicalInventory] and [DaysInPipelineInventory]
--02/22/2012	Somesh V          Changed logic for Anticipated Sale Date & Added more logic for Hold Time
--02/27/2012  Somesh V          Update Anticipated_Sale_Date as 2999-12-31 for all cancelled stocks 
--03/12/2012	Matt Stelter	  Added code for IBF Duration
--03/17/2012  Sidd Murao        Added new logic for Days in Physical Inventory
--03/30/2012  Somesh V		  Added additional logic for TitleInDate
--4/2/2012	Matt Stelter	  Added WHERE [__$operation] = 5 to CDC
--4/2/2012	Matt Stelter	  Added check for invalid dates
--4/04/2012   Somesh V		  Changed Salvage_title to Salvage_Info for TitleIn
--5/1/2012    Sidd Murao		  Added Original branch Change and Original Provider Transfer Dates.
--5/2/2012    Somesh V          Additional Logic for TitleProblemResolvedDate
--5/3/2012    Sidd Murao        Changed the logic for Original Branch Change Date
--5/7/2012    Sidd Murao        Logic to update Original Date once
--5/7/2012      Venkata V       Updated logic for FirstSoldDateime R5.3 S14
--5/8/2012	Matt Stelter	  Added Auction Date R5.3 S14
--5/9/2012    Sidd Murao        Added Logic For multiple Branch Events on same day
--5/10/2012      VV             Updated the Dates - #TMP_FactStockCycleTime_Stage 
--5/10/2012	Matt Stelter	  Added Auction Duration R5.4 S15
--5/15/2012   Somesh V          Added Advance Storage dates in Update Statement
--5/21/2012   Somesh V          Updated logic for Title Problem Resolved Date
--6/26/2012   Sidd Murao        Updated logic for Original Branch Transfer
--6/28/2012   Sidd Murao        Updated logic for Original Branch Change Date
--7/06/2012   Venkata Vempali   Updated logic for BusinessLosstoAssignment & CalendarLosstoAssignment S19 R5.5 
--7/06/2012   Venkata Vempali   Kept old logic for BusinessLosstoAssignment & CalendarLosstoAssignment R5.5
--7/16/2012   Venkata Vempali   Updated logic for BusinessLosstoAssignment & CalendarLosstoAssignment S19 R5.5 
--7/20/2012	Juan X Jacome	  Update logic for AuctionDate and OriginalAuctionDate
--8/08/2012   Somesh Velupalli  Added CDC tables to Anticipated auction date
--8/13/2012	Matt Stelter		Changed logic for Effective Days to use timezoned dates
--10/7/2012   Preetham Duvva	  Added code for ProviderSoldDate and updated date for settlement voids after 
--                              final settlement
--10/8/2012   Somesh Velupalli  Updated logic for Settlement date to be null if settlement has been voided      
--10/8/2012   Preetham Duvva    Added logic to consider ProviderSoldDate NULL for cancelled stocks,title only and tow only AssignmentType    
--10/8/2012   Venkata Vempali   Updated logic for Settlement date.
--10/30/2012  Preetham Duvva    Added provider sold date related durations
--11/6/2012   Preetham Duvva    Updated the logic for inventory provider durations
--11/19/2012  Preetham Duvva    Added Tower Kiosk DropOffDateTime and durations
--11/26/2012  Preetham Duvva    Updated the source for Tower Kiosk DropOffDateTime
--12/10/2012  Juan X. Jacome	  Added AdvanceChargePrepaidThruDateTime
--12/12/2012  Preetham Duvva    Added date validation check for Tower Kiosk DropOffDate
--12/21/2012  Preetham Duvva    Modified the calculation of Tower Kiosk DropOffDateTime
--09/13/2013  John Stires		  Modified to update the DropOf Dates and times for all and just Tower Kiosk
--									Also added the Time Zoned Drop Off Date and ID Fields
--09/26/2013	John Stires		  Added code to restore missing CertInDateTimes
--10/18/2013	Matt Stelter		Changed CertIn logic
--12/10/2013	Matt Stelter		Added BuyerFinanceDatetime
--1/14/2014	Matt Stelter		Restore missing CertInDateTimes	
--1/27/2014	Matt Stelter		Change Assignment Date to use ASGN Event - DW Merge
--1/27/2014	Matt Stelter		Change Buyer Pickup Date to use BYPU Event - DW Merge
--1/27/2014	Matt Stelter		Change Buyer Paid Date to use BYPD Event - DW Merge
--1/27/2014	Matt Stelter		Change Provider Quoted Release Date to use 'RLQT', 'RLQP' Events - DW Merge
--1/31/2014	Matt Stelter		Change Dispatch Date to use 'DPAS', 'DPDP' Events - DW Merge
--2/21/2014   Bruce Kutnick		Changed Inspection Date to use Salvage Event Codes 'AINS', 'VTRN' - DW Merge
--2/25/2014   Bruce Kutnick       Changed Pickup Date to use CSA Today Logic (use Salvage Event Code From Salvae_Event_History table)
--2/25/2014	Bruce Kutnick		Changed Auction Date to use CSA Today Logic
--2/26/2014   Bruce Kutnick	    Reverted back to earlier code for Pickup Date as Durations Coding for 1x has not been completed
--2/27/2014	Matt Stelter		Fixed AuctionDateTime
--2/28/2014	MAtt Stelter		Reverted Buyer Pickup
--3/19/2014   Bruce Kutnick	    Modified Buyer Pickup to use CSA Today logic.
--3/24/2014   Bruce Kutnick		Modified Dispatch DateTime to use CSA Today data source and logic.
--3/26/2014   Bruce Kutnick	    Roll back change to Buyer Pickup Date, did not allow full test cycle in QA
--3/26/2014	Bruce Kutnick		Roll back change to Dispatch Date, as using DimStockEvent logic is identical to ASAP, keep the change from 1/31/2014
--3/26/2014	Bruce Kutnick		Roll back change to Inspection Date Time
--3/27/2014	Matt Stelter		Fix to correct invalid Auction Dates
--4/3/2014	Matt Stelter		Auction Dates change for DW Merge
--6/3/2014	Matt Stelter		NMVTIS Dates
--10/2/2014	Matt Stelter		Added HD Dates
--10/7/2014	Matt Stelter		Changed Buyer Pickup Date to use BYPU Event - DW Merge,Pickup Date to use CSA Today Logic (use Salvage Event Code From Salvae_Event_History table)
--10/31/2014  Bruce Kutnick		Modified LossReportedDateTime (Loss_Key) to mirror CSA Today logic.  It will now take the latest non null value for Recovery or if this is null Loss. (Insert into #TMP_SEH_DATES)
--1/27/2015	Matt Stelter		Modifications to increase performance
--2/3/2015	Matt Stelter		Changed BusinessDaysReleaseToPickup, EffectiveDaysReleaseToPickup, BusinessDaysReleaseToDropOff, EffectiveDaysReleaseToDropoff
--4/8/2015	Matt Stelter		Added TimeZonedStockFirstDropOffDateTime
--4/22/2015	Matt Stelter		Added TimeZonedInspectionServicesCompletedDateTime
--5/29/2015   Yogitha Alwarsetty  Added MinSMVDDatetime
--5/29/2015   Yogitha Alwarsetty  Added MinSMVDDayID
--5/29/2015   Yogitha Alwarsetty  Added Selling Branch Received Forwarded Title Docs
--6/9/2015	  Yogitha Alwarsetty  Added First Release Date & First All Title Documents Received Date
--6/22/2015		Matt Stelter	Changed First Release Date to use Regular release date when first populated then never updated again
--7/27/2015   Yogitha Alwarsetty Updated the logic for  AllTitleDocumentsRecievedDatetime 
--8/5/2015		Matt Stelter	Changed logic for Earliest Sale Date
--9/11/2015   Pratyusha Koduru  Changed logic for TimeZonedInspectionServicesCompletedDateTime to include note 'Inspection Services Report enhancement marked complete' also(these changes are removed on 9/24 due to enhanced User Story)
--9/15/2015   Pratyusha Koduru  Updated Logic to show TimeZonedInspectionServicesCompletedDateTime default if InspectionServicePhotos is Cancelled
--9/24/2015	 Pratyusha Koduru	Added TimeZonedInspectionServicesReportsCompletedDateTime
--9/24/2015  Pratyusha Koduru   Updated Logic to show TimeZonedInspectionServicesReportsCompletedDateTime default if InspectionServiceReports is Cancelled
--11/11/2015 Pratyusha Koduru   Added PartialDocumentsReceivedDateTime
--11/23/2015 Pratyusha Koduru   Changed BusinessAssignmentToTitleIn, BusinessAssignmentToTitleIn, BusinessPickuptoTitleIn, CalendarPickuptoTitleIn, BusinessTitleIntoSMVD, CalendarTitleIntoSMVD to use AllTitleDocumentsRecievedDateTime instead of TitleInDateTime
--2/1/2016		Matt Stelter	Changed logic to use VIMG insteat of CKI2
--3/23/2016		Matt Stelter	Change for Inspection Services to use Level 3
--4/12/2016		Matt Stelter	Update MinSMVDDatetime if > SMVDDatetime
--4/21/2016 Yogitha Alwarsetty  Added DateTimeReportedtoNICB
--5/9/2016  Pratyusha Koduru    Added CalendarCertInToTitleApprovedByProvider,BusinessCertInToTitleApprovedByProvider,CalendarTitleApprovedByProviderToSoldProvider,BusinessTitleApprovedByProviderToSoldProvider,CalendarTitleApprovedByProviderToSold,BusinessTitleApprovedByProviderToSold  
--5/16/2016 Pratyusha Koduru    Updated TimeZonedInspectionServicesCompletedDateTime,TimeZonedInspectionServicesReportsCompletedDateTime and Added TimeZonedInspectionServicesAppraisalDateTime
--5/16/2016 Pratyusha Koduru    Updated InspectionServicesFlippedDateTime With new logic to use Notes
--6/6/2016  Pratyusha Koduru    Added TitleProcessingSLAFromDateTime and TitleProcessingSLAToDateTime
--6/16/2016 Yogitha Alwarsetty  Added [PartialDocAwaitingTitleDirectForm],[PartialDocLienholderDocsRcvd] and [PartialDocOwnerDocsRcvd]
--7/15/2016 Yogitha Alwarsetty  Added LastTitleAssistFormUploadDateTime
--7/21/2016 Pratyusha Koduru    Updated TitleProcessingSLA Dates
--12/19/2016 Yogitha Alwarsetty  Added BusinessDaysInspectionServices
--==============================================================================
--Usage Example:

--done
--==============================================================================
--*/
Create PROCEDURE [dbo].[usp_ETLFactStockCycleTime]
AS
  BEGIN

	  DECLARE @ExtractRowCount_sp INT,
			  @ETLLoadID_sp       INT,
			  @InsertRowCount_sp  INT,
			  @UpdateRowCount_sp  INT,
			  @MinLSN             BINARY(10),
			  @MaxLSN             BINARY (10),
			  @currentDateTime	  DateTime,
			  @MinAllowableDate		Datetime,
			  @MaxAllowableDate		Datetime,
			  @MinAllowableDayId	INT,
			  @MaxAllowableDayID	INT

	  SET NOCOUNT ON;

	  BEGIN TRY
		  --Get the Min and Max LSN's
		  SELECT @MinLSN=MinLSN,
				 @MaxLSN=MaxLSN
		  FROM   dbo.ETLRun WITH (NOLOCK)
		  WHERE  ETLJobName='DailyETLJob'

		SELECT @MinAllowableDate = MIN(DayDate)  
			   ,@MinAllowableDayId = (CONVERT(INT, CONVERT(CHAR(8), MIN(DayDate), 112)))
			   ,@MaxAllowableDate = MAX(DayDate)
			   ,@MaxAllowableDayID =  (CONVERT(INT, CONVERT(CHAR(8), MAX(DayDate), 112)))
		FROM DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231

	  /*****************START CREATE TEMP TABLES********************/
		  --IF OBJECT_ID('tempdb..#TMP_CT_Salvage_Info') IS NOT NULL DROP TABLE #TMP_CT_Salvage_Info
		  CREATE TABLE #TMP_CT_Salvage_Info
			([Salvage_ID]                              [INT] NOT NULL,
			 [Assignment_DateTime]                     [DATETIME] NULL,
			 [Assignment_Day_ID]                       [INT] NULL,
			 [Release_DateTime]                        [DATETIME] NULL,
			 [Release_Day_ID]                          [INT] NULL,
			 [Checkin_DateTime]                        [DATETIME] NULL,
			 [Checkin_Day_ID]                          [INT] NULL,
			 [Buyer_Payment_DateTime]                  [DATETIME] NULL,
			 [Buyer_Payment_Day_ID]                    [INT] NULL,
			 [Received_All_Documents_DateTime]         [DATETIME] NULL,
			 [Received_All_Documents_Day_ID]           [INT] NULL,
			 [Missing_Parts_Datetime]                  [DATETIME] NULL,
			 [Missing_Parts_Day_ID]                    [INT] NULL,
			 [Dispatch_Permanently_Cancelled_DateTime] [DATETIME] NULL,
			 [Dispatch_Permanently_Cancelled_DayID]    [INT] NULL,
			 [Hold_Release_Date_Time]                  [DATETIME] NULL,
			 [Hold_Release_Day_ID]                     [INT] NULL,
			 [PreBill_Paid_Date]                       [DATETIME] NULL,
			 [Prebill_day_ID]                          [INT] NULL,
			 [Sold_Date_Time]                          [DATETIME] NULL,
			 [Sold_Day_ID]                             [INT] NULL)

	  /*****************START CREATE TEMP TABLES********************/
		  --IF OBJECT_ID('tempdb..#TMP_CT_Salvage_Provider_Info') IS NOT NULL DROP TABLE #TMP_CT_Salvage_Provider_Info
		  CREATE TABLE #TMP_CT_Salvage_Provider_Info
			([STOCKID]                             [INT] NOT NULL,
			 [Update_DateTime]                        [DATETIME] NULL,
			 [ProviderID]                    [INT] NOT NULL )

			  CREATE TABLE #TMP_CT_Salavage_Event_REAB_IBTR_IBND
			([STOCKID]                             [INT] NOT NULL,
			 [Event_DateTime]                        [DATETIME] NULL
		)

				 CREATE TABLE #TMP_CT_Salavage_Event_Checkin
			([STOCKID]                             [INT] NOT NULL,
			 [Checkin_Date]                        [DATETIME] NULL
		)

		        CREATE TABLE #TMP_CT_Salavage_Event_Checkin_Future
			([STOCKID]                             [INT] NOT NULL,
			 [Checkin_Date_Fut]                        [DATETIME] NULL
		)

			CREATE TABLE #TMP_CT_Salavage_Event_Checkin_Future_MaxDate
			([STOCKID]                             [INT] NOT NULL,
			 [Checkin_Date_Fut_MaxDate]            [DATETIME] NULL
		)

 CREATE TABLE #TMP_CT_Salavage_Event_REAB
([StockID] INT NOT NULL ,
 [Event_DateTime] [DATETIME] NULL )

		  --IF OBJECT_ID('tempdb..#TMP_CT_Dispatch_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Dispatch_DateTime
		  CREATE TABLE #TMP_CT_Dispatch_DateTime
			([Salvage_ID]        [INT] NOT NULL,
			 [Dispatch_DateTime] [DATETIME] NULL,
			 [Dispatch_Day_ID]   [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_Pickup_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Pickup_DateTime
		  CREATE TABLE #TMP_CT_Pickup_DateTime
			([Salvage_ID]      [INT] NOT NULL,
			 [Pickup_DateTime] [DATETIME] NULL,
			 [Pickup_Day_ID]   [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_Settlement_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_Settlement_SalvageIDs
		  CREATE TABLE #TMP_CT_Settlement_SalvageIDs
			([Salvage_ID] [INT] NOT NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_Settlement_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Settlement_DateTime
		  CREATE TABLE #TMP_CT_Settlement_DateTime
			([Salvage_ID]          [INT] NOT NULL,
			 [Settlement_DateTime] [DATETIME] NULL,
			 [Settlement_Day_ID]   [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_Earliest_Sale_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Earliest_Sale_DateTime
		  CREATE TABLE #TMP_CT_Earliest_Sale_DateTime
			([Salvage_ID]             [INT] NOT NULL,
			 [Earliest_Sale_DateTime] [DATETIME] NULL,
			 [Earliest_Sale_Day_ID]   [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_SEH_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SEH_SalvageIDs
		  CREATE TABLE #TMP_CT_SEH_SalvageIDs
			([Salvage_ID] [INT] NOT NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_SEH_Dates') IS NOT NULL DROP TABLE #TMP_CT_SEH_Dates
		  CREATE TABLE #TMP_CT_SEH_Dates
			([Salvage_ID]                          [INT] NOT NULL,
			 [Advance_Storage_Verified_Date]       [DATETIME] NULL,
			 [Date_Of_Loss_Reported_DateTime]      [DATETIME] NULL,
			 [Inspection_DateTime]                 [DATETIME] NULL,
			 [Provider_Quoted_Release_DateTime]    [DATETIME] NULL,
			 [Recovery_Datetime]                   [DATETIME] NULL,
			 [Settlement_Paid_DateTime]            [DATETIME] NULL,
			 [VIC_Inspection_DateTime]             [DATETIME] NULL,
			 [Cancel_DateTime]                     [DATETIME] NULL,
			 [First_Sold_DateTime]                 [DATETIME] NULL,
			 [Loss_DateTime]                       [DATETIME] NULL,
			 [Sales_Document_Applied_Datetime]     [DATETIME] NULL,
			 [Title_Approved_By_Provider_DateTime] [DATETIME] NULL,
			 [HDCF_Event_Datetime]                 [DATETIME] NULL
			 --,[Max_Hold_Release_Datetime] [datetime] NULL
			 ,
			 [Release_Problem_Identified_Datetime] [DATETIME] NULL,
			 [Release_Problem_Resolved_Datetime]   [DATETIME] NULL,
			 [Title_Problem_Identified_Datetime]   [DATETIME] NULL,
			 [Title_Problem_Resolved_Datetime]     [DATETIME] NULL,
			 [Vehicle_Images_Loaded_Datetime]      [DATETIME] NULL,
			 [Check_In_Images_Taken_Datetime]      [DATETIME] NULL,
			 [Buyer_Renege_Cancel_Sale_Datetime]   [DATETIME] NULL,
			 [Check_Issued_Datetime]               [DATETIME] NULL,
			 [Placed_On_Hold_Datetime]             [DATETIME] NULL
			 --,[On_Hold_Release_Datetime] [datetime] NULL
			 ,
			 [Hold_Time_Business_Hours]            [DECIMAL](18, 9) NULL,
			 [Hold_Time_Calendar]                  [DECIMAL](18, 9) NULL,
			 [Log_Out_DateTime]                    [DATETIME] NULL,
			 [Log_In_Datetime]                     [DATETIME] NULL,
			 [Title_Received_Datetime]             [DATETIME] NULL,
			 [Sale_Document_Received_Datetime]     [DATETIME] NULL,
			 [Certificate_Received_Datetime]       [DATETIME] NULL,
			 [Titlework_Received_Datetime]         [DATETIME] NULL,
			 [SMVD_Datetime]                       [DATETIME] NULL,
		
			 [HDRM_Event_Date]                     [DATETIME] NULL,
			 [ASCN_Event_Date]                     [DATETIME] NULL,
			 [HDCN_Event_Date]                     [DATETIME] NULL,
			 [HDCR_Event_Date]                     [DATETIME] NULL,
			 [Hold_Release_Date]                   [DATETIME] NULL,
			 [Placed_On_Hold_Date_Duration]        [DATETIME] NULL,
			 Sale_Doc_Removed_Date                 [DATETIME] NULL,
			 Titlework_Removed_Date                [DATETIME] NULL,
			 Certificate_Removed_Date              [DATETIME] NULL,
			 All_Title_Documents_Received_Date     [DATETIME] NULL,
			 All_Title_Documents_Recieved_Date_Reset_No [Datetime] NULL,
			 Stock_Closed_Date_Time                [DATETIME] NULL,
			 [ASGN_Event_Date]                     [DATETIME] NULL,
			 [BYPD_Event_Date]                     [DATETIME] NULL,
			 FinalNMVTISDate                       [DATETIME] NULL,
			 InitialNMVTISDate                     [DATETIME] NULL,
			 Selling_Branch_Received_Forwarded_Title_Docs  [DATETIME] NULL,
			  DateTimeReportedtoNICB               [DATETIME] NULL,
              [PartialDocAwaitingTitleDirectForm]   [DATETIME] NULL,
              [PartialDocLienholderDocsRcvd]       [DATETIME] NULL,
              [PartialDocOwnerDocsRcvd]             [DATETIME] NULL
			 )

       --IF OBJECT_ID('tempdb..#TMP_CT_SEH_Dates_Min') IS NOT NULL DROP TABLE #TMP_CT_SEH_Dates_Min
 
		 CREATE TABLE #TMP_CT_SEH_Dates_Min
			([Salvage_ID]                          [INT] NOT NULL,
			Min_Sales_Document_Applied_Datetime    [DATETIME] NULL,
            --Min_Title_Received_Datetime            [DATETIME] NULL,
            --Min_Certificate_Received_Datetime      [DATETIME] NULL,
            --Min_Sale_Document_Received_Datetime    [DATETIME] NULL,
            --Min_Sale_Doc_Removed_Date              [DATETIME] NULL,
            --Min_Titlework_Removed_Date             [DATETIME] NULL,
            --Min_Certificate_Removed_Date           [DATETIME] NULL,
            --Min_Titlework_Received_Datetime	       [DATETIME] NULL,
            Min_SMVD_Datetime                      [DATETIME] NULL,
            First_Release_DateTime                 [DATETIME] NULL , 
            First_All_Title_Documents_Received_DateTime    [DATETIME] NULL  )


		  --IF OBJECT_ID('tempdb..#TMP_CT_Cert_In_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Cert_In_DateTime
		  
		  CREATE TABLE #TMP_CT_Cert_In_DateTime
			(Salvage_ID       [INT] NOT NULL,
			 Cert_In_DateTime [DATETIME] NULL,
			 Stock_Event_Code [CHAR](4) NOT NULL)
		 
		 --IF OBJECT_ID('tempdb..#TMP_CT_Cert_In_DateTime_Min') IS NOT NULL DROP TABLE #TMP_CT_Cert_In_DateTime_Min
			
		  CREATE TABLE #TMP_CT_Cert_In_DateTime_Min
			(Salvage_ID       [INT] NOT NULL,
			 Min_Cert_In_DateTime [DATETIME] NULL,
			 Min_Stock_Event_Code [CHAR](4) NOT NULL
			)
	 

		  --IF OBJECT_ID('tempdb..#TMP_CT_dups') IS NOT NULL DROP TABLE #TMP_CT_dups
		  CREATE TABLE #TMP_CT_dups
			(Salvage_ID       [INT] NOT NULL,
			 Stock_Event_Code [CHAR](4) NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_BuyerPickup') IS NOT NULL DROP TABLE #TMP_CT_BuyerPickup
		  CREATE TABLE #TMP_CT_BuyerPickup
			([Salvage_ID]            [INT] NOT NULL,
			 [Buyer_Pickup_DateTime] [DATETIME] NULL,
			 [Buyer_Pickup_Day_ID]   [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_Provider_Quoted_Release') IS NOT NULL DROP TABLE #TMP_CT_Provider_Quoted_Release
		  CREATE TABLE #TMP_CT_Provider_Quoted_Release
			([Salvage_ID]            [INT] NOT NULL,
			 [Provider_Quoted_Release_DateTime] [DATETIME] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_Provider_Refund') IS NOT NULL DROP TABLE #TMP_CT_Provider_Refund
		  CREATE TABLE #TMP_CT_Provider_Refund
			([Salvage_ID]                  [INT] NOT NULL,
			 [Last_Provider_Refund_Date]   [DATETIME] NULL,
			 [Last_Provider_Refund_Day_ID] [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_CPSD_SalvageIDs') IS NOT NULL DROP TABLE tempdb..#TMP_CT_CPSD_SalvageIDs
		  CREATE TABLE #TMP_CT_CPSD_SalvageIDs
			(Salvage_ID [INT] NOT NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_CPSS_Datetime') IS NOT NULL DROP TABLE tempdb..#TMP_CT_CPSS_Datetime
		  CREATE TABLE #TMP_CT_CPSS_Datetime
			(Salvage_ID                          [INT] NOT NULL,
			 Current_Process_Status_Start_Date   [DATETIME] NULL,
			 Current_Process_Status_Start_Day_ID [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_SMVD') IS NOT NULL DROP TABLE tempdb..#TMP_CT_SMVD
		  CREATE TABLE #TMP_CT_SMVD
			(StockID                     [INT] NOT NULL,
			 Salvage_ID                  [INT] NOT NULL,
			 Title_Received_Date         [DATETIME] NULL,
			 Title_Received_Day_ID       [INT] NULL,
			 Certificate_Received_Date   [DATETIME] NULL,
			 Certificate_Received_Day_ID [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_SMVD_Datetime') IS NOT NULL DROP TABLE tempdb..#TMP_CT_SMVD_Datetime
		  CREATE TABLE #TMP_CT_SMVD_Datetime
			(Salvage_ID   [INT] NOT NULL,
			 StockID      [INT] NOT NULL,
			 SMVDDatetime [DATETIME] NULL,
			 SMVDDayID    [INT] NULL)
				  
	    --IF OBJECT_ID('tempdb..#TMP_CT_SMVD_Datetime_Min') IS NOT NULL DROP TABLE tempdb..#TMP_CT_SMVD_Datetime_Min
	 
			 
			 
			 CREATE TABLE #TMP_CT_SMVD_Datetime_Min
			(Salvage_ID   [INT] NOT NULL,
			 StockID      [INT] NOT NULL,
			 MinSMVDDatetime [DATETIME] NULL,
			 MinSMVDDayID    [INT] NULL,
			 FirstReleaseDateTime [DATETIME] NULL,                  
            FirstAllTitleDocumentsReceivedDateTime [DATETIME] NULL) 

		  --IF OBJECT_ID('tempdb..#TMP_CT_ASI') IS NOT NULL DROP TABLE #TMP_CT_ASI
		  CREATE TABLE #TMP_CT_ASI
			([Salvage_ID]   [INT] NOT NULL,
			 [Start_Date]   [DATETIME] NULL,
			 [Start_Day_ID] [INT] NULL,
			 [End_Date]     [DATETIME] NULL,
			 [End_Day_ID]   [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_ASD') IS NOT NULL DROP TABLE #TMP_CT_ASD
		  CREATE TABLE #TMP_CT_ASD
			([Salvage_ID]              [INT] NOT NULL,
			 [Sold_Datetime]           [DATETIME] NULL,
			 [Auction_Datetime]        [DATETIME] NULL,
			 [Salvage_TitleDate]       [DATETIME] NULL,
			 [Anticipated_Sale_Date]   [DATETIME] NULL)
			 
		 CREATE TABLE #TMP_Anticipated_SalvageIDs
		     ([Salvage_ID]INT NOT NULL)

		  --IF OBJECT_ID('tempdb..#TMP_CT_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SalvageIDs
		  CREATE TABLE #TMP_CT_SalvageIDs
			([Salvage_ID] [INT] NOT NULL)

		  --IF OBJECT_ID('tempdb..#TMP_FactStockCycleTime_Stage') IS NOT NULL DROP TABLE #TMP_FactStockCycleTime_Stage
		  CREATE TABLE #TMP_FactStockCycleTime_Stage
			([StockID]                            [INT] NOT NULL,
			 ASAPSalvageID                        [INT] NOT NULL,
			 AdministrativeBranchNumber           [INT] NOT NULL,
			 AssignmentDateTime                   [DATETIME] NULL,
			 AssignmentDayID                      [INT] NULL,
			 ReleaseDateTime                      [DATETIME] NULL,
			 ReleaseDayID                         [INT] NULL,
			 CheckInDateTime                      [DATETIME] NULL,
			 CheckinDayID                         [INT] NULL,
			 LogInDateTime                        [DATETIME] NULL,
			 LogInDayID                           [INT] NULL,
			 LogOutDateTime                       [DATETIME] NULL,
			 LogOutDayID                          [INT] NULL,
			 BuyerPaymentDateTime                 [DATETIME] NULL,
			 BuyerPaymentDayID                    [INT] NULL,
			 ReceivedAllDocumentsDateTime         [DATETIME] NULL,
			 ReceivedAllDocumentsDayID            [INT] NULL,
			 MissingPartsDatetime                 [DATETIME] NULL,
			 MissingPartsDayID                    [INT] NULL,
			 SaleDocumentReceivedDateTime         [DATETIME] NULL,
			 SaleDocumentReceivedDayID            [INT] NULL,
			 TitleInDateTime                      [DATETIME] NULL,
			 TitleInDayID                         [INT] NULL,
			 DispatchDateTime                     [DATETIME] NULL,
			 DispatchDayID                        [INT] NULL,
			 PickupDateTime                       [DATETIME] NULL,
			 PickupDayID                          [INT] NULL,
			 SettlementDateTime                   [DATETIME] NULL,
			 SettlementDayID                      [INT] NULL,
			 EarliestSaleDate                     [DATETIME] NULL,
			 EarliestSaleDayId                    [INT] NULL,
			 DispatchPermanentlyCancelledDateTime [DATETIME] NULL,
			 DispatchPermanentlyCancelledDayID    [INT] NULL,
			 HoldReleaseDateTime                  [DATETIME] NULL,
			 HoldReleaseDayID                     [INT] NULL,
			 PreBillPaidDateTime                  [DATETIME] NULL,
			 PreBillPaidDayID                     [INT] NULL,
			 SoldDateTime                         [DATETIME] NULL,
			 SoldDayId                            [INT] NULL,
			 AdvanceStorageVerifiedDate           [DATETIME] NULL,
			 AdvanceStorageVerifiedDayID          [INT] NULL,
			 DateOfLossReportedDateTime           [DATETIME] NULL,
			 DateOfLossReportedDayID              [INT] NULL,
			 InspectionDateTime                   [DATETIME] NULL,
			 InspectionDayID                      [INT] NULL,
			 ProviderQuotedReleaseDateTime        [DATETIME] NULL,
			 ProviderQuotedReleaseDayID           [INT] NULL,
			 RecoveryDatetime                     [DATETIME] NULL,
			 RecoveryDayID                        [INT] NULL,
			 SettlementPaidDateTime               [DATETIME] NULL,
			 SettlementPaidDayID                  [INT] NULL,
			 VICInspectionDateTime                [DATETIME] NULL,
			 VICInspectionDayID                   [INT] NULL,
			 CancelDateTime                       [DATETIME] NULL,
			 CancelDayID                          [INT] NULL,
			 FirstSoldDateTime                    [DATETIME] NULL,
			 FirstSoldDayID                       [INT] NULL,
			 LossDateTime                         [DATETIME] NULL,
			 LossDayID                            [INT] NULL,
			 SalesDocumentAppliedDatetime         [DATETIME] NULL,
			 SalesDocumentAppliedDayID            [INT] NULL,
			   

			 TitleApprovedByProviderDateTime      [DATETIME] NULL,
			 TitleApprovedByProviderDayID         [INT] NULL,
			 CertINDateTime                       [DATETIME] NULL,
			 CertInDayID                          [INT] NULL,
			 BuyerPickupDateTime                  [DATETIME] NULL,
			 BuyerPickupDayID                     [INT] NULL,
			 LastProviderRefundDate               [DATETIME] NULL,
			 LastProviderRefundDayID              [INT] NULL,
			 CurrentProcessStatusStartDate        [DATETIME] NULL,
			 CurrentProcessStatusStartDayID       [INT] NULL
			 --,CurrentStockStatusStartDate [Datetime]	NULL
			 --,CurrentStockStatusStartDayID [int]	NULL
			 ,
			 SMVDDatetime                         [DATETIME] NULL,
			 SMVDDayID                            [INT] NULL,
			
			 [ReleaseTimezonedDateTime]           [DATETIME] NULL,
			 [ReleaseTimeZonedDayId]              [INT] NULL,
			 [PickupTimezonedDateTime]            [DATETIME] NULL,
			 [PickupTimezonedDayID]               [INT] NULL,
			 BusinessHoldTime                     [DECIMAL](18, 9) NULL,
			 CalendarHoldTime                     [DECIMAL](18, 9) NULL,
			 ReleaseProblemIdentifiedDate         [DATETIME] NULL,
			 ReleaseProblemIdentifiedDayID        [INT] NULL,
			 ReleaseProblemResolvedDate           [DATETIME] NULL,
			 ReleaseProblemResolvedDayID          [INT] NULL,
			 TitleProblemIdentifiedDate           [DATETIME] NULL,
			 TitleProblemIdentifiedDayID          [INT] NULL,
			 TitleProblemResolvedDate             [DATETIME] NULL,
			 TitleProblemResolvedDayID            [INT] NULL,
			 VehicleImagesLoadedDate              [DATETIME] NULL,
			 VehicleImagesLoadedDayID             [INT] NULL,
			 CheckInImagesTakenDate               [DATETIME] NULL,
			 CheckInImagesTakenDayID              [INT] NULL,
			 BuyerRenegeCancelSaleDate            [DATETIME] NULL,
			 BuyerRenegeCancelSaleDayID           [INT] NULL,
			 CheckIssuedDate                      [DATETIME] NULL,
			 CheckIssuedDayID                     [INT] NULL,
			 PlacedOnHoldDate                     [DATETIME] NULL,
			 PlacedOnHoldDayID                    [INT] NULL
			 --,OnHoldReleaseDate [datetime] NULL
			 --,OnHoldReleaseDayID [int] NULL
			 ,
			 AdvanceStorageStartDate              [DATETIME] NULL,
			 AdvanceStorageStartDayID             [INT] NULL,
			 AdvanceStorageEndDate                [DATETIME] NULL,
			 AdvanceStorageEndDayID               [INT] NULL,
			 RefundPaymentDate                    [DATETIME] NULL,
			 RefundPaymentDayID                   [INT] NULL,
			 IBuyFastDisplayStartDate             [DATETIME] NULL,
			 IBuyFastDisplayStartDayID            [INT] NULL,
			 IBuyFastDisplayEndDate               [DATETIME] NULL,
			 IBuyFastDisplayEndDayID              [INT] NULL,
			 AnticipatedSaleDate                  [DATETIME] NULL,
			 AnticipatedSaleDayID                 [INT] NULL,
			 LossTypeCode                         [NCHAR](3) NULL,
			 IBNDuration                          [DECIMAL](18, 9) NULL,
			 AllTitleDocumentsRecievedDatetime    [DATETIME] NULL,
			 AllTitleDocumentsRecievedDayID       [INT] NULL, 
			 StockClosedDatetime                  [DATETIME] NULL,
			 StockClosedDayID                     [INT] NULL,
			 AuctionDateTime					  [DATETIME] NULL DEFAULT ('12-31-2999'),
			 AuctionDayID						  [INT] NULL DEFAULT (29991231),
			 OriginalAuctionDateTime			  [DATETIME] NULL DEFAULT ('12-31-2999'),
			 OriginalAuctionDayID				  [INT] NULL DEFAULT (29991231),
			 ProviderSoldDateTime                 [DATETIME] NULL DEFAULT ('12-31-2999'),
			 ProviderSoldDayID                    [INT] NULL DEFAULT (29991231),
			 FinalNMVTISDateTime					[DATETIME] NULL DEFAULT ('12-31-2999'),
			 FinalNMVTISDayID						INT NULL DEFAULT (29991231),
			 InitialNMVTISDateTime					[DATETIME] NULL DEFAULT ('12-31-2999'),
			 InitialNMVTISDayID						INT NULL DEFAULT (29991231),
			 MinSMVDDatetime                           [DATETIME] NULL,
			 MinSMVDDayID                              [INT] NULL,
			 SellingBranchReceivedForwardedTitleDocs   [DATETIME] NULL,
			 FirstReleaseDateTime     [DATETIME] NULL,
			 FirstAllTitleDocumentsReceivedDateTime    [DATETIME] NULL,
			  DateTimeReportedtoNICB               [DATETIME] NULL,
			  [PartialDocAwaitingTitleDirectForm]   [DATETIME] NULL,
              [PartialDocLienholderDocsRcvd]      [DATETIME] NULL,
               [PartialDocOwnerDocsRcvd]           [DATETIME] NULL,
			   [LastTitleAssistFormUploadDateTime] Datetime NULL
			  )

		  --IF OBJECT_ID('tempdb..#TMP_Release_Problem_DateTime_By_SalvageID') IS NOT NULL DROP TABLE #TMP_Release_Problem_DateTime_By_SalvageID
		  CREATE TABLE #TMP_Release_Problem_DateTime_By_SalvageID
			(Salvage_ID                     [INT] NOT NULL,
			 Release_Problem_Start_DateTime [DATETIME] NOT NULL,
			 Release_Problem_End_DateTime   [DATETIME] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_Release_Problem_Dates') IS NOT NULL DROP TABLE #TMP_Release_Problem_Dates
		  CREATE TABLE #TMP_Release_Problem_Dates
			(Salvage_ID                     [INT] NOT NULL,
			 Release_Problem_Start_DateTime [DATETIME] NULL,
			 Release_Problem_End_DateTime   [DATETIME] NULL,
			 Administrative_Branch_Number   [INT] NULL)

		  --IF OBJECT_ID('tempdb..#TMP_Release_Problem_Time') IS NOT NULL DROP TABLE #TMP_Release_Problem_Time
		  CREATE TABLE #TMP_Release_Problem_Time
			(Salvage_ID                    [INT] NOT NULL,
			 Release_Problem_Time_Business [DECIMAL](18, 9) NULL,
			 Release_Problem_Time_Calendar [DECIMAL](18, 9) NULL)

		  --IF OBJECT_ID('tempdb..#TMP_FSCTDuraions_Stage') IS NOT NULL DROP TABLE #TMP_FSCTDuraions_Stage
		  CREATE TABLE #TMP_FSCTDuraions_Stage
			([StockID]                       [INT] NOT NULL,
			 [ASAPSalvageID]                 [INT] NOT NULL,
			 [BusinessAssignmentToPickup]    [DECIMAL](18, 9) NULL,
			 [CalendarAssignmentToPickup]    [DECIMAL](18, 9) NULL,
			 [BusinessAssignmentToRelease]   [DECIMAL](18, 9) NULL,
			 [CalendarAssignmentToRelease]   [DECIMAL](18, 9) NULL,
			 [BusinessAssignmentToSold]      [DECIMAL](18, 9) NULL,
			 [CalendarAssignmentToSold]      [DECIMAL](18, 9) NULL,
			 [BusinessAssignmentToTitleIn]   [DECIMAL](18, 9) NULL,
			 [CalendarAssignmentToTitleIn]   [DECIMAL](18, 9) NULL,
			 [BusinessBuyerPaymentToPickup]  [DECIMAL](18, 9) NULL,
			 [CalendarBuyerPaymentToPickup]  [DECIMAL](18, 9) NULL,
			 [BusinessCertInToSold]          [DECIMAL](18, 9) NULL,
			 [CalendarCertInToSold]          [DECIMAL](18, 9) NULL,
			 [BusinessFNOLToAssignment]      [DECIMAL](18, 9) NULL,
			 [CalendarFNOLToAssignment]      [DECIMAL](18, 9) NULL,
			 [BusinessLossToAssignment]      [DECIMAL](18, 9) NULL,
			 [CalendarLossToAssignment]      [DECIMAL](18, 9) NULL,
			 [BusinessLossToFNOL]            [DECIMAL](18, 9) NULL,
			 [CalendarLossToFNOL]            [DECIMAL](18, 9) NULL,
			 [BusinessLossToPickup]          [DECIMAL](18, 9) NULL,
			 [CalendarLossToPickup]          [DECIMAL](18, 9) NULL,
			 [BusinessPickupToTitleIn]       [DECIMAL](18, 9) NULL,
			 [CalendarPickupToTitleIn]       [DECIMAL](18, 9) NULL,
			 [BusinessReleaseToPickup]       [DECIMAL](18, 9) NULL,
			 [CalendarReleaseToPickup]       [DECIMAL](18, 9) NULL,
			 [BusinessSettledToPaid]         [DECIMAL](18, 9) NULL,
			 [CalendarSettledToPaid]         [DECIMAL](18, 9) NULL,
			 [BusinessSMVDToCertIn]          [DECIMAL](18, 9) NULL,
			 [CalendarSMVDToCertIn]          [DECIMAL](18, 9) NULL,
			 [BusinessSoldToBuyerPayment]    [DECIMAL](18, 9) NULL,
			 [CalendarSoldToBuyerPayment]    [DECIMAL](18, 9) NULL,
			 [BusinessTitleInToSMVD]         [DECIMAL](18, 9) NULL,
			 [CalendarTitleInToSMVD]         [DECIMAL](18, 9) NULL,
			 [BusinessTitleToSold]           [DECIMAL](18, 9) NULL,
			 [CalendarTitleToSold]           [DECIMAL](18, 9) NULL,
			 [BusinessAssignmenttoCancel]    [DECIMAL](18, 9) NULL,
			 [CalendarAssignmenttoCancel]    [DECIMAL](18, 9) NULL,
			 [BusinessSoldtoSettlement]      [DECIMAL](18, 9) NULL,
			 [CalendarSoldtoSettlement]      [DECIMAL](18, 9) NULL,
			 [BusinessReleaseProblemTime]    [DECIMAL](18, 9) NULL,
			 [CalendarReleaseProblemTime]    [DECIMAL](18, 9) NULL,
			 [BusinessPickupToCheckIn]       [DECIMAL](18, 9) NULL,
			 [CalendarPickuptoCheckIn]       [DECIMAL](18, 9) NULL,
			 [BusinessLogIntoCheckIn]        [DECIMAL](18, 9) NULL,
			 [CalendarLogIntoCheckIn]        [DECIMAL](18, 9) NULL,
			 [EffectiveDaysReleasetoPickup]  [DECIMAL](18, 9) NULL,
			 [BusinessLossToPickupTheftRule] [DECIMAL](18, 9) NULL,
			 [CalendarLossToPickupTheftRule] [DECIMAL](18, 9) NULL,
			 [BusinessCertInToAuction]		[DECIMAL](18, 9) NULL,
			 [CalendarCertInToAuction]		[DECIMAL](18, 9) NULL,
			 [BusinessAuctionToSettlement]	[DECIMAL](18, 9) NULL,
			 [CalendarAuctionToSettlement]	[DECIMAL](18, 9) NULL,
			 [CalendarCertInToSoldProvider]  [DECIMAL](18, 9) NULL,
			 [BusinessCertInToSoldProvider]  [DECIMAL](18, 9) NULL,
			 [CalendarSoldProviderToSettlement] [DECIMAL](18, 9) NULL,
			 [BusinessSoldProviderToSettlement] [DECIMAL](18, 9) NULL,
			 [CalendarAssignmentToSoldProvider] [DECIMAL](18, 9) NULL,
			 [BusinessAssignmentToSoldProvider] [DECIMAL](18, 9) NULL,
			 [CalendarSoldProviderToBuyerPayment] [DECIMAL](18, 9) NULL,
			 [BusinessSoldProviderToBuyerPayment] [DECIMAL](18, 9) NULL,
			 [DaysInPhysicalInventoryProvider] [DECIMAL](18, 9) NULL,
			 [DaysInPipelineInventoryProvider] [DECIMAL](18, 9) NULL)

		  --IF OBJECT_ID('tempdb..#temp_FSCTRecordCount') IS NOT NULL DROP TABLE #temp_FSCTRecordCount
		  CREATE TABLE #temp_FSCTRecordCount
			(ChangeType VARCHAR(30))

		  --IF OBJECT_ID('tempdb..#TMP_IBNStarted') IS NOT NULL DROP TABLE #TMP_IBNStarted
		  CREATE TABLE #TMP_IBNStarted
			(Salvage_ID      [INT] NOT NULL,
			 Update_DateTime [DATETIME] NULL,IBN_Run_Count INT NULL)

		  --IF OBJECT_ID('tempdb..#TMP_IBNEnded') IS NOT NULL DROP TABLE #TMP_IBNEnded
		  CREATE TABLE #TMP_IBNEnded
			(Salvage_ID              [INT] NOT NULL,
			 Update_DateTime         [DATETIME] NULL,
			 IBNLastStarted_DateTime [DATETIME] NULL,
			 IBNDuration             DECIMAL(18, 9) NULL)
			
		 --IF OBJECT_ID('tempdb..#TMP_FactStockEvent') IS NOT NULL DROP TABLE #TMP_FactStockEvent
		  CREATE TABLE #TMP_FactStockEvent
			(ASAPSalvageID        [INT] NOT NULL,
			 EventDateTime        [DATETIME] NULL,
			 EventCode            [CHAR](4) NULL ) 
			 
		  
		  --IF OBJECT_ID('tempdb..#TMP_Hold_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Dates
		   CREATE TABLE #TMP_Hold_Dates
			 (Salvage_ID        [INT] NOT NULL,
			  HoldCode          [CHAR](4) NULL,
			  RelCode           [CHAR](4) NULL,
			  Hold_Start_Date   [DATETIME] NULL,  
			  Hold_End_Date     [DATETIME] NULL ) 
			  
			  
		  --IF OBJECT_ID('tempdb..#TMP_Hold_Final_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Final_Dates
			CREATE TABLE #TMP_Hold_Final_Dates
			 (Salvage_ID        [INT] NOT NULL,
			  HoldCode          [CHAR](4) NULL,
			  RelCode           [CHAR](4) NULL,
			  Hold_Start_Date   [DATETIME] NULL,  
			  Hold_End_Date     [DATETIME] NULL ) 
						
			
		  --IF OBJECT_ID('tempdb..#TMP_Hold_Durations') IS NOT NULL DROP TABLE #TMP_Hold_Durations
			CREATE TABLE #TMP_Hold_Durations
			 (Salvage_ID         [INT] NOT NULL,
			  Hold_Time_Calendar [DECIMAL](18, 9) NULL, 
			  HOld_Time_Business [DECIMAL](18, 9) NULL)  

			CREATE TABLE #TMPAuctionStockID (Salvage_ID INT)
			
			CREATE TABLE #TMP_SEHAU (Salvage_ID INT,DropOffDateTime DATETIME, DropOffDayID INT)
			
		--	IF OBJECT_ID('tempdb..#TMP_Salvage_Provider_Info_AdvanceChargePrepaidThruDateTime') IS NOT NULL DROP TABLE #TMP_Salvage_Provider_Info_AdvanceChargePrepaidThruDateTime
		  CREATE TABLE #TMP_Salvage_Provider_Info_AdvanceChargePrepaidThruDateTime
			([STOCKID]                             [INT] NOT NULL,
			 [AdvanceChargePrepaidThruDateTime]    [DATETIME] NULL,
			  )				 

			CREATE TABLE #TMP_StockIDnASAPSalvageID	(
				  StockID		int	NOT NULL
				, ASAPSalvageID	int	NOT NULL		)

			CREATE TABLE #TMP_ASAPSalvageID_CertInDate	(
				  StockID			int	NOT NULL
				, ASAPSalvageID		int	NOT NULL
				, NewCertInDateTime	DateTime		)

         CREATE TABLE #TMP_LastTitleAssistForm
          ( ASAPSalvageID				INT,
            StockID					INT,
            [LastTitleAssistFormUploadDateTime] Datetime)  
	  /*****************END CREATE TEMP TABLES********************/
	  
	  
		  --DECLARE @MinLSN Binary(10)
		  --		,@MaxLSN Binary (10)
		  --SELECT @MinLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Salvage_info')
		  --SELECT @MaxLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()



		  INSERT INTO #TMP_CT_Salvage_Info
					  (Salvage_ID,
					   Assignment_DateTime,
					   Assignment_Day_ID,
					   Release_DateTime,
					   Release_Day_ID,
					   Checkin_DateTime,
					   Checkin_Day_ID,
					   Buyer_Payment_DateTime,
					   Buyer_Payment_Day_ID,
					   Received_All_Documents_DateTime,
					   Received_All_Documents_Day_ID,
					   Missing_Parts_Datetime,
					   Missing_Parts_Day_ID,
					   Dispatch_Permanently_Cancelled_DateTime,
					   Dispatch_Permanently_Cancelled_DayID,
					   Hold_Release_Date_Time,
					   Hold_Release_Day_ID,
					   PreBill_Paid_Date,
					   Prebill_day_ID,
					   Sold_Date_Time,
					   Sold_Day_ID)
		  SELECT Salvage_ID,
				 CASE WHEN Isnull(Assignment_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Assignment_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Assignment_DateTime,
				 CASE WHEN Isnull(Assignment_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Assignment_DateTime, 112)), 29991231) ELSE 29991231 END AS Assignment_Day_ID,
				 CASE WHEN Isnull(Release_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Release_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Release_DateTime,
				 CASE WHEN Isnull(Release_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Release_DateTime, 112)), 29991231)  ELSE 29991231 END AS Release_Day_ID,
				 CASE WHEN Isnull(Checkin_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Checkin_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Checkin_DateTime,
				 CASE WHEN Isnull(Checkin_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Checkin_DateTime, 112)), 29991231)  ELSE 29991231 END AS Checkin_Day_ID,
				 CASE WHEN Isnull(Buyer_Payment_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Buyer_Payment_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Buyer_Payment_DateTime,
				 CASE WHEN Isnull(Buyer_Payment_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Buyer_Payment_DateTime, 112)), 29991231)  ELSE 29991231 END AS Buyer_Payment_Day_ID,
				 CASE WHEN Isnull(Received_All_Documents_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Received_All_Documents_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Received_All_Documents_DateTime,
				 CASE WHEN Isnull(Received_All_Documents_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Received_All_Documents_DateTime, 112)), 29991231)  ELSE 29991231 END AS Received_All_Documents_Day_ID,
				 CASE WHEN Isnull(Missing_Parts_Datetime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Missing_Parts_Datetime, '2999-12-31') ELSE '2999-12-31' END AS Missing_Parts_Datetime,
				 CASE WHEN Isnull(Missing_Parts_Datetime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Missing_Parts_Datetime, 112)), 29991231)  ELSE 29991231 END AS Missing_Parts_Day_ID,
				 CASE WHEN Isnull(Dispatch_Permanently_Cancelled_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Dispatch_Permanently_Cancelled_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Dispatch_Permanently_Cancelled_DateTime,
				 CASE WHEN Isnull(Dispatch_Permanently_Cancelled_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Dispatch_Permanently_Cancelled_DateTime, 112)), 29991231)  ELSE 29991231 END AS Dispatch_Permanently_Cancelled_Day_ID,
				 CASE WHEN Isnull(Hold_Release_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Hold_Release_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Hold_Release_DateTime,
				 CASE WHEN Isnull(Hold_Release_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Hold_Release_DateTime, 112)), 29991231)  ELSE 29991231 END AS Hold_Release_Day_ID,
				 CASE WHEN Isnull(Prebill_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Prebill_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Prebill_DateTime,
				 CASE WHEN Isnull(Prebill_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Prebill_DateTime, 112)), 29991231)  ELSE 29991231 END AS Prebill_Day_ID,
				 CASE WHEN Isnull(Sold_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Sold_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Sold_DateTime,
				 CASE WHEN Isnull(Sold_DateTime, '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Sold_DateTime, 112)), 29991231)  ELSE 29991231 END AS Sold_Day_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_info](@MinLSN, @MaxLSN, 'all with merge')
		  WHERE [__$operation] = 5

		  CREATE UNIQUE CLUSTERED INDEX IX_TMP_CT_Salvage_Info
			ON #TMP_CT_Salvage_Info (Salvage_ID)
			WITH FILLFACTOR = 100;
		  
	  -- TFS 44394 CHANGE DATE LOGIC AS PART OF DW MERGE
	    --- PICKUP DateTime
		--old code
	 	--  INSERT INTO #TMP_CT_Pickup_DateTime
			--		  (Salvage_ID,
			--		   Pickup_DateTime,
			--		   Pickup_Day_ID)
		 -- SELECT ST.Salvage_ID,
			--	 Max(SEH.Event_DateTime) AS Pickup_DateTime,
			--	 CONVERT(INT, CONVERT(CHAR(8), Max(SEH.Event_Datetime), 112)) AS Pickup_Day_ID
		 -- FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge') AS SEH
			--	 INNER JOIN BI_ASAP_Rep.dbo.Salvage_Tow AS ST WITH(NOLOCK)
			--	   ON ST.TowBill_Number=SEH.TowBill_Number
			--		  AND SEH.Salvage_Event_Code='DROP'
			--		  AND ST.Tow_Type_Code='PK'
			--WHERE [__$operation] = 5
		 -- GROUP  BY ST.Salvage_ID 
		
		  		  INSERT INTO #TMP_CT_Pickup_DateTime
					(
					Salvage_ID,
					Pickup_DateTime,
					Pickup_Day_ID
					)
		    SELECT 	seh.Salvage_ID,
					MIN(seh.Event_DateTime)  Pickup_DateTime,
					CONVERT(INT, CONVERT(CHAR(8),MIN(seh.Event_DateTime), 112)) AS Pickup_Day_ID
			FROM [BI_ASAP_Rep].dbo.salvage_event_history SEH WITH (NOLOCK)
			INNER JOIN (SELECT * FROM	[BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge') AS SEH
						WHERE	seh.Salvage_Event_Code IN ('DROP','CKI1')
						AND	    [__$operation] = 5) AS Chgs
			ON SEH.Salvage_ID = Chgs.Salvage_ID
			WHERE	seh.Salvage_Event_Code IN ('DROP','CKI1')
			GROUP BY	
					 seh.Salvage_ID

		  INSERT INTO #TMP_CT_Settlement_SalvageIDs
					  (Salvage_ID)
		  SELECT DISTINCT Salvage_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_settlement](@MinLSN, @MaxLSN, 'all with merge')
		  WHERE [__$operation] = 5
		 
		  UNION
		 
		  SELECT DISTINCT Salvage_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge')
		  WHERE  [__$operation] = 5 AND Salvage_Event_Code IN ('STLV') -- Get any stocks that have been voided after settlement

		  
		  INSERT INTO #TMP_CT_Settlement_DateTime
					  (Salvage_ID,
					   Settlement_DateTime,
					   Settlement_Day_ID)
		  SELECT SS.Salvage_ID,
				 CASE WHEN Isnull(Max(SU.Settlement_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Max(SU.Settlement_Date), '2999-12-31') ELSE '2999-12-31' END AS Settlement_Date,
				 CASE WHEN Isnull(Max(SU.Settlement_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Max(SU.Settlement_Date), 112)), 29991231)  ELSE 29991231 END AS Settlement_Day_ID
		  FROM   BI_ASAP_Rep.dbo.Salvage_Settlement AS SS WITH (NOLOCK)
				 INNER JOIN BI_ASAP_Rep.dbo.Settlement_Unit AS SU WITH(NOLOCK)
				   ON SS.Settlement_Unit_ID=SU.Settlement_Unit_ID
					  AND SS.Status_Code='FSF'
					  AND SS.Settlement_Type_Code<>'PB'
					  AND SU.Payment_Status_Code IN ('UPD', 'PD')
				 INNER JOIN #TMP_CT_Settlement_SalvageIDs SI
				   ON SS.Salvage_ID=SI.Salvage_ID
		  GROUP  BY SS.Salvage_ID
	  
  		  
		  INSERT INTO #TMP_CT_Settlement_DateTime(Salvage_ID ,Settlement_DateTime ,Settlement_Day_ID) 
		  SELECT S.Salvage_ID -- Stocks that were voided after Settlement 
				,'2999-12-31' AS  Settlement_Date
				,29991231 AS Settlement_Day_ID
		  FROM #TMP_CT_Settlement_SalvageIDs S
		  WHERE S.Salvage_ID NOT IN (SELECT Salvage_ID FROM #TMP_CT_Settlement_DateTime)
		 
		  
		  
		  -- Get the Earliest Sale Date 
		  INSERT INTO #TMP_CT_Earliest_Sale_DateTime
					  (Salvage_ID,
					   Earliest_Sale_DateTime,
					   Earliest_Sale_Day_ID)
		  SELECT SI.Salvage_ID AS Salvage_ID,
				 CASE WHEN Isnull(Max(SI.Earliest_Sale_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Max(SI.Earliest_Sale_Date), '2999-12-31') ELSE '2999-12-31' END AS Earliest_Sale_DateTime,
				 CASE WHEN Isnull(Max(SI.Earliest_Sale_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Max(SI.Earliest_Sale_Date), 112)), 29991231)  ELSE 29991231 END AS Earliest_Sale_Day_ID
		  ----FROM  [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_note](@MinLSN, @MaxLSN, 'all with merge') AS SN
				----INNER JOIN BI_ASAP_Rep.dbo.Salvage_Info AS SI WITH(NOLOCK)
				----  ON SN.Salvage_ID=SI.Salvage_ID
		  ----WHERE  SN.Salvage_Event_Category_Code='VEH'
			 ----AND SN.Note_Text LIKE 'earliest sale date%'
			 ----AND [__$operation] = 5
			FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Info](
					@MinLSN,@MaxLSN,'all with merge') AS SI
					WHERE [__$operation] = 5  
		  GROUP  BY SI.Salvage_ID

		  INSERT INTO #TMP_CT_SEH_SalvageIDs
					  (Salvage_ID)
		  SELECT DISTINCT Salvage_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge')
		  WHERE  --Salvage_Event_Code IN ('ADSV', 'LSRC', 'AINS', 'RLQT','RCVY', 'STCI', 'DFPD', 'VTRN',
				--						'ASCN', 'LOSS', 'SDAP', 'TABP','SAL4', 'SAL9', 'HDRM', 'HDCF',
				--						'HDCN', 'HDCR', 'RLPR', 'RLRS','TPRG', 'TPRS', 'VIMG', 'CKI2',
				--						'BYCN', 'BYRE', 'HDRQ', 'HLCO','HRCO', 'DPLO', 'DPLI', 'TLRC',
				--						'SDRC', 'TWRC', 'HDCF', 'CTRC','SDRM', 'TRRM', 'CRRM','ADRC','CLOS','FSEN','STLV','SDRO',
				--						'ASGN','BYPU','BYPD','RLQP','DPAS', 'DPDP','NMCD','NMOV')
				--AND 
				[__$operation] = 5
			UNION                               
		  SELECT DISTINCT Salvage_ID
		  FROM BI_ASAP_Rep.cdc.fn_Cdc_get_net_Changes_dbo_Salvage_Info(@MinLSN, @MaxLSN, 'all with merge')  
					 WHERE [__$operation] = 5

		  CREATE UNIQUE CLUSTERED INDEX IX_TMP_CT_SEH_SalvageIDs
			ON #TMP_CT_SEH_SalvageIDs(Salvage_ID)
			WITH FILLFACTOR = 100;

		  INSERT INTO #TMP_CT_SEH_Dates
					  (Salvage_ID,
					   Advance_Storage_Verified_Date,
					   Date_Of_Loss_Reported_DateTime,
					   Inspection_DateTime,
					   Recovery_Datetime,
					   Settlement_Paid_DateTime,
					   VIC_Inspection_DateTime,
					   Cancel_DateTime,
					   Loss_DateTime,
					   Sales_Document_Applied_Datetime,
					   Title_Approved_By_Provider_DateTime,
					   HDCF_Event_Datetime,
					   Release_Problem_Identified_Datetime,
					   Release_Problem_Resolved_Datetime,
					   Title_Problem_Identified_Datetime,
					   Title_Problem_Resolved_Datetime,
					   Vehicle_Images_Loaded_Datetime,
					   Check_In_Images_Taken_Datetime,
					   Buyer_Renege_Cancel_Sale_Datetime,
					   Check_Issued_Datetime,
					   Log_Out_DateTime,
					   Log_In_Datetime,
					   Title_Received_Datetime,
					   Sale_Document_Received_Datetime,
					   Certificate_Received_Datetime,
					   Titlework_Received_Datetime,
					   HDRM_Event_Date,
					   ASCN_Event_Date,
					   HDCN_Event_Date,
					   HDCR_Event_Date,
					   Placed_On_Hold_Date_Duration,
					   Sale_Doc_Removed_Date,
					   Titlework_Removed_Date,
					   Certificate_Removed_Date,
					   All_Title_Documents_Received_Date,
					   All_Title_Documents_Recieved_Date_Reset_No,
					   Stock_Closed_Date_Time,
					   ASGN_Event_Date,
					   BYPD_Event_Date,
					   FinalNMVTISDate,
					   InitialNMVTISDate,
					   Selling_Branch_Received_Forwarded_Title_Docs,
					    DateTimeReportedtoNICB  ,
						[PartialDocAwaitingTitleDirectForm],
                        [PartialDocLienholderDocsRcvd],
                        [PartialDocOwnerDocsRcvd]
						       )
		  SELECT TSEH.Salvage_ID,
				 Max(CASE
					   WHEN DSE.EventCode='ADSV' THEN FSE.EventDateTime
					 END) Advance_Storage_Verified_Date,
				 Max(CASE
					   WHEN DSE.EventCode='LSRC' THEN FSE.EventDateTime
					 END) Date_Of_Loss_Reported_DateTime,
				 Max(CASE
					   WHEN DSE.EventCode='AINS' THEN FSE.EventDateTime
					 END) Inspection_DateTime,
				 Max(CASE
					   WHEN DSE.EventCode='RCVY' THEN FSE.EventDateTime
					 END) Recovery_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('STCI', 'DFPD') THEN FSE.EventDateTime
					 END) Settlement_Paid_DateTime,
				 Max(CASE
					   WHEN DSE.EventCode='VTRN' THEN FSE.EventDateTime
					 END) VIC_Inspection_DateTime,
				 Max(CASE
					   WHEN DSE.EventCode='ASCN' THEN FSE.EventDateTime
					 END) Cancel_DateTime,
				
				 --Max(CASE
					--   WHEN DSE.EventCode='LOSS' THEN FSE.EventDateTime
					-- END) Loss_DateTime,


				 Max(CASE
					   WHEN DSE.EventCode IN ('RCVY','LOSS') THEN FSE.EventDateTime
					 END) Loss_DateTime,


				 Max(CASE
					   WHEN DSE.EventCode='SDAP' THEN FSE.EventDateTime
					 END) Sales_Document_Applied_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode='TABP' THEN FSE.EventDateTime
					 END) Title_Approved_By_Provider_DateTime,
				 Max(CASE
					   WHEN DSE.EventCode='HDCF' THEN FSE.EventDateTime
					 END) HDCF_Event_Datetime,
				Max(CASE
					   WHEN DSE.EventCode='RLPR' THEN FSE.EventDateTime
					 END) Release_Problem_Identified_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode='RLRS' THEN FSE.EventDateTime
					 END) Release_Problem_Resolved_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode='TPRG' THEN FSE.EventDateTime
					 END) Title_Problem_Identified_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode='TPRS' THEN FSE.EventDateTime
					 END) Title_Problem_Resolved_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode='VIMG' THEN FSE.EventDateTime
					 END) Vehicle_Images_Loaded_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode='VIMG' THEN FSE.EventDateTime --WHEN DSE.EventCode='CKI2' THEN FSE.EventDateTime
					 END) Check_In_Images_Taken_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode IN('BYCN', 'BYRE') THEN FSE.EventDateTime
					 END) Buyer_Renege_Cancel_Sale_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode='STCI' THEN FSE.EventDateTime
					 END) Check_Issued_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('DPLO') THEN FSE.EventDateTime
					 END) Log_Out_DateTime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('DPLI') THEN FSE.EventDateTime
					 END) Log_In_DateTime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('TLRC') THEN FSE.EventDateTime
					 END) Title_Received_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('SDRC') THEN FSE.EventDateTime
					 END) Sale_Document_Received_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('CTRC') THEN FSE.EventDateTime
					 END) Certificate_Received_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('TWRC') THEN FSE.EventDateTime
					 END) Titlework_Received_Datetime,
				 Max(CASE
					   WHEN DSE.EventCode IN ('HDRM') THEN FSE.EventDateTime
					 END) HDRM_Event_Date,
				 Max(CASE
					   WHEN DSE.EventCode IN ('ASCN') THEN FSE.EventDateTime
					 END) ASCN_Event_Date,
				 Max(CASE
					   WHEN DSE.EventCode IN ('HDCN') THEN FSE.EventDateTime
					 END) HDCN_Event_Date,
				 Max(CASE
					   WHEN DSE.EventCode IN ('HDCR') THEN FSE.EventDateTime
					 END) HDCR_Event_Date,
				 Isnull(Max(CASE
							  WHEN DSE.EventCode IN ('HDCF') THEN FSE.EventDateTime
							END), '2999-12-31') Placed_On_Hold_Date_Duration,
				 Max(CASE
					   WHEN DSE.EventCode IN ('SDRM') THEN FSE.EventDateTime
					 END) Sale_Doc_Removed_Date,
				 Max(CASE
					   WHEN DSE.EventCode IN ('TRRM') THEN FSE.EventDateTime
					 END) Titlework_Removed_Date,
				 Max(CASE
					   WHEN DSE.EventCode IN ('CRRM') THEN FSE.EventDateTime
					 END) Certificate_Removed_Date
			     ,MAX(CASE 
			            WHEN DSE.EventCode='ADRC' THEN FSE.EventDateTime 
			         END) All_Title_Documents_Received_Date
			     ,MAX(CASE 
			            WHEN DSE.EventCode='ADRN' THEN 
			           FSE.EventDateTime 
			         END) All_Title_Documents_Recieved_Date_Reset_No    
                 ,MAX(CASE 
                         WHEN DSE.EventCode='CLOS' THEN FSE.EventDateTime 
                     END) Stock_Closed_Date_Time
                 ,MAX(CASE 
                         WHEN DSE.EventCode='ASGN' THEN FSE.EventDateTime 
                     END) ASGN_Event_Date	
                 ,MAX(CASE 
                         WHEN DSE.EventCode='BYPD' THEN FSE.EventDateTime 
                     END) BYPD_Event_Date
				,Max(CASE WHEN DSE.EventCode='NMCD' THEN FSE.EventDateTime END) AS FinalNMVTISDate --VehicleChangeDispDatetime
				,Max(CASE WHEN DSE.EventCode='NMOV' THEN FSE.EventDateTime END) AS InitialNMVTISDate --ObtainVehicleDatetime
				,Max(CASE
					   WHEN DSE.EventCode='SBRC' THEN FSE.EventDateTime
					 END)	Selling_Branch_Received_Forwarded_Title_Docs	
				,Max(CASE
					   WHEN DSE.EventCode='NCPR' THEN FSE.EventDateTime
					 END)	DateTimeReportedtoNICB		
				,Max(CASE
					   WHEN DSE.EventCode='TATD' THEN FSE.EventDateTime
					 END)[PartialDocAwaitingTitleDirectForm]
				,Max(CASE
					   WHEN DSE.EventCode='TLDR' THEN FSE.EventDateTime
					 END)[PartialDocLienholderDocsRcvd]	 
				,Max(CASE
					   WHEN DSE.EventCode='TODR' THEN FSE.EventDateTime
					 END)[PartialDocOwnerDocsRcvd]	 	  			 					 				 		 
		  FROM   dbo.FactStockEvent AS FSE WITH(NOLOCK)
				 INNER JOIN dbo.DimStockEventType AS DSE WITH(NOLOCK)
				   ON FSE.EventTypeID=DSE.EventTypeID
				 INNER JOIN #TMP_CT_SEH_SalvageIDs AS TSEH WITH(NOLOCK)
				   ON TSEH.Salvage_ID=FSE.ASAPSalvageID
		  WHERE  DSE.EventCode IN ('ADSV', 'LSRC', 'AINS', 'RLQT','RCVY', 'STCI', 'DFPD', 'VTRN',
								   'ASCN', 'LOSS', 'SDAP', 'TABP','HDRM', 'HDCF', 'HDCN', 'HDCR',
								   'RLPR', 'RLRS', 'TPRG', 'TPRS','VIMG', 'CKI2', 'BYCN', 'BYRE',
								   'HDRQ', 'HLCO', 'HRCO', 'DPLO','DPLI', 'TLRC', 'SDRC', 'TWRC',
								   'HDCF', 'CTRC', 'SDRM', 'TRRM', 'CRRM','ADRC','ADRN', 'CLOS',
								   'ASGN','BYPD','RLQP','NMCD','NMOV','SBRC','NCPR','TATD','TLDR','TODR')
			 AND FSE.IsMostRecentEvent=1
		  GROUP  BY TSEH.Salvage_ID

		  CREATE UNIQUE CLUSTERED INDEX IX_TMP_CT_SEH_Dates
			ON #TMP_CT_SEH_Dates (Salvage_ID)
			WITH FILLFACTOR = 100;

	  MERGE #TMP_CT_SEH_Dates AS DST USING (SELECT TSEH.Salvage_ID, Min(FSE.EventDateTime) First_Sold_DateTime FROM dbo.FactStockEvent AS FSE WITH
	  (
	  NOLOCK) INNER JOIN dbo.DimStockEventType AS DSE WITH(NOLOCK) ON FSE.EventTypeID = DSE.EventTypeID INNER JOIN #TMP_CT_SEH_SalvageIDs AS TSEH
	  WITH(NOLOCK) ON TSEH.Salvage_ID = FSE.ASAPSalvageID WHERE DSE.EventCode IN ('SAL4', 'SAL9') AND FSE.IsFirstEvent = 1 GROUP BY TSEH.Salvage_ID)
	  AS
	  SRC ON SRC.Salvage_ID = DST.Salvage_ID WHEN MATCHED THEN UPDATE SET DST.First_Sold_DateTime = SRC.First_Sold_DateTime WHEN NOT MATCHED THEN
	  INSERT
	  (Salvage_ID, First_Sold_DateTime) VALUES (SRC.Salvage_ID, SRC.First_Sold_DateTime);
	--  Update title recvd datetime with data from Salvage_Title if not on same day 
	  UPDATE #TMP_CT_SEH_Dates
	  SET 
	  Title_Received_Datetime = ISNULL(CASE WHEN ST.Title_Received_DateTime IS NOT NULL THEN
	          
	                CASE WHEN DATEDIFF(DAY,ISNULL(SEH.Title_Received_Datetime,'2999-12-31'),ISNull(ST.Title_Received_DateTime,'2999-12-31')) <> 0 THEN ST.Title_Received_DateTime 
	                    ELSE SEH.Title_Received_Datetime   END
		      
		      ELSE NULL 
	          END ,'2999-12-31')
			  
	  FROM 
	  #TMP_CT_SEH_Dates AS SEH
	  LEFT OUTER JOIN  BI_ASAP_Rep.dbo.Salvage_Info as ST WITH (NOLOCK)
	  ON SEH.Salvage_ID = ST.Salvage_ID 
	   

      UPDATE #TMP_CT_SEH_Dates 
      SET
      Title_Received_Datetime = CASE WHEN ISNULL(Title_Received_Datetime,'2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN ISNULL(Title_Received_Datetime,'2999-12-31') ELSE '2999-12-31' END
      
      
     Insert Into #TMP_CT_SEH_Dates_Min
			([Salvage_ID]                         ,
			Min_Sales_Document_Applied_Datetime  , 
            --Min_Title_Received_Datetime            ,
            --Min_Certificate_Received_Datetime     ,
            --Min_Sale_Document_Received_Datetime  ,
            --Min_Sale_Doc_Removed_Date,
            --Min_Titlework_Removed_Date,
            --Min_Certificate_Removed_Date ,
            --Min_Titlework_Received_Datetime	
            First_Release_DateTime                , 
            First_All_Title_Documents_Received_DateTime       
              )
            
       SELECT TSEH.Salvage_ID,
      
      Min(CASE
					   WHEN DSE.Eventcode='SDAP' THEN FSE.EventDateTime
					 END) Min_Sales_Document_Applied_Datetime ,
					--  Min(CASE
					--   WHEN DSE.Eventcode IN ('TLRC') THEN FSE.EventDateTime
					-- END) Min_Title_Received_Datetime,
					--  Min(CASE
					--   WHEN DSE.Eventcode IN ('CTRC') THEN FSE.EventDateTime
					-- END)  Min_Certificate_Received_Datetime,
					--  Min(CASE
					--   WHEN DSE.Eventcode IN ('SDRC') THEN FSE.EventDateTime
					-- END) Min_Sale_Document_Received_Datetime ,
					--   Min(CASE
					--   WHEN DSE.EventCode IN ('SDRM') THEN FSE.EventDateTime
					-- END) Min_Sale_Doc_Removed_Date,
				 --Min(CASE
					--   WHEN DSE.EventCode IN ('TRRM') THEN FSE.EventDateTime
					-- END) Min_Titlework_Removed_Date,
				 --Min(CASE
					--   WHEN DSE.EventCode IN ('CRRM') THEN FSE.EventDateTime
					-- END) Min_Certificate_Removed_Date ,
				 --Min(CASE
					--   WHEN DSE.EventCode IN ('TWRC') THEN FSE.EventDateTime
					-- END) Min_Titlework_Received_Datetime	     
          Min( Case when EventCode='RLPD' and EventDescription ='Actual Release Date'   then   [eventdatetime]
                               end )  First_Release_DateTime ,
          Min( Case when EventCode='ADRC' and EventDescription ='All Title Documents Received'   then   [eventdatetime]
                               end )      First_All_Title_Documents_Received_DateTime        
    FROM   edw.dbo.FactStockEvent AS FSE WITH(NOLOCK)
				 INNER JOIN edw.dbo.DimStockEventType AS DSE WITH(NOLOCK)
				   ON FSE.EventTypeID=DSE.EventTypeID
				 INNER JOIN #TMP_CT_SEH_SalvageIDs AS TSEH WITH(NOLOCK)
				   ON TSEH.Salvage_ID=FSE.ASAPSalvageID
		  WHERE  DSE.EventCode IN ('SDAP','RLPD','ADRC')
		  ---,'TLRC','CTRC','SDRC','SDRM','TRRM','CRRM','TWRC')
			
		  GROUP  BY TSEH.Salvage_ID
         
            
     CREATE UNIQUE CLUSTERED INDEX MIX_TMP_CT_SEH_Dates_Min
			ON #TMP_CT_SEH_Dates_Min (Salvage_ID)
			WITH FILLFACTOR = 100;
			
	
               
            
            
 /********************CERT IN DATE*************************/
		  UPDATE SEH
		  SET    Sale_Document_Received_Datetime=CASE
												   WHEN Isnull(Sale_Doc_Removed_Date, '1/1/1900')>Sale_Document_Received_Datetime THEN '12/31/2999'
												   ELSE Sale_Document_Received_Datetime
												 END,
				 Titlework_Received_Datetime=CASE
											   WHEN Isnull(Titlework_Removed_Date, '1/1/1900')>Titlework_Received_Datetime THEN '12/31/2999'
											   ELSE Titlework_Received_Datetime
											 END,
				 Certificate_Received_Datetime=CASE
												 WHEN Isnull(Certificate_Removed_Date, '1/1/1900')>Certificate_Received_Datetime THEN '12/31/2999'
												 ELSE Certificate_Received_Datetime
											   END
		  FROM   #TMP_CT_SEH_Dates AS SEH

		  --If there is not a Certificate_Received_Datetime then use Titlework_Received_Datetime
		  UPDATE SEH
		  SET    Certificate_Received_Datetime=CASE
												 WHEN Certificate_Received_Datetime IS NULL THEN Titlework_Received_Datetime
												 ELSE Certificate_Received_Datetime
											   END
		  FROM   #TMP_CT_SEH_Dates AS SEH





		  INSERT INTO #TMP_CT_Cert_In_DateTime
					  (Salvage_ID,
					   Cert_In_DateTime,
					   Stock_Event_Code)
		  SELECT Salvage_ID,
				 COALESCE(Sale_Document_Received_Datetime, Certificate_Received_Datetime, '12/31/2999') AS Cert_In_DateTime,
				 CASE
				   WHEN Sale_Document_Received_Datetime IS NOT NULL THEN 'SDRC'
				   WHEN Certificate_Received_Datetime IS NOT NULL THEN 'CTRC'
				   ELSE ''
				 END AS Stock_Event_Code
		  FROM   #TMP_CT_SEH_Dates
		  ORDER  BY 1

		  CREATE NONCLUSTERED INDEX IX_Stock_Item_ID
			ON #TMP_CT_Cert_In_DateTime(Salvage_ID)

		  CREATE NONCLUSTERED INDEX IX_Stock_Event_Code
			ON #TMP_CT_Cert_In_DateTime(Stock_Event_Code)
			
			
		/********************Min CERT IN DATE*************************/
		  --UPDATE SEHM
		  --SET    Min_Sale_Document_Received_Datetime=CASE
				--								   WHEN Isnull(Min_Sale_Doc_Removed_Date, '1/1/1900')>Min_Sale_Document_Received_Datetime THEN '12/31/2999'
				--								   ELSE Min_Sale_Document_Received_Datetime
				--								 END,
				-- Min_Titlework_Received_Datetime=CASE
				--							   WHEN Isnull(Min_Titlework_Removed_Date, '1/1/1900')>Min_Titlework_Received_Datetime THEN '12/31/2999'
				--							   ELSE Min_Titlework_Received_Datetime
				--							 END,
				-- Min_Certificate_Received_Datetime=CASE
				--								 WHEN Isnull(Min_Certificate_Removed_Date, '1/1/1900')>Min_Certificate_Received_Datetime THEN '12/31/2999'
				--								 ELSE Min_Certificate_Received_Datetime
				--							   END
		  --FROM   #TMP_CT_SEH_Dates_Min AS SEHM

		  ----If there is not a Certificate_Received_Datetime then use Titlework_Received_Datetime
		  --UPDATE SEHM
		  --SET    Min_Certificate_Received_Datetime=CASE
				--								 WHEN Min_Certificate_Received_Datetime IS NULL THEN Min_Titlework_Received_Datetime
				--								 ELSE Min_Certificate_Received_Datetime
				--							   END
		  --FROM   #TMP_CT_SEH_Dates_Min AS SEHM
 
	
		 
		 --INSERT INTO #TMP_CT_Cert_In_DateTime_Min
			--		  (Salvage_ID,
			--		   Min_Cert_In_DateTime,
			--		   Min_Stock_Event_Code
			--		   )
		 -- SELECT Salvage_ID,
			--	 COALESCE(Min_Sale_Document_Received_Datetime, Min_Certificate_Received_Datetime, '12/31/2999') AS Min_Cert_In_DateTime,
			--	 CASE
			--	   WHEN Min_Sale_Document_Received_Datetime IS NOT NULL THEN 'SDRC'
			--	   WHEN Min_Certificate_Received_Datetime IS NOT NULL THEN 'CTRC'
			--	   ELSE ''
			--	 END AS Min_Stock_Event_Code
				
		 -- FROM   #TMP_CT_SEH_Dates_Min
		 -- ORDER  BY 1

		 -- CREATE NONCLUSTERED INDEX Min_IX_Stock_Item_ID
			--ON #TMP_CT_Cert_In_DateTime_Min(Salvage_ID)	
			
			

		  /*****If CTRC and SDRC events for a stock remove the SDRC date*****/
		  INSERT INTO #TMP_CT_dups
					  (Salvage_ID,
					   Stock_Event_Code)
		  SELECT t1.Salvage_ID,
				 t1.Stock_Event_Code
		  FROM   #TMP_CT_Cert_In_DateTime AS t1 WITH(NOLOCK)
				 INNER JOIN #TMP_CT_Cert_In_DateTime AS t2 WITH(NOLOCK)
				   ON t1.Salvage_ID=t2.Salvage_ID
		  WHERE  t1.Stock_Event_Code<>t2.Stock_Event_Code
		  ORDER  BY t1.Salvage_ID

		  DELETE TCCTD
		  FROM   #TMP_CT_Cert_In_DateTime AS TCCTD WITH (NOLOCK)
				 INNER JOIN #TMP_CT_dups AS D WITH(NOLOCK)
				   ON TCCTD.Salvage_ID=D.Salvage_ID
					  AND TCCTD.Stock_Event_Code=D.Stock_Event_Code
		  WHERE  D.Stock_Event_Code='SDRC'

		  UPDATE SEH
		  SET
		  SMVD_Datetime=CASE
						  WHEN [Sales_Document_Applied_Datetime]='2999-12-31' THEN '2999-12-31'
						  WHEN [Sales_Document_Applied_Datetime]<[Title_Received_Datetime]
							   AND [Title_Received_Datetime]<'2999-12-31' THEN [Title_Received_Datetime]
						  WHEN [Sales_Document_Applied_Datetime]>[Cert_In_DateTime]
							   AND [Title_Received_Datetime]<'2999-12-31'
							   AND [Cert_In_DateTime]<'2999-12-31' THEN [Title_Received_Datetime]
						  ELSE [Sales_Document_Applied_Datetime]
						END,
		  Title_Problem_Resolved_Datetime=CASE
											WHEN Title_Problem_Identified_Datetime>Title_Problem_Resolved_Datetime THEN '2999-12-31'
											ELSE Title_Problem_Resolved_Datetime
										  END
		  FROM   #TMP_CT_SEH_Dates SEH
				 INNER JOIN BI_ASAP_Rep.dbo.Salvage AS S WITH(NOLOCK)
				   ON SEH.Salvage_ID=S.Salvage_ID
				 INNER JOIN #TMP_CT_Cert_In_DateTime AS TCCTD WITH (NOLOCK)
				   ON SEH.Salvage_ID=TCCTD.Salvage_ID


   /*****	If  ADRN event happend and this is the most recent event then 
            All_Title_Documents_Recieved_Date should be set to NULL  *****/
		 
		 UPDATE SEH
		  SET 		   
		    All_Title_Documents_Received_Date = NUll
		   FROM   #TMP_CT_SEH_Dates SEH WITH(NOLOCK)
		  WHERE   SEH.All_Title_Documents_Recieved_Date_Reset_No > SEH.All_Title_Documents_Received_Date 
		
			
		  
		   
		   
		   
		   
		  	   
		 UPDATE SEHM
		  SET
		  Min_SMVD_Datetime=CASE
						  WHEN [Min_Sales_Document_Applied_Datetime]='2999-12-31' THEN '2999-12-31'
						  --WHEN [Min_Sales_Document_Applied_Datetime]<[Min_Title_Received_Datetime]
							 --  AND [Min_Title_Received_Datetime]<'2999-12-31' THEN [Min_Title_Received_Datetime]
						  --WHEN [Min_Sales_Document_Applied_Datetime]>[Min_Cert_In_DateTime]
							 --  AND [Min_Title_Received_Datetime]<'2999-12-31'
							 --  AND [Min_Cert_In_DateTime]<'2999-12-31' THEN [Min_Title_Received_Datetime]
						  ELSE [Min_Sales_Document_Applied_Datetime]
						END
		 
		  FROM   #TMP_CT_SEH_Dates_Min SEHM
				 INNER JOIN BI_ASAP_Rep.dbo.Salvage AS S WITH(NOLOCK)
				   ON SEHM.Salvage_ID=S.Salvage_ID
				 --INNER JOIN #TMP_CT_Cert_In_DateTime_Min AS TCCTD WITH (NOLOCK)
				 --  ON SEHM.Salvage_ID=TCCTD.Salvage_ID		   
				   
				   
		 -----------------BusinessHoldTime and CalendarHoldTime------------------------------------
		 INSERT INTO #TMP_FactStockEvent
		 (ASAPSalvageID,
		  EventDateTime,
		  EventCode)
		 SELECT 
		 ASAPSalvageID, EventDateTime, EventCode
		 FROM dbo.FactStockEvent as FSE WITH (NOLOCK)
		 INNER JOIN dbo.DimStockEventType as DS WITH (NOLOCK)
			   ON FSE.EventTypeID = DS.EventTypeID          
		 INNER JOIN #TMP_CT_SEH_SalvageIDs AS TMP
			   ON TMP.Salvage_ID = FSE.ASAPSalvageID
		 WHERE EventCode in ('HDCF','HDRM','HDCN','HDCR','ASCN')
		 
		
		 
		 INSERT INTO #TMP_Hold_Dates
		 (Salvage_ID,
		  HoldCode,
		  RelCode,
		  Hold_Start_Date,
		  Hold_End_Date)
		 SELECT 
		 S.Salvage_ID
		,SEH1.EventCode AS HoldCode
		,SEH5.EventCode AS RelCode
		,Hold_Start_Date = SEH1.EventDateTime
		,Hold_End_Date = (SEH5.EventDateTime)
		 FROM 
		 BI_ASAP_Rep.dbo.Salvage as S WITH (NOLOCK)
			INNER JOIN #TMP_FactStockEvent SEH1 WITH (NOLOCK)
				  ON S.Salvage_ID = SEH1.ASAPSalvageID
				  AND SEh1.EventCode = 'HDCF'
			LEFT OUTER JOIN #TMP_FactStockEvent SEH3 WITH (NOLOCK)
				  ON SEH1.ASAPSalvageID = SEH3.ASAPSalvageID
				  AND SEH3.EventCode = 'HDCF'
				  AND SEH3.EventDateTime = (SELECT TOP 1 SEH2.EventDateTime
											FROM #TMP_FactStockEvent as SEH2 WITH (NOLOCK)
											WHERE SEH2.EventCode = 'HDCF'
											AND SEH2.ASAPSalvageID = SEH1.ASAPSalvageID
											AND SEH2.EventDateTime > SEH1.EventDateTime ORDER BY SEH2.EventDateTime)
			LEFT OUTER JOIN #TMP_FactStockEvent as SEH5 WITH (NOLOCK) 
				  ON SEH1.ASAPSalvageID = SEH5.ASAPSalvageID
				  AND SEH5.EventCode in ('HDRM','HDCN','HDCR','ASCN')
				  AND SEH5.EventDateTime = (SELECT Top 1 SEH4A.EventDateTime
										   FROM (SELECT SEH4.EventDateTime,  CASE WHEN EventCode =  'HDRM' THEN 1
																				WHEN EventCode =  'HDCN' THEN 2
																				WHEN EventCode =  'HDCR' THEN 3
																				WHEN EventCode =  'ASCN' THEN 4 END AS ECOrder
													FROM #TMP_FactStockEvent as SEH4
													WHERE SEH4.EventCode in ('HDRM','HDCN','HDCR','ASCN')
													AND SEH4.ASAPSalvageID = SEH1.ASAPSalvageID
													AND SEH4.EventDateTime > SEH1.EventDateTime
													AND SEH4.EventDateTime < ISNULL(SEH3.EventDateTime,'2999-12-31')) AS SEH4A
										   ORDER BY SEH4A.ECOrder,                          
												SEH4A.EventDateTime)
									   

		 INSERT INTO #TMP_Hold_Final_Dates
		 (Salvage_ID,
		 HoldCode,
		 RelCode,
		 Hold_Start_Date,
		 Hold_End_Date)
		 SELECT Salvage_ID, HoldCode, RelCode,Hold_Start_Date , 
		 CASE WHEN Hold_End_Date IS NULL 
				THEN (SELECT Min(EventDateTime) AS EventDateTime
						FROM [dbo].[FactStockEvent] as FSE WITH (NOLOCK)
									INNER JOIN dbo.DimStockEventType as dse WITH (NOLOCK)
										  ON fse.EventTypeID = dse.EventTypeID
											 AND #TMP_Hold_Dates.Salvage_ID = FSE.ASAPSalvageID
									INNER JOIN dbo.DimStock as s WITH (NOLOCK)
										  ON s.ASAPSalvageID = fse.ASAPSalvageID
											 WHERE EventCode in ('HDRM','HDCN','HDCR','ASCN') 
											 HAVING Hold_Start_Date <= Min(EventDateTime)) 
							   ELSE Hold_End_Date
							   END AS Hold_End_Date 
		 FROM #TMP_Hold_Dates
		 ORDER BY Salvage_ID,Hold_Start_Date                                           
		  
		 DELETE T2
		 FROM #TMP_Hold_Final_Dates T2
		   INNER JOIN 
			(SELECT T.Salvage_ID,Min(T.Hold_Start_Date) AS Hold_Start_Date,T.Hold_End_Date 
			  FROM #TMP_Hold_Final_Dates T
			   INNER JOIN 
					  (SELECT Salvage_ID ,Hold_End_Date ,COUNT(*) AS Cnt
							   FROM #TMP_Hold_Final_Dates
					  GROUP BY Salvage_ID ,Hold_End_Date HAVING COUNT(*) > 1)T1
			  ON T.Salvage_ID = T1.Salvage_ID AND T.Hold_End_Date = T1.Hold_End_Date
			  GROUP BY T.Salvage_ID ,T.Hold_End_Date
			  )T3
 		ON T2.Salvage_ID = T3.Salvage_ID 
		AND T2.Hold_Start_Date <> T3.Hold_Start_Date
		AND T2.Hold_End_Date = T3.Hold_End_Date                                           

	   
	   UPDATE #TMP_Hold_Final_Dates
	   SET Hold_End_Date =  NULL
	   FROM #TMP_Hold_Final_Dates
	   WHERE Salvage_ID IN (SELECT DISTINCT Salvage_ID FROM #TMP_Hold_Final_Dates
							 WHERE Hold_End_Date IS NULL) 
	   

		  INSERT INTO #TMP_Hold_Durations
		  (Salvage_ID,
		  Hold_Time_Calendar,
		  HOld_Time_Business)                
		  SELECT salvage_ID,
			   Hold_Time_Calendar = SUM(CASE WHEN Hold_End_Date IS NOT NULL AND  Hold_Start_Date IS NOT NULL 
									THEN (CAST(Hold_End_Date  AS FLOAT) - CAST(Hold_Start_Date AS FLOAT)) END )
		   
			  ,Hold_Time_Business =SUM( CASE WHEN Hold_End_Date IS NOT NULL AND  Hold_Start_Date IS NOT NULL 
									THEN ([dbo].[fn_EDWBusinessHours](Hold_Start_Date,Hold_End_Date,AdministrativeBranchNumber)/9) END)
		  FROM #TMP_Hold_Final_Dates T
		  INNER JOIN dbo.DimStock AS DS WITH (NOLOCK)
		  ON DS.ASAPSalvageID = T.Salvage_ID
		  GROUP  BY Salvage_ID           
		  
		  /**********************************************************************/
		  /*  Modify Buyer Pickup Date Time As Part of DW Merge	10-07-2014		*/
 		  /**********************************************************************/
		  --INSERT INTO #TMP_CT_BuyerPickup
				--	  (Salvage_ID,
				--	   Buyer_Pickup_DateTime,
				--	   Buyer_Pickup_Day_ID)
		  --SELECT Salvage_ID,
		  -- Max(Status_DateTime) AS Buyer_Pickup_DateTime,
		  --CONVERT(INT, CONVERT(CHAR(8), Max(Status_DateTime), 112)) AS Buyer_Pickup_Day_ID
		  --FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_process_status](@MinLSN, @MaxLSN, 'all with merge')
		  --WHERE  Status_Code IN ('M45', 'M65') AND [__$operation] = 5
		  --GROUP  BY Salvage_ID

		  		  INSERT INTO #TMP_CT_BuyerPickup
					  (Salvage_ID,
					   Buyer_Pickup_DateTime,
					   Buyer_Pickup_Day_ID)
		 SELECT 
		 ASAPSalvageID, Max(EventDateTime) AS Buyer_Pickup_DateTime, CONVERT(INT, CONVERT(CHAR(8), Max(EventDateTime), 112))
		 FROM dbo.FactStockEvent as FSE WITH (NOLOCK)
		 INNER JOIN dbo.DimStockEventType as DS WITH (NOLOCK)
			   ON FSE.EventTypeID = DS.EventTypeID          
		 INNER JOIN #TMP_CT_SEH_SalvageIDs AS TMP
			   ON TMP.Salvage_ID = FSE.ASAPSalvageID
		 WHERE EventCode in ('BYPU')
		 Group BY ASAPSalvageID

		 INSERT INTO #TMP_CT_Provider_Quoted_Release
					  (Salvage_ID,
					   Provider_Quoted_Release_DateTime)
		 SELECT 
		 ASAPSalvageID, MIN(EventDateTime) AS Provider_Quoted_Release_DateTime
		 FROM dbo.FactStockEvent as FSE WITH (NOLOCK)
		 INNER JOIN dbo.DimStockEventType as DS WITH (NOLOCK)
			   ON FSE.EventTypeID = DS.EventTypeID          
		 INNER JOIN #TMP_CT_SEH_SalvageIDs AS TMP
			   ON TMP.Salvage_ID = FSE.ASAPSalvageID
		 WHERE EventCode IN ('RLQT', 'RLQP')
		 Group BY ASAPSalvageID

		  INSERT INTO #TMP_CT_Dispatch_DateTime
					  (Salvage_ID,
					   Dispatch_DateTime,
					   Dispatch_Day_ID)
		 SELECT 
		 ASAPSalvageID, MIN(EventDateTime) AS Dispatch_DateTime,Isnull(CONVERT(INT, CONVERT(CHAR(8), MIN(FSE.EventDatetime), 112)), 29991231)
		 FROM dbo.FactStockEvent as FSE WITH (NOLOCK)
		 INNER JOIN dbo.DimStockEventType as DS WITH (NOLOCK)
			   ON FSE.EventTypeID = DS.EventTypeID          
		 INNER JOIN #TMP_CT_SEH_SalvageIDs AS TMP
			   ON TMP.Salvage_ID = FSE.ASAPSalvageID
		 WHERE EventCode IN ('DPAS', 'DPDP')
		 Group BY ASAPSalvageID
		
		  INSERT INTO #TMP_CT_Provider_Refund
					  (Salvage_ID,
					   Last_Provider_Refund_Date,
					   Last_Provider_Refund_Day_ID)
		  SELECT Salvage_ID,
				 Max(Created_Datetime) AS Last_Provider_Refund_Date,
				 CONVERT(INT, CONVERT(CHAR(8), Max(Created_Datetime), 112)) AS Last_Provider_Refund_Day_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_provider_refund](@MinLSN, @MaxLSN, 'all with merge')
		  WHERE  Status_Code='PD' AND [__$operation] = 5
		  GROUP  BY Salvage_ID

		  INSERT INTO #TMP_CT_CPSD_SalvageIDs
					  (Salvage_ID)
		  SELECT DISTINCT Salvage_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_process_status](@MinLSN, @MaxLSN, 'all with merge') WHERE [__$operation] = 5
		  UNION
		  SELECT DISTINCT Salvage_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_info](@MinLSN, @MaxLSN, 'all with merge') WHERE [__$operation] = 5

		  INSERT INTO #TMP_CT_CPSS_Datetime
					  (Salvage_ID,
					   Current_Process_Status_Start_Date,
					   Current_Process_Status_Start_Day_ID)
		  SELECT SPS.Salvage_ID,
				 Max(SPS.Status_DateTime) AS Current_Process_Status_Start_Date,
				 CONVERT(INT, CONVERT(CHAR(8), Max(SPS.Status_DateTime), 112)) AS Current_Process_Status_Start_Day_ID
		  FROM   [BI_ASAP_Rep].[dbo].[Salvage_Process_Status] AS SPS WITH(NOLOCK)
				 INNER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Info] AS SI WITH(NOLOCK)
				   ON SPS.Salvage_ID=SI.Salvage_ID
					  AND SPS.Status_Code=SI.Process_Status_Code
				 INNER JOIN #TMP_CT_CPSD_SalvageIDs TSI
				   ON TSI.Salvage_ID=SPS.Salvage_ID
		  GROUP  BY SPS.Salvage_ID
		  ORDER  BY SPS.Salvage_ID

		  INSERT INTO #TMP_CT_ASI
					  (Salvage_ID,
					   [Start_Date],
					   Start_Day_ID,
					   End_Date,
					   End_Day_ID)
		  SELECT [Salvage_ID],
				 CASE WHEN Isnull(Min(Start_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Min(Start_Date), '2999-12-31') ELSE '2999-12-31' END AS Start_Date,
				 CASE WHEN Isnull(Min(Start_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Min(Start_Date), 112)), 29991231)  ELSE 29991231 END AS Start_Day_ID,
				 CASE WHEN Isnull(Max(End_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Max(End_Date), '2999-12-31') ELSE '2999-12-31' END AS End_Date,
				 CASE WHEN Isnull(Max(End_Date), '2999-12-31')  BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Max(End_Date), 112)), 29991231)  ELSE 29991231 END AS End_Day_ID
		  FROM   [BI_ASAP_Rep].[dbo].[Advance_Storage_Info] WITH(NOLOCK)
		  WHERE  Salvage_ID IN (SELECT DISTINCT Salvage_ID
								FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_advance_storage_info](@MinLSN, @MaxLSN, 'all with merge') WHERE [__$operation] = 5)
		  GROUP  BY Salvage_ID

/****************IBF DATES PART 1**************************/
INSERT INTO #TMP_IBNStarted (Salvage_ID, Update_DateTime,IBN_Run_Count)
SELECT IBNSH.Salvage_ID,MAX(IBNSH.Update_DateTime),IBNSH.IBN_Run_Count
FROM   [BI_ASAP_Rep].dbo.ibn_salvage_history IBNSH
	   INNER JOIN (SELECT DISTINCT Salvage_ID
				   FROM   [BI_ASAP_Rep].cdc.Fn_cdc_get_all_changes_dbo_ibn_salvage_history(@MinLSN, @MaxLSN, 'all')
				   WHERE  IBN_SP_Approved_Ind=1 AND IBN_Qualified_Ind=1 AND IBN_AC_Qualified_Ind=0
					  AND (IBN_Auction_Date IS NULL OR Update_DateTime>IBN_Auction_Date) AND Update_User_ID=1
				   UNION
				   SELECT DISTINCT Salvage_ID
				   FROM   [BI_ASAP_Rep].cdc.Fn_cdc_get_all_changes_dbo_ibn_salvage_history(@MinLSN, @MaxLSN, 'all')
				   WHERE  IBN_SP_Approved_Ind=1 AND IBN_Qualified_Ind=1 AND IBN_AC_Qualified_Ind=1) IBNSH_New
		 ON IBNSH.Salvage_ID=IBNSH_New.Salvage_ID
INNER JOIN BI_ASAP_REP.dbo.Salvage_Provider_Info SPI WITH (NOLOCK)
	ON IBNSH.Salvage_ID = SPI.Salvage_ID
WHERE (SPI.IBN_Run_Threshold_Reached_Ind = 0 OR (SPI.IBN_Run_Threshold_Reached_Ind = 1 AND IBNSH.IBN_Run_Count < SPI.IBN_Run_Count))
	AND SPI.IBN_Run_Count > 0
	AND IBNSH.IBN_SP_Approved_Ind=1 AND IBNSH.IBN_Qualified_Ind=1 AND IBNSH.IBN_AC_Qualified_Ind=0
	AND IBNSH.Update_User_ID=1
GROUP BY IBNSH.Salvage_ID,IBNSH.IBN_Run_Count	
ORDER  BY IBNSH.Salvage_ID,IBNSH.IBN_Run_Count	

INSERT INTO #TMP_IBNEnded (Salvage_ID, Update_DateTime)
SELECT IBNSH.Salvage_ID, IBNSH.Update_DateTime
FROM   [BI_ASAP_Rep].dbo.ibn_salvage_history IBNSH
	   INNER JOIN (SELECT DISTINCT Salvage_ID
				   FROM   [BI_ASAP_Rep].cdc.Fn_cdc_get_all_changes_dbo_ibn_salvage_history(@MinLSN, @MaxLSN, 'all')
				   WHERE  IBN_SP_Approved_Ind=1 AND IBN_Qualified_Ind=1 AND IBN_AC_Qualified_Ind=1
				   UNION
				   SELECT DISTINCT Salvage_ID
				   FROM   [BI_ASAP_Rep].cdc.Fn_cdc_get_all_changes_dbo_ibn_salvage_history(@MinLSN, @MaxLSN, 'all')
				   WHERE  IBN_SP_Approved_Ind=1 AND IBN_Qualified_Ind=1 AND IBN_AC_Qualified_Ind=0
					  AND (IBN_Auction_Date IS NULL OR Update_DateTime>IBN_Auction_Date) AND Update_User_ID=1) IBNSH_New
		 ON IBNSH.Salvage_ID=IBNSH_New.Salvage_ID
WHERE  IBNSH.IBN_SP_Approved_Ind=1 AND IBNSH.IBN_Qualified_Ind=1 AND IBNSH.IBN_AC_Qualified_Ind=1
ORDER  BY IBNSH.Salvage_ID, IBNSH.Update_DateTime 
			 
INSERT INTO #TMP_IBNEnded (Salvage_ID, Update_DateTime)
SELECT FSCT.Salvage_ID, Sold_Datetime
FROM   BI_ASAP_REP.dbo.Salvage_Info FSCT
	 INNER JOIN (SELECT Salvage_ID, Max(Update_DateTime) AS MaxStartDate FROM #TMP_IBNStarted GROUP BY Salvage_ID) IBNS
	   ON FSCT.Salvage_ID=IBNS.Salvage_ID
	 LEFT OUTER JOIN (SELECT Salvage_ID, Max(Update_DateTime) AS MaxENDDate FROM   #TMP_IBNEnded GROUP  BY Salvage_ID) IBNE
	   ON IBNS.Salvage_ID=IBNE.Salvage_ID AND Isnull(IBNE.MaxENDDate, '12/31/2999')<Isnull(IBNS.MaxStartDate, '12/31/2999')
WHERE  Isnull(FSCT.Sold_Datetime, '12/31/2999') Between @MinAllowableDate and @MaxAllowableDate

/****************END IBF DATES PART 1**************************/
		  
		 INSERT INTO #TMP_Anticipated_SalvageIDs(Salvage_ID)
		 SELECT Salvage_ID  FROM  [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_info](@MinLSN, @MaxLSN, 'all with merge') WHERE [__$operation] = 5
		 UNION 
		 SELECT Salvage_ID  FROM  [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_Title](@MinLSN, @MaxLSN, 'all with merge') WHERE [__$operation] = 5
		 UNION 
		 SELECT Salvage_ID  FROM  [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Item](@MinLSN, @MaxLSN, 'all with merge') WHERE [__$operation] = 5
		 
		  INSERT INTO #TMP_CT_ASD
					  ([Salvage_ID],
					   [Sold_Datetime],
					   [Auction_Datetime],
					   [Salvage_TitleDate],
					   [Anticipated_Sale_Date])
		 SELECT 
				SI.Salvage_ID
			   ,CASE WHEN Sold_DateTime BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Sold_DateTime ELSE '12/31/2999' END AS Sold_DateTime
			   ,CASE WHEN MAX(ASCH.Auction_DateTime) BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN  MAX(ASCH.Auction_DateTime) ELSE '12/31/2999' END AS Auction_Datetime
			   ,CASE WHEN Anticipated_Sale_Date BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Anticipated_Sale_Date ELSE '12/31/2999' END as SalvageTitleDate
			   ,CASE WHEN ISNULL(CASE WHEN SI.Sold_DateTime IS NULL  AND Max(ASCH.Auction_DateTime) < GETDATE()
														 THEN COALESCE(ST.Anticipated_Sale_Date,Max(ASCH.Auction_DateTime)) 
													 WHEN SI.Sold_DateTime IS NULL  AND Max(ASCH.Auction_DateTime) > GETDATE()
														  THEN COALESCE(Max(ASCH.Auction_DateTime),ST.Anticipated_Sale_Date)   
													 WHEN SI.Sold_DateTime IS NULL 
														  THEN COALESCE(Max(ASCH.Auction_DateTime),ST.Anticipated_Sale_Date)
												 ELSE '2999-12-31' END ,'2999-12-31')
				    BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN 
						ISNULL(CASE WHEN SI.Sold_DateTime IS NULL  AND Max(ASCH.Auction_DateTime) < GETDATE()
														 THEN COALESCE(ST.Anticipated_Sale_Date,Max(ASCH.Auction_DateTime)) 
													 WHEN SI.Sold_DateTime IS NULL  AND Max(ASCH.Auction_DateTime) > GETDATE()
														  THEN COALESCE(Max(ASCH.Auction_DateTime),ST.Anticipated_Sale_Date)   
													 WHEN SI.Sold_DateTime IS NULL 
														  THEN COALESCE(Max(ASCH.Auction_DateTime),ST.Anticipated_Sale_Date)
												 ELSE '2999-12-31' END ,'2999-12-31')
				 ELSE '12/31/2999' END as Anticipated_Sale_Date
		  FROM 
		  [BI_ASAP_Rep].dbo.Salvage_Info AS SI 
		  INNER JOIN #TMP_Anticipated_SalvageIDs T
		  ON SI.Salvage_ID = T.Salvage_ID
		  LEFT OUTER JOIN BI_ASAP_Rep.dbo.Salvage_Title as ST WITH (NOLOCK)
		  ON T.Salvage_ID = ST.Salvage_ID
		  LEFT OUTER JOIN  BI_ASAP_Rep.dbo.Auction_Item as AI WITH (NOLOCK)
		  ON T.Salvage_ID = AI.Salvage_ID
		  LEFT OUTER JOIN BI_ASAP_Rep.dbo.Auction_Schedule as ASCH WITH (NOLOCK)
		  ON AI.Auction_Schedule_ID = ASCH.Auction_Schedule_ID
		  GROUP BY 
		  SI.Salvage_ID,
		  Sold_DateTime,
		  Anticipated_Sale_Date

		  
		  --Update Anticipated_Sale_Date as 2999-12-31 for all cancelled stocks 
		  UPDATE #TMP_CT_ASD
		  SET 
		  Anticipated_Sale_Date = '2999-12-31'
		  FROM 
		  #TMP_CT_ASD
		  WHERE Salvage_ID IN (SELECT DISTINCT Salvage_ID
								 FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge')
								 WHERE  Salvage_Event_Code IN ('ASCN') AND  [__$operation] = 5)
		  
--Get all stocks with Auction Changes
INSERT INTO #TMPAuctionStockID
            (Salvage_ID)
SELECT DISTINCT Salvage_ID
FROM (SELECT Salvage_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Item](@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5) AI

--Changes to Auction_Entry
INSERT INTO #TMPAuctionStockID
            (Salvage_ID)
SELECT DISTINCT Salvage_ID
FROM (SELECT Auction_Item_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Entry](@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5) AE
INNER JOIN [BI_ASAP_Rep].[dbo].Auction_Item AI WITH (NOLOCK)
	ON AI.Auction_Item_ID = AE.Auction_Item_ID

--Changes to Auction_Schedule
INSERT INTO #TMPAuctionStockID
            (Salvage_ID)
SELECT DISTINCT Salvage_ID
FROM (SELECT Auction_Schedule_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Schedule](@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5) ASched
INNER JOIN [BI_ASAP_Rep].[dbo].Auction_Item AI WITH (NOLOCK)
	ON AI.Auction_Schedule_ID = ASched.Auction_Schedule_ID
		  
--===========================  Restore missing CertInDataTimes - Begin ================================================

Insert into  #TMP_StockIDnASAPSalvageID	(StockID,ASAPSalvageID)
Select	  ds.StockID, ds.ASAPSalvageID
From	DimStock			AS	ds WITH (NOLOCK)
  Join	FactStockCycleTime	AS	fsct WITH (NOLOCK)
	on	ds.StockID	=	fsct.StockID
  Join	FactStockEvent		AS	fse WITH (NOLOCK)
	on	ds.ASAPSalvageID = fse.ASAPSalvageID
  Join	DimStockEventType	AS	dset WITH (NOLOCK)
	on	fse.EventTypeID = dset.EventTypeID
Where	ds.IsStockSold	= 1
	AND	dset.EventCode = 'SDRM'
	AND	CONVERT(VARCHAR(10), FSCT.CertINDateTime, 101) = '12/31/2999'
Group by  ds.StockID, ds.ASAPSalvageID

Insert into  #TMP_ASAPSalvageID_CertInDate	(StockID, ASAPSalvageID, NewCertInDateTime)
Select 	  sas.StockID, sas.ASAPSalvageID, max(fse.EventDateTime)	AS NewCertInDateTime
From	#TMP_StockIDnASAPSalvageID  AS sas WITH (NOLOCK)
  Join	FactStockCycleTime	AS	fsct WITH (NOLOCK)
	on	sas.StockID	=	fsct.StockID
  Join	FactStockEvent		AS	fse WITH (NOLOCK)
	on	sas.ASAPSalvageID = fse.ASAPSalvageID
  Join	DimStockEventType	AS	dset WITH (NOLOCK)
	on	fse.EventTypeID = dset.EventTypeID
Where	dset.EventCode = 'SDRC'
Group by sas.StockID, sas.ASAPSalvageID

-------------------------------- Logic to Populate LastTitleAssistFormUpload DateTime  -----------------------------
INSERT INTO #TMP_LastTitleAssistForm
( ASAPSalvageID				,
 
 [LastTitleAssistFormUploadDateTime] )

SELECT Salvage_ID
       ,MAX(CASE WHEN Note_Text = 'Title Assist uploaded from CSAToday' THEN (Update_Datetime) END) AS [LastTitleAssistFormUploadDateTime]
   	
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5 
and  Note_Text = 'Title Assist uploaded from CSAToday'
GROUP BY Salvage_ID



		  INSERT INTO #TMP_CT_SalvageIDs (Salvage_ID)
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_Salvage_Info
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_Dispatch_DateTime
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_Pickup_DateTime
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_Settlement_DateTime
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_Earliest_Sale_DateTime
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_SEH_SalvageIDs
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_Cert_In_DateTime
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_BuyerPickup
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_Provider_Refund
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_CPSS_Datetime
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_ASI
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_IBNStarted
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_IBNEnded
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMP_CT_ASD
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM   #TMPAuctionStockID
		  UNION
		  SELECT DISTINCT ASAPSalvageID  FROM   #TMP_ASAPSalvageID_CertInDate
		  UNION
		  SELECT DISTINCT Salvage_ID  FROM #TMP_CT_Provider_Quoted_Release		  

		  CREATE UNIQUE CLUSTERED INDEX IX_TMP_CT_SalvageIDs
			ON #TMP_CT_SalvageIDs (Salvage_ID)
			WITH FILLFACTOR = 100;

		  INSERT INTO #TMP_FactStockCycleTime_Stage
					  (StockID,
					   ASAPSalvageID,
					   AdministrativeBranchNumber,
					   AssignmentDateTime,
					   AssignmentDayID,
					   ReleaseDateTime,
					   ReleaseDayID,
					   CheckInDateTime,
					   CheckinDayID,
					   LogInDateTime,
					   LogInDayID,
					   LogOutDateTime,
					   LogOutDayID,
					   ReceivedAllDocumentsDateTime,
					   ReceivedAllDocumentsDayID,
					   MissingPartsDatetime,
					   MissingPartsDayID,
					   SaleDocumentReceivedDateTime,
					   SaleDocumentReceivedDayID,
					   TitleInDateTime,
					   TitleInDayID,
					   DispatchDateTime,
					   DispatchDayID,
					   PickupDateTime,
					   PickupDayID,
					   SettlementDateTime,
					   SettlementDayID,
					   EarliestSaleDate,
					   EarliestSaleDayId,
					   DispatchPermanentlyCancelledDateTime,
					   DispatchPermanentlyCancelledDayID,
					   HoldReleaseDateTime,
					   HoldReleaseDayID,
					   PreBillPaidDateTime,
					   PreBillPaidDayID,
					   SoldDateTime,
					   SoldDayId,
					   AdvanceStorageVerifiedDate,
					   AdvanceStorageVerifiedDayID,
					   DateOfLossReportedDateTime,
					   DateOfLossReportedDayID,
					   InspectionDateTime,
					   InspectionDayID,
					   ProviderQuotedReleaseDateTime,
					   ProviderQuotedReleaseDayID,
					   RecoveryDatetime,
					   RecoveryDayID,
					   SettlementPaidDateTime,
					   SettlementPaidDayID,
					   VICInspectionDateTime,
					   VICInspectionDayID,
					   CancelDateTime,
					   CancelDayID,
					   FirstSoldDateTime,
					   FirstSoldDayID,
					   LossDateTime,
					   LossDayID,
					   SalesDocumentAppliedDatetime,
					   SalesDocumentAppliedDayID,
					   TitleApprovedByProviderDateTime,
					   TitleApprovedByProviderDayID,
					   CertINDateTime,
					   CertInDayID,
					   BuyerPickupDateTime,
					   BuyerPickupDayID,
					   LastProviderRefundDate,
					   LastProviderRefundDayID,
					   CurrentProcessStatusStartDate,
					   CurrentProcessStatusStartDayID,
					   SMVDDatetime,
					   SMVDDayID,
					  
					   ReleaseTimezonedDateTime,
					   ReleaseTimeZonedDayId,
					   PickupTimezonedDateTime,
					   PickupTimezonedDayID,
					   BusinessHoldTime,
					   CalendarHoldTime,
					   TitleProblemIdentifiedDate,
					   TitleProblemIdentifiedDayID,
					   TitleProblemResolvedDate,
					   TitleProblemResolvedDayID,
					   VehicleImagesLoadedDate,
					   VehicleImagesLoadedDayID,
					   CheckInImagesTakenDate,
					   CheckInImagesTakenDayID,
					   BuyerRenegeCancelSaleDate,
					   BuyerRenegeCancelSaleDayID,
					   CheckIssuedDate,
					   CheckIssuedDayID,
					   --PlacedOnHoldDate,PlacedOnHoldDayID ,OnHoldReleaseDate ,OnHoldReleaseDayID,
					   AdvanceStorageStartDate,
					   AdvanceStorageStartDayID,
					   AdvanceStorageEndDate,
					   AdvanceStorageEndDayID,
					   IBuyFastDisplayStartDate,
					   IBuyFastDisplayStartDayID,
					   IBuyFastDisplayEndDate,
					   IBuyFastDisplayEndDayID,
					   AnticipatedSaleDate,
					   AnticipatedSaleDayID,
					   LossTypeCode,
					   IBNDuration,
					   AllTitleDocumentsRecievedDatetime,   
			           AllTitleDocumentsRecievedDayID,  
			           StockClosedDatetime,                 
			           StockClosedDayID,
					   BuyerPaymentDateTime,
					   BuyerPaymentDayID,
					   FinalNMVTISDateTime,
					   FinalNMVTISDayID,
					   InitialNMVTISDateTime,
					   InitialNMVTISDayID,
					   MinSMVDDatetime,
					   MinSMVDDayID,
					   SellingBranchReceivedForwardedTitleDocs,
					   FirstReleaseDateTime,
					   FirstAllTitleDocumentsReceivedDateTime,
					   DateTimeReportedtoNICB,
					   [PartialDocAwaitingTitleDirectForm],
                       [PartialDocLienholderDocsRcvd],
                       [PartialDocOwnerDocsRcvd],
					   [LastTitleAssistFormUploadDateTime])                    
		  SELECT DS.StockID,
				 DS.ASAPSalvageID,
				 DS.AdministrativeBranchNumber,
				 COALESCE(SEH.ASGN_Event_Date, FSCT.AssignmentDateTime, '2999-12-31') AS AssignmentDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.ASGN_Event_Date, 112))), FSCT.AssignmentDayID, 29991231) AS AssignmentDayID,
				 COALESCE(SI.Release_DateTime, FSCT.ReleaseDateTime, '2999-12-31') AS ReleaseDateTime,
				 COALESCE(SI.Release_Day_ID, FSCT.ReleaseDayID, 29991231) AS ReleaseDayID,
				 COALESCE(SI.Checkin_DateTime, FSCT.CheckInDateTime, '2999-12-31') AS CheckInDateTime,
				 COALESCE(SI.Checkin_Day_ID, FSCT.CheckinDayID, 29991231) AS CheckinDayID,
				 COALESCE(SEH.Log_In_DateTime, FSCT.LogInDateTime, '2999-12-31') AS LogInDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Log_In_DateTime, 112))), FSCT.LogInDayID, 29991231) AS LogInDayID,
				 COALESCE(SEH.Log_Out_DateTime, FSCT.LogOutDateTime, '2999-12-31') AS LogOutDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Log_Out_DateTime, 112))), FSCT.LogOutDayID, 29991231) AS LogOutDayID,
				 COALESCE(SI.Received_All_Documents_DateTime, FSCT.ReceivedAllDocumentsDateTime, '2999-12-31') AS ReceivedAllDocumentsDateTime,
				 COALESCE(SI.Received_All_Documents_Day_ID, FSCT.ReceivedAllDocumentsDayID, 29991231) AS ReceivedAllDocumentsDayID,
				 COALESCE(SI.Missing_Parts_Datetime, FSCT.MissingPartsDatetime, '2999-12-31') AS MissingPartsDatetime,
				 COALESCE(SI.Missing_Parts_Day_ID, FSCT.MissingPartsDayID) AS MissingPartsDayID,
				 COALESCE(SEH.Sale_Document_Received_Datetime, FSCT.SaleDocumentReceivedDateTime, '2999-12-31') AS SaleDocumentReceivedDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Sale_Document_Received_Datetime, 112))), FSCT.SaleDocumentReceivedDayID, 29991231) AS SaleDocumentReceivedDayID,
				 COALESCE(SEH.Title_Received_Datetime, FSCT.TitleInDateTime, '2999-12-31') AS TitleInDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Title_Received_Datetime, 112))), FSCT.TitleInDayID, 29991231) AS TitleInDayID,
				 COALESCE(DD.Dispatch_DateTime, FSCT.DispatchDateTime, '2999-12-31') AS DispatchDateTime,
				 COALESCE(DD.Dispatch_Day_ID, FSCT.DispatchDayID, 29991231) AS DispatchDayID,
				 COALESCE(PD.Pickup_DateTime, FSCT.PickupDateTime, '2999-12-31') AS PickupDateTime,
				 COALESCE(PD.Pickup_Day_ID, FSCT.PickupDayID, 29991231) AS PickupDayID,
				 COALESCE(SD.Settlement_DateTime, FSCT.SettlementDateTime, '2999-12-31') AS SettlementDateTime,
				 COALESCE(SD.Settlement_Day_ID, FSCT.SettlementDayID, 29991231) AS SettlementDayID,
				 COALESCE(ESD.Earliest_Sale_DateTime, FSCT.EarliestSaleDate, '2999-12-31') AS EarliestSaleDatetime,
				 COALESCE(ESD.Earliest_Sale_Day_ID, FSCT.EarliestSaleDayID, 29991231) AS EarliestSaleDayID,
				 COALESCE(SI.Dispatch_Permanently_Cancelled_DateTime, FSCT.DispatchPermanentlyCancelledDateTime, '2999-12-31') AS DispatchPermanentlyCancelledDateTime,
				 COALESCE(SI.Dispatch_Permanently_Cancelled_DayID, FSCT.DispatchPermanentlyCancelledDayID, 29991231) AS  DispatchPermanentlyCancelledDayID,
				 COALESCE(SEH.Hold_Release_Date, FSCT.HoldReleaseDateTime, '2999-12-31') AS HoldReleaseDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Hold_Release_Date, 112))), FSCT.HoldReleaseDayID, 29991231) AS HoldReleaseDayID,
				 COALESCE(SI.PreBill_Paid_Date, FSCT.PreBillPaidDateTime, '2999-12-31') AS PreBillPaidDateTime,
				 COALESCE(SI.Prebill_day_ID, FSCT.PreBillPaidDayID, 29991231) AS PreBillPaidDayID,
				 COALESCE(SI.Sold_Date_Time, FSCT.SoldDateTime, '2999-12-31') AS SoldDateTime,
				 COALESCE(SI.Sold_Day_ID, FSCT.SoldDayID, 29991231) AS SoldDayID,
				 COALESCE(SEH.Advance_Storage_Verified_Date, FSCT.AdvanceStorageVerifiedDate, '2999-12-31') AS AdvanceStorageVerifiedDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Advance_Storage_Verified_Date, 112))), FSCT.AdvanceStorageVerifiedDayID, 29991231) AS AdvanceStorageVerifiedDayID,
				 COALESCE(SEH.Date_Of_Loss_Reported_DateTime, FSCT.DateOfLossReportedDateTime, '2999-12-31') AS DateOfLossReportedDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Date_Of_Loss_Reported_DateTime, 112))), FSCT.DateOfLossReportedDayID, 29991231) AS DateOfLossReportedDayID,
				 COALESCE(SEH.Inspection_DateTime, FSCT.InspectionDateTime, '2999-12-31') AS InspectionDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Inspection_DateTime, 112))), FSCT.InspectionDayID, 29991231) AS InspectionDayID,
				 COALESCE(PQR.Provider_Quoted_Release_DateTime, FSCT.ProviderQuotedReleaseDateTime, '2999-12-31') AS ProviderQuotedReleaseDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), PQR.Provider_Quoted_Release_DateTime, 112))), FSCT.ProviderQuotedReleaseDayID, 29991231) AS ProviderQuotedReleaseDayID,
				 COALESCE(SEH.Recovery_Datetime, FSCT.RecoveryDatetime, '2999-12-31') AS RecoveryDatetime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Recovery_Datetime, 112))), FSCT.RecoveryDayID, 29991231) AS RecoveryDayID,
				 COALESCE(SEH.Settlement_Paid_DateTime, FSCT.SettlementPaidDateTime, '2999-12-31') AS SettlementPaidDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Settlement_Paid_DateTime, 112))), FSCT.SettlementPaidDayID, 29991231) AS SettlementPaidDayID,
				 COALESCE(SEH.VIC_Inspection_DateTime, FSCT.VICInspectionDateTime, '2999-12-31') AS VICInspectionDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.VIC_Inspection_DateTime, 112))), FSCT.VICInspectionDayID, 29991231) AS VICInspectionDayID,
				 COALESCE(SEH.Cancel_DateTime, FSCT.CancelDateTime, '2999-12-31') AS CancelDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Cancel_DateTime, 112))), FSCT.CancelDayID, 29991231) AS CancelDayID,
				 COALESCE(SEH.First_Sold_DateTime, FSCT.FirstSoldDateTime, '2999-12-31') AS FirstSoldDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.First_Sold_DateTime, 112))), FSCT.FirstSoldDayID, 29991231) AS FirstSoldDayID,
				 COALESCE(SEH.Loss_DateTime, FSCT.LossDateTime, '2999-12-31') AS LossDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Loss_DateTime, 112))), FSCT.LossDayID, 29991231) AS LossDayID,
				 COALESCE(SEH.Sales_Document_Applied_Datetime, FSCT.SalesDocumentAppliedDatetime, '2999-12-31') AS SalesDocumentAppliedDatetime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Sales_Document_Applied_Datetime, 112))), FSCT.SalesDocumentAppliedDayID, 29991231) AS SalesDocumentAppliedDayID,
				 COALESCE(SEH.Title_Approved_By_Provider_DateTime, FSCT.TitleApprovedByProviderDateTime, '2999-12-31') AS TitleApprovedByProviderDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Title_Approved_By_Provider_DateTime, 112))), FSCT.TitleApprovedByProviderDayID, 29991231) AS TitleApprovedByProviderDayID,
				 COALESCE(CI.Cert_In_DateTime, FSCT.CertINDateTime, '2999-12-31') AS CertINDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), CI.Cert_In_DateTime, 112))), FSCT.CertInDayID, 29991231) AS CertInDayID,
				 COALESCE(BP.Buyer_Pickup_DateTime, FSCT.BuyerPickupDateTime, '2999-12-31') AS BuyerPickupDateTime,
				 COALESCE(BP.Buyer_Pickup_Day_ID, FSCT.BuyerPickupDayID, 29991231) AS BuyerPickupDayID,
				 COALESCE(PR.Last_Provider_Refund_Date, FSCT.LastProviderRefundDate, '2999-12-31') AS LastProviderRefundDate,
				 COALESCE(PR.Last_Provider_Refund_Day_ID, FSCT.LastProviderRefundDayID, 29991231) AS LastProviderRefundDayID,
				 COALESCE(CPSS.Current_Process_Status_Start_Date, FSCT.CurrentProcessStatusStartDate, '2999-12-31') AS CurrentProcessStatusStartDate,
				 COALESCE(CPSS.Current_Process_Status_Start_Day_ID, FSCT.CurrentProcessStatusStartDayID, 29991231) AS CurrentProcessStatusStartDayID,
				 COALESCE(SEH.SMVD_Datetime, FSCT.SMVDDatetime, '2999-12-31') AS SMVDDatetime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.SMVD_Datetime, 112))), FSCT.SMVDDayID, 29991231) AS SMVDDayID,
				
				
				
				 dbo.Fn_timezoneconvert(COALESCE(SI.Release_DateTime, FSCT.ReleaseDateTime, '2999-12-31'), DS.AdministrativeBranchNumber) AS ReleaseTimezonedDateTime,
				 CONVERT(CHAR(8), dbo.Fn_timezoneconvert(COALESCE(SI.Release_DateTime, FSCT.ReleaseDateTime
				 , '2999-12-31'), DS.AdministrativeBranchNumber), 112) AS ReleaseTimezonedDayID,
				 dbo.Fn_timezoneconvert(COALESCE(PD.Pickup_DateTime, FSCT.PickupDateTime, '2999-12-31'), DS.AdministrativeBranchNumber) AS PickupTimezonedDateTime,
				 CONVERT(CHAR(8), dbo.Fn_timezoneconvert(COALESCE(PD.Pickup_DateTime, FSCT.PickupDateTime, '2999-12-31'), DS.AdministrativeBranchNumber), 112) AS PickupTimezonedDayID,
				 COALESCE(THD.Hold_Time_Business, FSCT.BusinessHoldTime) AS BusinessHoldTime,
				 COALESCE(THD.Hold_Time_Calendar, FSCT.CalendarHoldTime) AS CalendarHoldTime,
				 COALESCE(SEH.Title_Problem_Identified_Datetime, FSCT.TitleProblemIdentifiedDate, '2999-12-31') AS TitleProblemIdentifiedDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Title_Problem_Identified_Datetime, 112))), FSCT.TitleProblemIdentifiedDayID, 29991231) AS TitleProblemIdentifiedDayID,
				 COALESCE(SEH.Title_Problem_Resolved_Datetime, FSCT.TitleProblemResolvedDate, '2999-12-31') AS TitleProblemResolvedDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Title_Problem_Resolved_Datetime, 112))), FSCT.TitleProblemResolvedDayID, 29991231) AS TitleProblemResolvedDayID,
				 COALESCE(SEH.Vehicle_Images_Loaded_Datetime, FSCT.VehicleImagesLoadedDate, '2999-12-31') AS VehicleImagesLoadedDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Vehicle_Images_Loaded_Datetime, 112))), FSCT.VehicleImagesLoadedDayID, 29991231) AS VehicleImagesLoadedDayID,
				 COALESCE(SEH.Check_In_Images_Taken_Datetime, FSCT.CheckInImagesTakenDate, '2999-12-31') AS CheckInImagesTakenDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Check_In_Images_Taken_Datetime, 112))), FSCT.CheckInImagesTakenDayID, 29991231) AS CheckInImagesTakenDayID,
				 COALESCE(SEH.Buyer_Renege_Cancel_Sale_Datetime, FSCT.BuyerRenegeCancelSaleDate, '2999-12-31') AS BuyerRenegeCancelSaleDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Buyer_Renege_Cancel_Sale_Datetime, 112))), FSCT.BuyerRenegeCancelSaleDayID, 29991231) AS BuyerRenegeCancelSaleDayID,
				 COALESCE(SEH.Check_Issued_Datetime, FSCT.CheckIssuedDate, '2999-12-31') AS CheckIssuedDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Check_Issued_Datetime, 112))), FSCT.CheckIssuedDayID, 29991231) AS CheckIssuedDayID,
				 COALESCE(ASI.[Start_Date], FSCT.AdvanceStorageStartDate, '2999-12-31') AS AdvanceStorageStartDate,
				 COALESCE(ASI.Start_Day_ID, FSCT.AdvanceStorageStartDayID, 29991231) AS AdvanceStorageStartDayID,
				 COALESCE(ASI.End_Date, FSCT.AdvanceStorageEndDate, '2999-12-31') AS AdvanceStorageEndDate,
				 COALESCE(ASI.End_Day_ID, FSCT.AdvanceStorageEndDayID, 29991231) AS AdvanceStorageEndDayID,
				 FSCT.IBuyFastDisplayStartDate,
				 FSCT.IBuyFastDisplayStartDayID,
				 FSCT.IBuyFastDisplayEndDate,
				 FSCT.IBuyFastDisplayEndDayID,
				 COALESCE(ASD.Anticipated_Sale_Date, FSCT.AnticipatedSaleDate, '2999-12-31') AS AnticipatedSaleDate,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), ASD.Anticipated_Sale_Date, 112))), FSCT.AnticipatedSaleDayID, 29991231) AS  AnticipatedSaleDayID,
				 DS.LossTypeCode,
				 FSCT.CalendarDaysSpentOnIBF,
				 COALESCE(SEH.All_Title_Documents_Received_Date,FSCT.AllTitleDocumentsRecievedDatetime, '2999-12-31') AS AllTitleDocumentsRecievedDatetime,
				  COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.All_Title_Documents_Received_Date, 112))),FSCT.AllTitleDocumentsRecievedDayID, 29991231) AS  AllTitleDocumentsRecievedDayID,
				 COALESCE(SEH.Stock_Closed_Date_Time, FSCT.StockClosedDateTime, '2999-12-31') AS StockClosedDatetime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.Stock_Closed_Date_Time, 112))), FSCT.StockClosedDayID, 29991231) AS  StockClosedDayID,
				 COALESCE(SEH.BYPD_Event_Date, FSCT.BuyerPaymentDateTime, '2999-12-31') AS BuyerPaymentDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.BYPD_Event_Date, 112))), FSCT.BuyerPaymentDayID, 29991231) AS BuyerPaymentDayID,
				 COALESCE(SEH.FinalNMVTISDate, FSCT.FinalNMVTISDateTime, '2999-12-31') AS FinalNMVTISDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.FinalNMVTISDate, 112))), FSCT.FinalNMVTISDayID, 29991231) AS FinalNMVTISDayID,
				 COALESCE(SEH.InitialNMVTISDate, FSCT.InitialNMVTISDateTime, '2999-12-31') AS InitialNMVTISDateTime,
				 COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEH.InitialNMVTISDate, 112))), FSCT.InitialNMVTISDayID, 29991231) AS InitialNMVTISDayID,
				 COALESCE(SEHM.Min_SMVD_Datetime,FSCT.MinSMVDDatetime, '2999-12-31') AS MinSMVDDatetime,
                COALESCE((CONVERT(INT, CONVERT(CHAR(8), SEHM.Min_SMVD_Datetime, 112))), FSCT.MinSMVDDayID,29991231) AS MinSMVDDayID,

				COALESCE(SEH.Selling_Branch_Received_Forwarded_Title_Docs,FSCT.SellingBranchReceivedForwardedTitleDocs, '2999-12-31') AS SellingBranchReceivedForwardedTitleDocs,
				--COALESCE(SEHM.First_Release_DateTime,FSCT.FirstReleaseDateTime, '2999-12-31') AS FirstReleaseDateTime,
				CASE WHEN ISNULL(FSCT.FirstReleaseDateTime,'12/31/2999') = '12/31/2999' THEN SI.Release_DateTime ELSE FSCT.FirstReleaseDateTime END AS FirstReleaseDateTime,
                COALESCE(SEHM.First_All_Title_Documents_Received_DateTime,FSCT.FirstAllTitleDocumentsReceivedDateTime, '2999-12-31') AS FirstAllTitleDocumentsReceivedDateTime,
				  COALESCE(SEH.DateTimeReportedtoNICB,FSCT.DateTimeReportedtoNICB, '2999-12-31') AS DateTimeReportedtoNICB,
				  COALESCE( SEH.[PartialDocAwaitingTitleDirectForm],FSCT.[PartialDocAwaitingTitleDirectForm], '2999-12-31')[PartialDocAwaitingTitleDirectForm],
                  COALESCE( SEH.[PartialDocLienholderDocsRcvd],FSCT.[PartialDocLienholderDocsRcvd], '2999-12-31')[PartialDocLienholderDocsRcvd],
                   COALESCE( SEH.[PartialDocOwnerDocsRcvd],FSCT.[PartialDocOwnerDocsRcvd], '2999-12-31')[PartialDocOwnerDocsRcvd],
				    COALESCE( LTAF.[LastTitleAssistFormUploadDateTime],FSCT.[LastTitleAssistFormUploadDateTime], '2999-12-31') [LastTitleAssistFormUploadDateTime]
		  FROM   #TMP_CT_SalvageIDs AS S
				 INNER JOIN dbo.DimStock AS DS WITH(NOLOCK)
				   ON S.Salvage_ID=DS.ASAPSalvageID
				 LEFT OUTER JOIN dbo.FactStockCycleTime AS FSCT WITH(NOLOCK)
				   ON DS.StockID=FSCT.StockID
				 LEFT OUTER JOIN #TMP_CT_Salvage_Info AS SI
				   ON S.Salvage_ID=SI.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_Dispatch_DateTime AS DD
				   ON S.Salvage_ID=DD.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_Pickup_DateTime AS PD
				   ON S.Salvage_ID=PD.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_Settlement_DateTime AS SD
				   ON S.Salvage_ID=SD.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_Earliest_Sale_DateTime AS ESD
				   ON S.Salvage_ID=ESD.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_SEH_Dates AS SEH
				   ON S.Salvage_ID=SEH.Salvage_ID
				  LEFT OUTER JOIN #TMP_CT_SEH_Dates_Min AS SEHM 
				   ON S.Salvage_ID=SEHM.Salvage_ID
				 
  
				 LEFT OUTER JOIN #TMP_CT_Cert_In_DateTime AS CI
				   ON S.Salvage_ID=CI.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_BuyerPickup AS BP
				   ON S.Salvage_ID=BP.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_Provider_Refund AS PR
				   ON S.Salvage_ID=PR.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_CPSS_Datetime AS CPSS
				   ON S.Salvage_ID=CPSS.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_ASI AS ASI
				   ON S.Salvage_ID=ASI.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_ASD AS ASD
				   ON S.Salvage_ID=ASD.Salvage_ID
				 LEFT OUTER JOIN #TMP_Hold_Durations AS THD
				   ON S.Salvage_ID = THD.Salvage_ID
				 LEFT OUTER JOIN #TMP_CT_Provider_Quoted_Release AS PQR
				   ON S.Salvage_ID = PQR.Salvage_ID
				  LEFT OUTER JOIN   #TMP_LastTitleAssistForm AS LTAF
				    ON S.Salvage_ID = LTAF.ASAPSalvageID
		  CREATE UNIQUE CLUSTERED INDEX IX_TMP_FactStockCycleTime_Stage
			ON #TMP_FactStockCycleTime_Stage (StockID)
			WITH FILLFACTOR = 100;
			
			
		

		  CREATE NONCLUSTERED INDEX IX_TMP_FSCT_SalavageID
			ON #TMP_FactStockCycleTime_Stage (ASAPSalvageID)
			WITH FILLFACTOR = 100;

--===========================  Restore missing CertInDataTimes - Update ================================================

UPDATE	#TMP_FactStockCycleTime_Stage	SET
	  CertINDateTime	=	tassc.NewCertInDateTime
	, CertInDayID		=	 CONVERT(INT, CONVERT(CHAR(8), tassc.NewCertInDateTime , 112))
From	#TMP_ASAPSalvageID_CertInDate	AS tassc
  INNER Join #TMP_FactStockCycleTime_Stage	AS fsct WITH (NOLOCK)
				on	fsct.StockID = tassc.StockID

--===========================  Restore missing CertInDataTimes - Finish ===============================================

       -- Update FirstSoldDateTime when FirstSoldDateTime > SoldDateTime R5.3 S14
          
		UPDATE #TMP_FactStockCycleTime_Stage SET
		FirstSoldDateTime = CASE WHEN FirstSoldDayID > SoldDayID
									THEN SoldDateTime ELSE FirstSoldDateTime END
		,FirstSoldDayID = CASE WHEN FirstSoldDayID > SoldDayId 
									THEN SoldDayId ELSE FirstSoldDayID END 									 
		FROM #TMP_FactStockCycleTime_Stage
		WHERE FirstSoldDayID > SoldDayId AND SoldDayId <> 29991231 




		UPDATE #TMP_FactStockCycleTime_Stage
		SET 
		TitleProblemResolvedDate =ISNULL(
                                     CASE WHEN AllTitleDocumentsRecievedDatetime < '2999-12-31' AND AllTitleDocumentsRecievedDatetime > TitleProblemIdentifiedDate THEN AllTitleDocumentsRecievedDatetime
                                          WHEN TitleInDateTime < '2999-12-31' AND TitleInDateTime > TitleProblemIdentifiedDate THEN TitleInDateTime
                                          WHEN CertINDateTime < '2999-12-31'  AND CertINDateTime > TitleProblemIdentifiedDate THEN CertInDatetime 
                                          WHEN StockClosedDateTime < '2999-12-31' AND StockClosedDateTime > TitleProblemIdentifiedDate THEN StockClosedDateTime END ,'2999-12-31')
		 FROM  #TMP_FactStockCycleTime_Stage TFSCT
		 INNER JOIN dbo.DimStock DS 
		 ON TFSCT.StockID = DS.StockID
		 WHERE 
		 DS.IsTitleProblem = 0 
		 AND TitleProblemIdentifiedDate <> '2999-12-31'
		 AND TitleProblemResolvedDate = '2999-12-31'


        
		UPDATE #TMP_FactStockCycleTime_Stage
		SET TitleProblemResolvedDayID = (CONVERT(INT, CONVERT(CHAR(8), TitleProblemResolvedDate, 112)))
 







	UPDATE #TMP_FactStockCycleTime_Stage SET 

	AdvanceStorageVerifiedDate = CASE WHEN AdvanceStorageVerifiedDate < @MinAllowableDate OR  AdvanceStorageVerifiedDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(AdvanceStorageVerifiedDate,'12/31/2999') END,
	AdvanceStorageVerifiedDayID = CASE WHEN AdvanceStorageVerifiedDayID < @MinAllowableDayId OR  AdvanceStorageVerifiedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(AdvanceStorageVerifiedDayID,29991231) END,
	DateOfLossReportedDateTime = CASE WHEN DateOfLossReportedDateTime < @MinAllowableDate OR DateOfLossReportedDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(DateOfLossReportedDateTime,'12/31/2999') END,
	DateOfLossReportedDayID = CASE WHEN DateOfLossReportedDayID < @MinAllowableDayId OR  DateOfLossReportedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(DateOfLossReportedDayID,29991231) END,
	InspectionDateTime = CASE WHEN InspectionDateTime < @MinAllowableDate OR InspectionDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(InspectionDateTime,'12/31/2999') END,
	InspectionDayID = CASE WHEN InspectionDayID < @MinAllowableDayId OR InspectionDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(InspectionDayID,29991231) END,
	ProviderQuotedReleaseDateTime = CASE WHEN ProviderQuotedReleaseDateTime < @MinAllowableDate OR ProviderQuotedReleaseDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL( ProviderQuotedReleaseDateTime,'12/31/2999') END,
	ProviderQuotedReleaseDayID = CASE WHEN ProviderQuotedReleaseDayID < @MinAllowableDayId OR ProviderQuotedReleaseDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(ProviderQuotedReleaseDayID,29991231) END,
	RecoveryDatetime = CASE WHEN RecoveryDatetime < @MinAllowableDate OR RecoveryDatetime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(RecoveryDatetime,'12/31/2999') END,
	RecoveryDayID = CASE WHEN RecoveryDayID < @MinAllowableDayId OR RecoveryDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(RecoveryDayID,29991231) END,
	SettlementDateTime = CASE WHEN SettlementDateTime < @MinAllowableDate OR SettlementDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(SettlementDateTime,'12/31/2999') END,
	SettlementDayID = CASE WHEN  SettlementDayID < @MinAllowableDayId OR  SettlementDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(SettlementDayID,29991231) END,
	VICInspectionDateTime = CASE WHEN VICInspectionDateTime < @MinAllowableDate OR VICInspectionDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(VICInspectionDateTime,'12/31/2999') END,
	VICInspectionDayID = CASE WHEN VICInspectionDayID < @MinAllowableDayId OR  VICInspectionDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(VICInspectionDayID,29991231) END,
	CancelDateTime = CASE WHEN CancelDateTime < @MinAllowableDate OR CancelDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(CancelDateTime,'12/31/2999') END,
	CancelDayID = CASE WHEN  CancelDayID <@MinAllowableDayId OR  CancelDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(CancelDayID,29991231) END,
	LossDateTime = CASE WHEN LossDateTime < @MinAllowableDate OR LossDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(LossDateTime,'12/31/2999') END,
	LossDayID = CASE WHEN LossDayID <@MinAllowableDayId OR  LossDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(LossDayID,29991231) END,
	SalesDocumentAppliedDatetime = CASE WHEN SalesDocumentAppliedDatetime < @MinAllowableDate OR SalesDocumentAppliedDatetime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(SalesDocumentAppliedDatetime,'12/31/2999') END,
	SalesDocumentAppliedDayID = CASE WHEN SalesDocumentAppliedDayID < @MinAllowableDayId OR SalesDocumentAppliedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(SalesDocumentAppliedDayID,29991231) END,
	TitleApprovedByProviderDateTime = CASE WHEN TitleApprovedByProviderDateTime < @MinAllowableDate OR TitleApprovedByProviderDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(TitleApprovedByProviderDateTime,'12/31/2999') END,
	TitleApprovedByProviderDayID = CASE WHEN TitleApprovedByProviderDayID < @MinAllowableDayId OR TitleApprovedByProviderDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(TitleApprovedByProviderDayID,29991231) END,
	ReleaseProblemIdentifiedDate = CASE WHEN ReleaseProblemIdentifiedDate < @MinAllowableDate OR ReleaseProblemIdentifiedDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(ReleaseProblemIdentifiedDate,'12/31/2999') END,
	ReleaseProblemIdentifiedDayID = CASE WHEN ReleaseProblemIdentifiedDayID < @MinAllowableDayId OR ReleaseProblemIdentifiedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(ReleaseProblemIdentifiedDayID,29991231) END,
	ReleaseProblemResolvedDate = CASE WHEN ReleaseProblemResolvedDate < @MinAllowableDate OR ReleaseProblemResolvedDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(ReleaseProblemResolvedDate,'12/31/2999') END,
	ReleaseProblemResolvedDayID = CASE WHEN  ReleaseProblemResolvedDayID <@MinAllowableDayId OR ReleaseProblemResolvedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(ReleaseProblemResolvedDayID,29991231) END,
	TitleProblemIdentifiedDate = CASE WHEN TitleProblemIdentifiedDate < @MinAllowableDate OR  TitleProblemIdentifiedDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(TitleProblemIdentifiedDate,'12/31/2999') END,
	TitleProblemIdentifiedDayID = CASE WHEN TitleProblemIdentifiedDayID < @MinAllowableDayId OR TitleProblemIdentifiedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(TitleProblemIdentifiedDayID,29991231) END,
	TitleProblemResolvedDate = CASE WHEN TitleProblemResolvedDate < @MinAllowableDate OR TitleProblemResolvedDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(TitleProblemResolvedDate,'12/31/2999') END,
	TitleProblemResolvedDayID = CASE WHEN  TitleProblemResolvedDayID <@MinAllowableDayId OR TitleProblemResolvedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(TitleProblemResolvedDayID,29991231) END,
	VehicleImagesLoadedDate = CASE WHEN VehicleImagesLoadedDate < @MinAllowableDate OR VehicleImagesLoadedDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(VehicleImagesLoadedDate,'12/31/2999') END,
	VehicleImagesLoadedDayID = CASE WHEN VehicleImagesLoadedDayID < @MinAllowableDayId OR VehicleImagesLoadedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(VehicleImagesLoadedDayID,29991231) END,
	CheckInDateTime = CASE WHEN CheckInDateTime < @MinAllowableDate OR CheckInDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(CheckInDateTime,'12/31/2999') END,
	CheckinDayID = CASE WHEN CheckinDayID < @MinAllowableDayId OR CheckinDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(CheckinDayID,29991231) END,
	BuyerRenegeCancelSaleDate = CASE WHEN BuyerRenegeCancelSaleDate < @MinAllowableDate OR BuyerRenegeCancelSaleDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(BuyerRenegeCancelSaleDate,'12/31/2999') END,
	BuyerRenegeCancelSaleDayID = CASE WHEN BuyerRenegeCancelSaleDayID < @MinAllowableDayId OR BuyerRenegeCancelSaleDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(BuyerRenegeCancelSaleDayID,29991231) END,
	CheckIssuedDate = CASE WHEN CheckIssuedDate < @MinAllowableDate OR CheckIssuedDate > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(CheckIssuedDate,'12/31/2999') END,
	CheckIssuedDayID = CASE WHEN CheckIssuedDayID < @MinAllowableDayId OR CheckIssuedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(CheckIssuedDayID,29991231) END,
	LogInDateTime = CASE WHEN LogInDateTime < @MinAllowableDate OR LogInDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(LogInDateTime,'12/31/2999') END,
	LogInDayID = CASE WHEN LogInDayID < @MinAllowableDayId OR LogInDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(LogInDayID,29991231) END,
	LogOutDateTime = CASE WHEN LogOutDateTime < @MinAllowableDate OR LogOutDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(LogOutDateTime,'12/31/2999') END,
	LogOutDayID = CASE WHEN LogOutDayID < @MinAllowableDayId OR LogOutDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(LogOutDayID,29991231) END,
	TitleInDateTime = CASE WHEN TitleInDateTime < @MinAllowableDate OR TitleInDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(TitleInDateTime,'12/31/2999') END,
	TitleInDayID = CASE WHEN TitleInDayID < @MinAllowableDayId OR TitleInDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(TitleInDayID,29991231) END,
	SaleDocumentReceivedDateTime = CASE WHEN SaleDocumentReceivedDateTime < @MinAllowableDate OR SaleDocumentReceivedDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(SaleDocumentReceivedDateTime,'12/31/2999') END,
	SaleDocumentReceivedDayID = CASE WHEN SaleDocumentReceivedDayID < @MinAllowableDayId OR SaleDocumentReceivedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(SaleDocumentReceivedDayID,29991231) END,
	AllTitleDocumentsRecievedDatetime = CASE WHEN AllTitleDocumentsRecievedDatetime < @MinAllowableDate OR AllTitleDocumentsRecievedDatetime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(AllTitleDocumentsRecievedDatetime,'12/31/2999') END,
	AllTitleDocumentsRecievedDayID = CASE WHEN AllTitleDocumentsRecievedDayID < @MinAllowableDayId OR AllTitleDocumentsRecievedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(AllTitleDocumentsRecievedDayID,29991231) END,
	StockClosedDatetime = CASE WHEN StockClosedDatetime < @MinAllowableDate OR StockClosedDatetime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(StockClosedDatetime,'12/31/2999') END,
	StockClosedDayID = CASE WHEN StockClosedDayID < @MinAllowableDayId OR StockClosedDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(StockClosedDayID,29991231) END,
	FirstSoldDateTime = CASE WHEN FirstSoldDateTime < @MinAllowableDate OR FirstSoldDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(FirstSoldDateTime,'12/31/2999') END,
	FirstSoldDayID = CASE WHEN FirstSoldDayID < @MinAllowableDayId OR FirstSoldDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(FirstSoldDayID,29991231) END, 
	BuyerPaymentDateTime = CASE WHEN BuyerPaymentDateTime < @MinAllowableDate OR BuyerPaymentDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(BuyerPaymentDateTime,'12/31/2999') END,
	BuyerPaymentDayID = CASE WHEN BuyerPaymentDayID < @MinAllowableDayId OR BuyerPaymentDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(BuyerPaymentDayID,29991231) END 
	FROM #TMP_FactStockCycleTime_Stage Stage 
		INNER JOIN #TMP_CT_SEH_Dates SEH 
			ON Stage.ASAPSalvageID = SEH.Salvage_ID


        INSERT INTO  #TMP_CT_Salvage_Provider_Info
		([StockID]   ,
		 [Update_DateTime]  ,
		 [ProviderID]  )
		SELECT DS.StockID,
		       SPI.Update_DateTime,
		       DS.CurrentProviderid
		FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Provider_Info](@MinLSN,@MaxLSN,'all with merge') AS SPI
                       INNER JOIN Dimstock  DS WITH (NOLOCK) 
                           ON  (DS.asapsalvageid = SPI.Salvage_id)
                        WHERE [__$operation] = 5

	INSERT INTO #TMP_CT_Salavage_Event_Checkin
		([StockID]  ,
		 [Checkin_Date] 
		)
		SELECT  DS.StockID,
		        min(Update_Datetime )
		FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge' ) AS SER
                       INNER JOIN Dimstock  DS WITH (NOLOCK) 
                          ON (DS.ASAPSalvageID = SER.Salvage_id and Salvage_Event_Code in ('CKI1'))	
        WHERE [__$operation] = 5
        GROUP BY StockID
	  


/***************IBF Dates Part 2*******************/
DELETE FROM IBNE
FROM   #TMP_IBNEnded IBNE
	   INNER JOIN #TMP_IBNStarted D
		 ON IBNE.Salvage_ID=D.Salvage_ID AND IBNE.Update_DateTime=D.Update_DateTime

DELETE FROM IBNE
FROM   #TMP_IBNEnded AS IBNE
	   LEFT OUTER JOIN (SELECT Salvage_ID, Min(Update_DateTime) AS IBN_Start_Date
						FROM   #TMP_IBNStarted
						GROUP  BY Salvage_ID) IBNS
		 ON IBNE.Salvage_ID=IBNS.Salvage_ID
WHERE  IBNE.Update_DateTime<IBN_Start_Date

DECLARE @Starts INT

SELECT @Starts=Count(*)
FROM   #TMP_IBNStarted

CREATE TABLE #TMP_Duration
  (Salvage_ID     INT,
   IBN_Start_Date DATETIME,
   IBN_End_Date   DATETIME,
   IBNDuration    DECIMAL(18, 9))

WHILE @Starts>0 BEGIN
	  INSERT INTO #TMP_Duration(Salvage_ID, IBN_Start_Date, IBN_End_Date, IBNDuration)
	  SELECT IBNS.Salvage_ID, IBN_Start_Date, IBN_End_Date, Cast(IBN_End_Date-IBN_Start_Date AS DECIMAL(18, 9)) AS IBNDuration
	  FROM   (SELECT Salvage_ID, Min(Update_DateTime) AS IBN_Start_Date
			  FROM   #TMP_IBNStarted
			  GROUP  BY Salvage_ID) IBNS
			 LEFT OUTER JOIN (SELECT Salvage_ID, Min(Update_DateTime) AS IBN_End_Date
							  FROM   #TMP_IBNEnded
							  GROUP  BY Salvage_ID) IBNE
			   ON IBNS.Salvage_ID=IBNE.Salvage_ID AND IBN_Start_Date<ISNULL(IBN_End_Date,'12/31/2999')

	  DELETE FROM #TMP_IBNStarted
	  FROM   #TMP_IBNStarted IBNS
			 INNER JOIN #TMP_Duration D
			   ON IBNS.Salvage_ID=D.Salvage_ID AND IBN_Start_Date=Update_DateTime

	  DELETE FROM IBNE FROM #TMP_IBNEnded AS IBNE
			 LEFT OUTER JOIN (SELECT Salvage_ID, Min(Update_DateTime) AS IBN_Start_Date
							  FROM   #TMP_IBNStarted
							  GROUP  BY Salvage_ID) IBNS
			   ON IBNE.Salvage_ID=IBNS.Salvage_ID
		WHERE  IBNE.Update_DateTime<IBN_Start_Date

	  DELETE FROM #TMP_IBNEnded FROM #TMP_IBNEnded IBNE
			 INNER JOIN #TMP_Duration D 
			 ON IBNE.Salvage_ID=D.Salvage_ID AND IBN_END_Date=Update_DateTime

	  SELECT @Starts=Count(*) FROM   #TMP_IBNStarted
  END

UPDATE FSCT
SET    IBNDuration=Total_IBNDuration,
	   IBuyFastDisplayStartDate=MIN_IBN_Start_Date,
	   IBuyFastDisplayEndDate=MAX_IBN_End_Date
FROM   #TMP_FactStockCycleTime_Stage FSCT
	   INNER JOIN dbo.DimStock DS
		 ON FSCT.StockID=DS.StockID
	   INNER JOIN (SELECT Salvage_ID, Count(*) AS Count, Min(IBN_Start_Date) AS MIN_IBN_Start_Date,
						  Max(IBN_End_Date) AS MAX_IBN_End_Date, Sum(IBNDuration) AS Total_IBNDuration
				   FROM   #TMP_Duration
				   GROUP  BY Salvage_ID) IBFDays
		 ON DS.ASAPSalvageID=IBFDays.Salvage_ID

UPDATE FSCT
SET    IBuyFastDisplayEndDate='12/31/2999'
FROM   #TMP_FactStockCycleTime_Stage FSCT
	   INNER JOIN dbo.DimStock DS
		 ON FSCT.StockID=DS.StockID
	   INNER JOIN (SELECT Salvage_ID, Max(IBN_Start_Date) AS MAX_IBN_Start_Date
				   FROM   #TMP_Duration
				   GROUP  BY Salvage_ID) IBFDays
		 ON DS.ASAPSalvageID=IBFDays.Salvage_ID AND IBuyFastDisplayEndDate<MAX_IBN_Start_Date

UPDATE FSCT SET IBuyFastDisplayEndDate='12/31/2999' FROM #TMP_FactStockCycleTime_Stage FSCT WHERE IBuyFastDisplayEndDate IS NULL OR IBuyFastDisplayStartDate IS NULL
UPDATE FSCT SET IBuyFastDisplayStartDate='12/31/2999' FROM #TMP_FactStockCycleTime_Stage FSCT WHERE IBuyFastDisplayStartDate IS NULL

UPDATE FSCT SET IBuyFastDisplayStartDayID=Cast(CONVERT(CHAR(8), IBuyFastDisplayStartDate, 112) AS INT),
				IBuyFastDisplayEndDayID=Cast(CONVERT(CHAR(8), IBuyFastDisplayEndDate, 112) AS INT)
	FROM #TMP_FactStockCycleTime_Stage FSCT 

/****************END IBF DATES PART 2*************;-)*******/

/****************Update Auction Dates***********************/

UPDATE FSCT
SET    AuctionDateTime = CASE WHEN DS.IsStockSold <= 1 THEN ISNULL(MAXAuctionDateTime,'12/31/2999') ELSE '12/31/2999' END,
		AuctionDayID = CASE WHEN DS.IsStockSold <= 1 THEN CAST(CONVERT(CHAR(8),ISNULL(MAXAuctionDateTime,'12/31/2999'),112) AS INT) ELSE 29991231 END
FROM   #TMP_FactStockCycleTime_Stage FSCT
       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON FSCT.StockID=DS.StockID
       INNER JOIN (SELECT StockID,
                          Max(AuctionDateTime) AS MAXAuctionDateTime
                   FROM   dbo.DimAuctionItem WITH (NOLOCK)
					--WHERE AuctionDateTime <= GETDATE()                
					GROUP  BY StockID) MaxAuction
         ON FSCT.StockID=MaxAuction.StockID

-- TFS 44394 CHANGE DATE LOGIC AS PART OF DW MERGE
	    --- Auction DateTime

UPDATE FSCT
SET    AuctionDateTime = ISNULL(MAXAuctionDateTime,'12/31/2999'),
		AuctionDayID = CAST(CONVERT(CHAR(8),ISNULL(MAXAuctionDateTime,'12/31/2999'),112) AS INT)
FROM   #TMP_FactStockCycleTime_Stage FSCT
       INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
         ON FSCT.StockID=DS.StockID
       INNER JOIN (SELECT ds.StockID
						,MAX(st.Anticipated_Sale_Date) MAXAuctionDateTime
					FROM BI_Asap_rep.dbo.Salvage_Title st WITH (NOLOCK)
					INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
						ON st.Salvage_ID = DS.ASAPSalvageID
					GROUP BY ds.StockID) MaxAuction
         ON FSCT.StockID=MaxAuction.StockID
		 WHERE DS.IsStockSold = 0 and AuctionDateTime = '12/31/2999'

UPDATE FSCT
SET    OriginalAuctionDateTime = ISNULL(MinAuctionDateTime,'12/31/2999'),
		OriginalAuctionDayID = CAST(CONVERT(CHAR(8),ISNULL(MinAuctionDateTime,'12/31/2999'),112) AS INT)
FROM   #TMP_FactStockCycleTime_Stage FSCT
       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON FSCT.StockID=DS.StockID
       INNER JOIN (SELECT ai2.StockID,
                          Min(ai2.AuctionDatetime) AS MinAuctionDateTime
                   FROM   dbo.DimAuctionItem ai2 WITH (NOLOCK)
                   WHERE AuctionDateTime <= GETDATE()
				   GROUP  BY ai2.StockID) MA
         ON FSCT.StockID=MA.StockID 

	UPDATE #TMP_FactStockCycleTime_Stage SET 
	AuctionDateTime = CASE WHEN AuctionDateTime < @MinAllowableDate OR AuctionDateTime > @MaxAllowableDate THEN '12/31/2999' ELSE ISNULL(AuctionDateTime,'12/31/2999') END,
	AuctionDayID = CASE WHEN AuctionDayID < @MinAllowableDayId OR AuctionDayID > @MaxAllowableDayID THEN 29991231 ELSE ISNULL(AuctionDayID,29991231) END 
	FROM #TMP_FactStockCycleTime_Stage Stage 

/****************END Update Auction Dates***********************/


		  INSERT INTO #TMP_Release_Problem_DateTime_By_SalvageID
					  (Salvage_ID,
					   Release_Problem_Start_DateTime,
					   Release_Problem_End_DateTime)
		  SELECT S.Salvage_ID,
				 Release_Problem_Start_DateTime = FSEH1.EventDateTime,
				 Release_Problem_End_DateTime = Isnull(FSEH5.EventDateTime, Cast('2999-12-31' AS DATETIME))
		  FROM   #TMP_CT_SalvageIDs AS S WITH (NOLOCK)
				 INNER JOIN dbo.FactStockEvent AS FSEH1 WITH (NOLOCK)
				   ON S.Salvage_ID=FSEH1.ASAPSalvageID
					  AND FSEH1.EventTypeID=(SELECT EventTypeID
											 FROM   dbo.DimStockEventType WITH (NOLOCK)
											 WHERE  EventCode='RLPR')
				 LEFT OUTER JOIN dbo.FactStockEvent AS FSEH3 WITH (NOLOCK)
				   ON FSEH1.ASAPSalvageID=FSEH3.ASAPSalvageID
					  AND FSEH3.EventTypeID=(SELECT EventTypeID
											 FROM   dbo.DimStockEventType WITH (NOLOCK)
											 WHERE  EventCode='RLPR')
					  AND FSEH3.EventDateTime=(SELECT TOP 1 FSEH2.EventDateTime
											   FROM   dbo.FactStockEvent AS FSEH2 WITH (NOLOCK)
											   WHERE  FSEH2.EventTypeID=(SELECT EventTypeID
																		 FROM   dbo.DimStockEventType
																		 WHERE  EventCode='RLPR')
												  AND FSEH1.ASAPSalvageID=FSEH2.ASAPSalvageID
												  AND FSEH2.EventDateTime>FSEH1.EventDateTime
											   ORDER  BY FSEH2.EventDateTime)
				 LEFT OUTER JOIN dbo.FactStockEvent AS FSEH5 WITH (NOLOCK)
				   ON FSEH1.ASAPSalvageID=FSEH5.ASAPSalvageID
					  AND FSEH5.EventTypeID=(SELECT EventTypeID
											 FROM   dbo.DimStockEventType WITH (NOLOCK)
											 WHERE  EventCode='RLRS')
					  AND FSEH5.EventDateTime=(SELECT TOP 1 FSEH4.EventDateTime
											   FROM   dbo.FactStockEvent AS FSEH4 WITH (NOLOCK)
											   WHERE  FSEH4.EventTypeID=(SELECT EventTypeID
																		 FROM   dbo.DimStockEventType WITH (NOLOCK)
																		 WHERE  EventCode='RLRS')
												  AND FSEH4.ASAPSalvageID=FSEH1.ASAPSalvageID
												  AND FSEH4.EventDateTime>FSEH1.EventDateTime
												  AND FSEH4.EventDateTime<Isnull(FSEH3.EventDateTime, Cast('2999-12-31' AS DATETIME))
											   ORDER  BY FSEH4.EventDateTime DESC)

		  INSERT INTO #TMP_Release_Problem_Dates
					  (Salvage_ID,
					   Release_Problem_Start_DateTime,
					   Release_Problem_End_DateTime,
					   Administrative_Branch_Number)
		  SELECT RP.Salvage_ID,
				 CASE
				   WHEN RP.Release_Problem_Start_DateTime<SI.Assignment_DateTime THEN SI.Assignment_DateTime
				   ELSE RP.Release_Problem_Start_DateTime
				 END AS Release_Problem_Start_DateTime,
				 CASE
				   WHEN RP.Release_Problem_End_DateTime>BD.BusinessDateTime THEN BD.BusinessDateTime
				   ELSE RP.Release_Problem_End_DateTime
				 END AS Release_Problem_End_DateTime,
				 S.Administrative_Branch_Number
		  FROM   #TMP_Release_Problem_DateTime_By_SalvageID AS RP WITH(NOLOCK)
				 INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Info AS SI WITH(NOLOCK)
				   ON RP.Salvage_ID=SI.Salvage_ID
				 INNER JOIN [BI_ASAP_Rep].dbo.Salvage AS S WITH(NOLOCK)
				   ON RP.Salvage_ID=S.Salvage_ID
				 CROSS JOIN dbo.BusinessDay AS BD WITH(NOLOCK)

		  CREATE CLUSTERED INDEX [IX_TMP_Release_Problem_Dates]
			ON #TMP_Release_Problem_Dates([Salvage_ID], [Release_Problem_Start_DateTime])
			WITH FILLFACTOR = 100

		  /*****Get Release Problem Time Data*****/
		  INSERT INTO #TMP_Release_Problem_Time
					  (Salvage_ID,
					   Release_Problem_Time_Business,
					   Release_Problem_Time_Calendar)
		  SELECT Salvage_ID,
				 Sum([dbo].[Fn_edwbusinesshours](Release_Problem_Start_DateTime, Release_Problem_End_DateTime, Administrative_Branch_Number)) AS
				 Release_Problem_Time_Business,
				 Sum(Cast(Release_Problem_End_DateTime AS FLOAT)-Cast(Release_Problem_Start_DateTime AS FLOAT)) AS Release_Problem_Time_Calendar
		  FROM   #TMP_Release_Problem_Dates WITH(NOLOCK)
		  GROUP  BY Salvage_ID

/***ProviderSoldDate***/

CREATE TABLE #TMP_PSD_FINAL
(
  StockID INT,
  ASAPSalvageID INT,
  ProviderSoldDate DATETIME,
  AgreementTermTypeCode varchar(4),
  SoldDateTime DATETIME
)


INSERT INTO #TMP_PSD_FINAL
(
  StockID ,
  ASAPSalvageID,
  ProviderSoldDate,
  AgreementTermTypeCode,
  SoldDateTime
   
 )
SELECT 
       DS.StockID
       ,DS.ASAPSalvageID
      ,CASE WHEN DS.AgreementTermTypeCode IN ('CFF','POS') THEN FST.SoldDateTime
            WHEN DS.AgreementTermTypeCode IN ('PAA') THEN FST.SettlementDateTime ELSE '2999-12-31' END AS ProviderSoldDate
      ,DS.AgreementTermTypeCode
      ,FST.SoldDateTime
FROM DimStock DS WITH(NOLOCK)
INNER JOIN #TMP_FactStockCycleTime_Stage FST ON DS.StockID = FST.StockID 
WHERE DS.AgreementTermTypeCode IN ('CFF','POS','PAA')  


---TO get SoldDate/BulkApprovedDate before the final settlement for renege/cancel

CREATE TABLE #TMP_SEHA
(
  Salvage_ID INT,
  Event_DateTime DATETIME,
  Salvage_Event_Code varchar(4)
)
INSERT INTO #TMP_SEHA
(
  Salvage_ID,
  Event_DateTime,
  Salvage_Event_Code 
)
SELECT 
	  SEHA.Salvage_ID
      ,SEHA.Event_DateTime
      ,SEHA.Salvage_Event_Code
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History] (@MinLSN,@MaxLSN,'all with merge') SEHA
WHERE SEHA.Salvage_Event_Code in ( 'SAL4','SAL9','FSEN','BYRE','BYCN') 
AND  [__$operation] = 5
UNION
SELECT 
  SEH.Salvage_ID
      ,SEH.Event_DateTime
      ,SEH.Salvage_Event_Code
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History] (@MinLSN,@MaxLSN,'all with merge') SEHA
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Event_History SEH ON SEHA.Salvage_ID = SEH.Salvage_ID 
WHERE SEH.Salvage_Event_Code in ( 'SAL4','SAL9','FSEN','BYRE','BYCN') 
AND  [__$operation] = 5


--- For reneges/cancel after settlement


CREATE TABLE #TMP_BRC
(
  Salvage_ID INT,
  EventDate DATETIME
)
INSERT INTO #TMP_BRC
(
  Salvage_ID ,
  EventDate 
)

SELECT Salvage_ID,MAX(Event_DateTime) AS EventDate 
FROM #TMP_SEHA 
WHERE Salvage_Event_Code IN ('BYRE','BYCN') 
GROUP BY Salvage_ID 

CREATE TABLE #TMP_PSD
(
  Salvage_ID INT,
  ProviderSoldDate DATETIME
)
INSERT INTO #TMP_PSD
(
  Salvage_ID ,
  ProviderSoldDate 
)
SELECT TR.Salvage_ID ,MAX(TR.Event_DateTime) AS ProviderSoldDate 
FROM 
( SELECT Salvage_ID,MAX(Event_DateTime) AS EventDate 
  FROM #TMP_SEHA 
  WHERE Salvage_Event_Code = 'FSEN' 
  GROUP BY Salvage_ID) T
INNER JOIN #TMP_SEHA TR ON T.Salvage_ID = TR.Salvage_ID
INNER JOIN #TMP_BRC TB ON TR.Salvage_ID = TB.Salvage_ID
WHERE TR.Event_DateTime <  T.EventDate 
AND TR.Salvage_Event_Code IN ('SAL4','SAL9') 
AND T.EventDate < TB.EventDate
GROUP BY TR.Salvage_ID


--Update the #TMP_PSD_FINAL with the final ProviderSoldDate

UPDATE TPF
SET ProviderSoldDate = TP.ProviderSoldDate
FROM #TMP_PSD_FINAL TPF
INNER JOIN #TMP_PSD TP ON TPF.ASAPSalvageID = TP.Salvage_ID
WHERE TPF.AgreementTermTypeCode = 'PAA'


--Check for Settlement Void after the FinalSettlement 

CREATE TABLE #TMP_STLV
( Salvage_ID INT, 
  Salvage_Event_Code varchar(4),
  ProviderSoldDate DATETIME,
  EventDate DATETIME
)
 INSERT INTO #TMP_STLV
( Salvage_ID , 
  Salvage_Event_Code, 
  ProviderSoldDate ,
  EventDate 
)

 SELECT TSE.Salvage_ID 
  ,TSE.Salvage_Event_Code
  ,'2999-12-31' AS ProviderSoldDate
  ,T.EventDate
 FROM 
 ( SELECT Salvage_ID,MAX(Event_DateTime) AS EventDate FROM #TMP_SEHA WHERE Salvage_Event_Code = 'FSEN' GROUP BY Salvage_ID) T
 INNER JOIN #TMP_SEHA TSE ON T.Salvage_ID = TSE.Salvage_ID
 WHERE TSE.Event_DateTime >  T.EventDate AND TSE.Salvage_Event_Code IN ('STLV')

--Update for PAA and CFF/POS stocks

UPDATE TPF
SET ProviderSoldDate = CASE WHEN TPF.AgreementTermTypeCode = 'PAA' THEN TS.ProviderSoldDate
                            WHEN TPF.AgreementTermTypeCode IN ('CFF','POS') THEN TPF.ProviderSoldDate ELSE '2999-12-31' END
FROM #TMP_PSD_FINAL TPF
INNER JOIN #TMP_STLV TS ON TPF.ASAPSalvageID = TS.Salvage_ID 

 

--Update NULL when buyer renege/cancel after SettlementVoid

UPDATE TPF
SET ProviderSoldDate = '2999-12-31'
FROM #TMP_PSD_FINAL TPF
INNER JOIN #TMP_STLV TS ON TPF.ASAPSalvageID = TS.Salvage_ID 
INNER JOIN #TMP_BRC TBC ON TS.Salvage_ID = TBC.Salvage_ID
WHERE TBC.EventDate > TS.EventDate 

--Update Sold Date for CFF/POS stocks after reneges 

UPDATE TPF
SET ProviderSoldDate = TPF.SoldDateTime 
FROM #TMP_PSD_FINAL TPF 
INNER JOIN #TMP_SEHA TR on TPF.ASAPSalvageID = tr.Salvage_ID 
INNER JOIN #TMP_STLV TS ON TPF.ASAPSalvageID = TS.Salvage_ID
INNER JOIN #TMP_BRC TBC ON TS.Salvage_ID = TBC.Salvage_ID
WHERE TR.Event_DateTime > TS.EventDate 
AND TBC.EventDate < TR.Event_DateTime
AND TR.salvage_event_code IN ('SAL4','SAL9') AND TPF.AgreementTermTypeCode IN ('CFF','POS')


--Update ProviderSoldDate in #TMP_FactStockCycleTime_Stage

UPDATE FST
SET ProviderSoldDateTime = ISNULL(TPF.ProviderSoldDate,'2999-12-31'),
    ProviderSoldDayID  = ISNULL(CONVERT(INT, CONVERT(CHAR(8),TPF.ProviderSoldDate, 112)), 29991231)
FROM #TMP_FactStockCycleTime_Stage FST WITH(NOLOCK)
INNER JOIN #TMP_PSD_FINAL TPF ON FST.StockID = TPF.StockID

--Update ProviderSoldDate NULL for 'Title Work Only' and cancelled stocks

UPDATE FST 
SET FST.ProviderSoldDateTime = '2999-12-31',
    FST.ProviderSoldDayID = 29991231
FROM #TMP_FactStockCycleTime_Stage FST WITH(NOLOCK)
INNER JOIN #TMP_PSD_FINAL TPF ON FST.StockID = TPF.StockID
INNER JOIN DIMSTOCK DS WITH(NOLOCK) ON FST.StockID = DS.StockID
WHERE (DS.AssignmentTypeCode = 'TTL' OR DS.AssignmentTypeCode = 'TOW' OR DS.IsCancel = 1)

-------Update MinSMVDDatetime--------------
UPDATE FST 
SET MinSMVDDatetime = SMVDDatetime,
    MinSMVDDayID = SMVDDayID
FROM #TMP_FactStockCycleTime_Stage FST WITH(NOLOCK)
WHERE ISNULL(MinSMVDDatetime,'12/31/2999') > ISNULL(SMVDDatetime,'12/31/2999')

 ----Durations

		  INSERT INTO #TMP_FSCTDuraions_Stage
					  (StockID,
					   ASAPSalvageID,
					   BusinessAssignmentToCancel,
					   CalendarAssignmentToCancel,
					   BusinessAssignmentToPickup,
					   CalendarAssignmentToPickup,
					   BusinessAssignmentToRelease,
					   CalendarAssignmenttoRelease,
					   BusinessAssignmentToSold,
					   CalendarAssignmenttoSold,
					   BusinessAssignmentToTitleIn,
					   CalendarAssignmenttoTitleIn,
					   BusinessBuyerPaymentToPickup,
					   CalendarBuyerPaymenttoPickup,
					   BusinessCertIntoSold,
					   CalendarCertIntoSold,
					   BusinessFNOLtoAssignment,
					   CalendarFNOLtoAssignment,
					   BusinessLosstoAssignment,
					   CalendarLosstoAssignment,
					   BusinessLosstoFNOL,
					   CalendarLosstoFNOL,
					   BusinessLosstoPickup,
					   CalendarLosstoPickup,
					   BusinessPickuptoTitleIn,
					   CalendarPickuptoTitleIn,
					   BusinessReleasetoPickup,
					   CalendarReleasetoPickup,
					   BusinessSettledToPaid,
					   CalendarSettledtoPaid,
					   BusinessSMVDtoCertIN,
					   CalendarSMVDtoCertIn,
					   BusinessSoldtoBuyerPayment,
					   CalendarSoldtoBuyerPayment,
					   BusinessSoldtoSettlement,
					   CalendarSoldtoSettlement,
					   BusinessTitleIntoSMVD,
					   CalendarTitleIntoSMVD,
					   BusinessReleaseProblemTime,
					   CalendarReleaseProblemTime,
					   BusinessPickupToCheckIn,
					   CalendarPickuptoCheckIn,
					   BusinessLogIntoCheckIn,
					   CalendarLogIntoCheckIn,
					   EffectiveDaysReleasetoPickup,
					   BusinessLossToPickupTheftRule,
					   CalendarLossToPickupTheftRule,
					   BusinessCertInToAuction,
					   CalendarCertInToAuction,
					   BusinessAuctionToSettlement,
					   CalendarAuctionToSettlement,
					   CalendarCertInToSoldProvider ,
					   BusinessCertInToSoldProvider,
                       CalendarSoldProviderToSettlement,
					   BusinessSoldProviderToSettlement ,
					   CalendarAssignmentToSoldProvider ,
					   BusinessAssignmentToSoldProvider ,
					   CalendarSoldProviderToBuyerPayment ,
					   BusinessSoldProviderToBuyerPayment ,
					   DaysInPhysicalInventoryProvider ,
					   DaysInPipelineInventoryProvider 
					 
						
						)
		  SELECT StockID,
				 FSCT.ASAPSalvageID,
				 (CASE
					WHEN AssignmentDayID<29991231
						 AND CancelDayID<29991231 THEN
					  CASE
						WHEN CancelDatetime<AssignmentDatetime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](AssignmentDatetime, CancelDatetime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessAssignmentToCancel,
				 (CASE
					WHEN AssignmentDayID<29991231
						 AND CancelDayID<29991231 THEN
					  CASE
						WHEN CancelDatetime<AssignmentDatetime THEN 0
						ELSE Cast(CancelDatetime AS FLOAT)-Cast(AssignmentDatetime AS FLOAT)
					  END
				  END) AS CalendarAssignmentToCancel,
				 (CASE
					WHEN AssignmentDayID<29991231
						 AND PickupDayID<29991231 THEN
					  CASE
						WHEN PickupDateTime<AssignmentDatetime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](AssignmentDatetime, PickupDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessAssignmentToPickup,
				 (CASE
					WHEN AssignmentDayID<29991231
						 AND PickupDayID<29991231 THEN
					  CASE
						WHEN PickupDateTime<AssignmentDatetime THEN 0
						ELSE Cast(PickupDateTime AS FLOAT)-Cast(AssignmentDatetime AS FLOAT)
					  END
				  END) AS CalendarAssignmentToPickup,
				 (CASE
					WHEN AssignmentDayID<29991231
						 AND ReleaseDayID<29991231 THEN
					  CASE
						WHEN ReleaseDateTime<AssignmentDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](AssignmentDatetime, ReleaseDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessAssignmentToRelease,
				 CASE
				   WHEN AssignmentDayID<29991231
						AND ReleaseDayID<29991231 THEN
					 CASE
					   WHEN ReleaseDateTime<AssignmentDateTime THEN 0
					   ELSE Cast(ReleaseDateTime AS FLOAT)-Cast(AssignmentDateTime AS FLOAT)
					 END
				 END CalendarAssignmenttoRelease,
				 (CASE
					WHEN AssignmentDayID<29991231
						 AND SoldDayId<29991231 THEN
					  CASE
						WHEN SoldDateTime<AssignmentDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](AssignmentDatetime, SoldDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessAssignmentToSold,
				 CASE
				   WHEN AssignmentDayID<29991231
						AND SoldDayID<29991231 THEN
					 CASE
					   WHEN SoldDateTime<AssignmentDateTime THEN 0
					   ELSE (Cast(SoldDateTime AS FLOAT)-Cast(AssignmentDateTime AS FLOAT))
					 --- CalendarHoldTime 
					 END
				 END CalendarAssignmenttoSold,
				 --(CASE
					--WHEN AssignmentDayID<29991231
					--	 AND TitleInDayID<29991231 THEN
					--  CASE
					--	WHEN TitleInDateTime<AssignmentDateTime THEN 0
					--	ELSE [dbo].[Fn_edwbusinesshours](AssignmentDatetime, TitleInDateTime, AdministrativeBranchNumber)
					--  END
				 -- END)/9 AS BusinessAssignmentToTitleIn,
				 --CASE
				 --  WHEN AssignmentDayID<29991231
					--	AND TitleInDayID<29991231 THEN
					-- CASE
					--   WHEN TitleInDateTime<AssignmentDateTime THEN 0
					--   ELSE Cast(TitleInDateTime AS FLOAT)-Cast(AssignmentDateTime AS FLOAT)
					-- END
				 --END CalendarAssignmenttoTitleIn,
				        (CASE WHEN AssignmentDayID<29991231
						THEN CASE WHEN AllTitleDocumentsRecievedDayID<29991231
									THEN CASE WHEN AllTitleDocumentsRecievedDateTime<AssignmentDateTime
											  THEN 0  ELSE  EDW.[dbo].[Fn_edwbusinesshours](AssignmentDatetime, AllTitleDocumentsRecievedDateTime, AdministrativeBranchNumber)  END
								  WHEN TitleInDayID<29991231 
									   THEN CASE WHEN TitleInDateTime<AssignmentDateTime
												THEN 0 ELSE EDW.[dbo].[Fn_edwbusinesshours](AssignmentDatetime, TitleInDateTime, AdministrativeBranchNumber) END	
							END							
				END)/9 AS BusinessAssignmentToTitleIn,
                CASE WHEN AssignmentDayID<29991231
						THEN CASE WHEN AllTitleDocumentsRecievedDayID<29991231
									THEN CASE WHEN AllTitleDocumentsRecievedDateTime<AssignmentDateTime
												THEN 0  ELSE Cast(AllTitleDocumentsRecievedDateTime AS FLOAT)- Cast(AssignmentDateTime AS FLOAT) END
								  WHEN TitleInDayID<29991231 
									THEN CASE WHEN TitleInDateTime<AssignmentDateTime
												THEN 0 ELSE Cast(TitleInDateTime AS FLOAT)- Cast(AssignmentDateTime AS FLOAT) END	
							END							
				END CalendarAssignmentToTitleIn,
				 (CASE
					WHEN BuyerPaymentDayID<29991231
						 AND BuyerPickupDayID<29991231 THEN
					  CASE
						WHEN BuyerPickupDateTime<BuyerPaymentDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](BuyerPaymentDateTime, BuyerPickupDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessBuyerPaymentToPickup,
				 CASE
				   WHEN BuyerPaymentDayID<29991231
						AND BuyerPickupDayID<29991231 THEN
					 CASE
					   WHEN BuyerPickupDateTime<BuyerPaymentDateTime THEN 0
					   ELSE Cast(BuyerPickupDateTime AS FLOAT)-Cast(BuyerPaymentDateTime AS FLOAT)
					 END
				 END CalendarBuyerPaymenttoPickup,
				 (CASE
					WHEN SoldDayId<29991231
						 AND CertInDayID<29991231 THEN
					  CASE
						WHEN SoldDateTime<CertINDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](CertInDateTime, SoldDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessCertIntoSold,
				 CASE
				   WHEN SoldDayID<29991231
						AND CertInDayID<29991231 THEN
					 CASE
					   WHEN SoldDateTime<CertINDateTime THEN 0
					   ELSE Cast(SoldDateTime AS FLOAT)-Cast(CertINDateTime AS FLOAT)
					 END
				 END CalendarCertIntoSold,
				 (CASE
					WHEN DateOfLossReportedDayID<29991231
						 AND AssignmentDayID<29991231 THEN
					  CASE
						WHEN AssignmentDateTime<DateOfLossReportedDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](DateOfLossReportedDateTime, AssignmentDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessFNOLtoAssignment,
				 CASE
				   WHEN AssignmentDayID<29991231
						AND DateOfLossReportedDayID<29991231 THEN
					 CASE
					   WHEN AssignmentDateTime<DateOfLossReportedDateTime THEN 0
					   ELSE Cast(AssignmentDateTime AS FLOAT)-Cast(DateOfLossReportedDateTime AS FLOAT)
					 END
				 END CalendarFNOLtoAssignment,
				
				(CASE WHEN AssignmentDayID < 29991231
						THEN CASE WHEN RecoveryDayID < 29991231
									THEN CASE WHEN AssignmentDateTime < RecoveryDateTime
												THEN 0  ELSE [dbo].[Fn_edwbusinesshours](RecoveryDateTime, AssignmentDateTime, AdministrativeBranchNumber) END
								  WHEN LossDayID < 29991231 
									THEN CASE WHEN AssignmentDateTime < LossDateTime 
												THEN 0 ELSE [dbo].[Fn_edwbusinesshours](LossDateTime, AssignmentDateTime, AdministrativeBranchNumber) END	
							END							
				END)/9 BusinessLosstoAssignment,
				CASE WHEN AssignmentDayID < 29991231
						THEN CASE WHEN RecoveryDayID < 29991231
									THEN CASE WHEN AssignmentDateTime < RecoveryDateTime
												THEN 0  ELSE Cast(AssignmentDateTime AS FLOAT)- Cast(RecoveryDateTime AS FLOAT) END
								  WHEN LossDayID < 29991231 
									THEN CASE WHEN AssignmentDateTime < LossDateTime 
												THEN 0 ELSE Cast(AssignmentDateTime AS FLOAT)- Cast(LossDateTime AS FLOAT) END	
							END							
				END CalendarLosstoAssignment,
			
				
				 (CASE
					WHEN LossDayID<29991231
						 AND DateOfLossReportedDayID<29991231 THEN
					  CASE
						WHEN DateOfLossReportedDateTime<LossDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](LossDateTime, DateOfLossReportedDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessLosstoFNOL,
				 CASE
				   WHEN DateOfLossReportedDayID<29991231
						AND LossDayID<29991231 THEN
					 CASE
					   WHEN DateOfLossReportedDateTime<LossDateTime THEN 0
					   ELSE Cast(DateOfLossReportedDateTime AS FLOAT)-Cast(LossDateTime AS FLOAT)
					 END
				 END CalendarLosstoFNOL,
				 (CASE
					WHEN LossDayID<29991231
						 AND PickupDayID<29991231 THEN
					  CASE
						WHEN PickupDateTime<LossDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](LossDateTime, PickupDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessLosstoPickup,
				 CASE
				   WHEN LossDayID<29991231
						AND PickupDayID<29991231 THEN
					 CASE
					   WHEN PickupDateTime<LossDateTime THEN 0
					   ELSE Cast(PickupDateTime AS FLOAT)-Cast(LossDateTime AS FLOAT)
					 END
				 END CalendarLosstoPickup,
				 --(CASE
					--WHEN PickupDayID<29991231
					--	 AND TitleInDayID<29991231 THEN
					--  CASE
					--	WHEN TitleInDateTime<PickupDateTime THEN 0
					--	ELSE [dbo].[Fn_edwbusinesshours](PickupDateTime, TitleInDateTime, AdministrativeBranchNumber)
					--  END
				 -- END)/9 AS BusinessPickuptoTitleIn,
				 --CASE
				 --  WHEN PickupDayID<29991231
					--	AND TitleInDayID<29991231 THEN
					-- CASE
					--   WHEN TitleInDateTime<PickupDateTime THEN 0
					--   ELSE Cast(TitleInDateTime AS FLOAT)-Cast(PickupDateTime AS FLOAT)
					-- END
				 --END CalendarPickuptoTitleIn,
				  (CASE WHEN PickupDayID<29991231
						THEN CASE WHEN AllTitleDocumentsRecievedDayID<29991231
									THEN CASE WHEN AllTitleDocumentsRecievedDateTime<PickupDateTime
												THEN 0  ELSE  EDW.[dbo].[Fn_edwbusinesshours](PickupDateTime, AllTitleDocumentsRecievedDateTime, AdministrativeBranchNumber)  END
								  WHEN TitleInDayID<29991231 
									THEN CASE WHEN TitleInDateTime<PickupDateTime
												THEN 0 ELSE EDW.[dbo].[Fn_edwbusinesshours](PickupDateTime, TitleInDateTime, AdministrativeBranchNumber) END	
							END							
				        END)/9 AS BusinessPickuptoTitleIn,
				 CASE WHEN PickupDayID<29991231
						THEN CASE WHEN AllTitleDocumentsRecievedDayID<29991231
									THEN CASE WHEN AllTitleDocumentsRecievedDateTime<PickupDateTime
												THEN 0  ELSE Cast(AllTitleDocumentsRecievedDateTime AS FLOAT)- Cast(PickupDateTime AS FLOAT) END
								  WHEN TitleInDayID<29991231 
									THEN CASE WHEN TitleInDateTime<PickupDateTime 
												THEN 0 ELSE Cast(TitleInDateTime AS FLOAT)- Cast(PickupDateTime AS FLOAT) END	
							END							
				        END CalendarPickuptoTitleIn,
				 (CASE
					WHEN ReleaseDayID<29991231
						 AND PickupDayID<29991231 THEN
					  CASE
						WHEN PickupDateTime<ReleaseDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours2](ReleaseDateTime, PickupDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessReleasetoPickup,
				 CASE
				   WHEN PickupDayID<29991231
						AND ReleaseDayID<29991231 THEN
					 CASE
					   WHEN PickupDateTime<ReleaseDateTime THEN 0
					   ELSE Cast(PickupDateTime AS FLOAT)-Cast(ReleaseDateTime AS FLOAT)
					 END
				 END CalendarReleasetoPickup,
				 (CASE
					WHEN SettlementDayID<29991231
						 AND SettlementPaidDayID<29991231 THEN
					  CASE
						WHEN SettlementPaidDateTime<SettlementDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](SettlementDateTime, SettlementPaidDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessSettledtoPaid,
				 CASE
				   WHEN SettlementDayID<29991231
						AND SettlementPaidDayID<29991231 THEN
					 CASE
					   WHEN SettlementPaidDateTime<SettlementDateTime THEN 0
					   ELSE Cast(SettlementPaidDateTime AS FLOAT)-Cast(SettlementDateTime AS FLOAT)
					 END
				 END CalendarSettledtoPaid,
				 (CASE
					WHEN SMVDDayID<29991231
						 AND CertInDayID<29991231 THEN
					  CASE
						WHEN CertINDateTime<SMVDDatetime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](SMVDDatetime, CertINDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessSMVDtoCertIN,
				 CASE
				   WHEN CertInDayID<29991231
						AND SMVDDayID<29991231 THEN
					 CASE
					   WHEN CertINDateTime<SMVDDatetime THEN 0
					   ELSE Cast(CertINDateTime AS FLOAT)-Cast(SMVDDatetime AS FLOAT)
					 END
				 END CalendarSMVDtoCertIn,
				 (CASE
					WHEN SoldDayId<29991231
						 AND BuyerPaymentDayID<29991231 THEN
					  CASE
						WHEN BuyerPaymentDateTime<SoldDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](SoldDateTime, BuyerPaymentDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessSoldtoBuyerPayment,
				 CASE
				   WHEN BuyerPaymentDayID<29991231
						AND SoldDayID<29991231 THEN
					 CASE
					   WHEN BuyerPaymentDateTime<SoldDateTime THEN 0
					   ELSE Cast(BuyerPaymentDateTime AS FLOAT)-Cast(SoldDateTime AS FLOAT)
					 END
				 END CalendarSoldtoBuyerPayment,
				 (CASE
					WHEN SoldDayId<29991231
						 AND SettlementDayID<29991231 THEN
					  CASE
						WHEN SettlementDateTime<SoldDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](SoldDateTime, SettlementDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessSoldtoSettlement,
				 CASE
				   WHEN SoldDayID<29991231
						AND SettlementDayID<29991231 THEN
					 CASE
					   WHEN SettlementDateTime<SoldDateTime THEN 0
					   ELSE Cast(SettlementDateTime AS FLOAT)-Cast(SoldDateTime AS FLOAT)
					 END
				 END CalendarSoldtoSettlement,
				 --(CASE
					--WHEN TitleInDayID<29991231
					--	 AND SMVDDayID<29991231 THEN
					--  CASE
					--	WHEN SMVDDatetime<TitleInDateTime THEN 0
					--	ELSE [dbo].[Fn_edwbusinesshours](TitleInDateTime, SMVDDatetime, AdministrativeBranchNumber)
					--  END
				 -- END)/9 AS BusinessTitleIntoSMVD,
				 --CASE
				 --  WHEN SMVDDayID<29991231
					--	AND TitleInDayID<29991231 THEN
					-- CASE
					--   WHEN SMVDDatetime<TitleInDateTime THEN 0
					--   ELSE Cast(SMVDDatetime AS FLOAT)-Cast(TitleInDateTime AS FLOAT)
					-- END
				 --END CalendarTitleIntoSMVD,
				 		 (CASE WHEN SMVDDayID<29991231
						THEN CASE WHEN AllTitleDocumentsRecievedDayID<29991231
									THEN CASE WHEN SMVDDatetime<AllTitleDocumentsRecievedDateTime
												THEN 0  ELSE  EDW.[dbo].[Fn_edwbusinesshours](AllTitleDocumentsRecievedDateTime, SMVDDatetime, AdministrativeBranchNumber)  END
								  WHEN TitleInDayID<29991231 
									THEN CASE WHEN SMVDDatetime<TitleInDateTime
												THEN 0 ELSE EDW.[dbo].[Fn_edwbusinesshours](TitleInDateTime, SMVDDatetime, AdministrativeBranchNumber) END	
							END							
				END)/9 AS BusinessTitleIntoSMVD,
                CASE WHEN SMVDDayID<29991231
						THEN CASE WHEN AllTitleDocumentsRecievedDayID<29991231
									THEN CASE WHEN SMVDDatetime<AllTitleDocumentsRecievedDateTime
												THEN 0  ELSE Cast(SMVDDatetime AS FLOAT)- Cast(AllTitleDocumentsRecievedDateTime AS FLOAT) END
								  WHEN TitleInDayID<29991231 
									THEN CASE WHEN TitleInDateTime<PickupDateTime 
												THEN 0 ELSE Cast(SMVDDatetime AS FLOAT)- Cast(TitleInDateTime AS FLOAT) END	
							END							
				END CalendarTitleIntoSMVD,
				 CASE
				   WHEN RP.Release_Problem_Time_Business<0 THEN 0
				   ELSE RP.Release_Problem_Time_Business
				 END AS BusinessReleaseProblemTime,
				 CASE
				   WHEN RP.Release_Problem_Time_Calendar<0 THEN 0
				   ELSE RP.Release_Problem_Time_Calendar
				 END AS CalendarReleaseProblemTime,
				 (CASE
					WHEN PickupDayID<29991231
						 AND CheckinDayID<29991231 THEN
					  CASE
						WHEN CheckInDateTime<PickupDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](PickupDateTime, CheckInDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessPickuptoCheckin,
				 CASE
				   WHEN PickupDayID<29991231
						AND CheckinDayID<29991231 THEN
					 CASE
					   WHEN CheckInDateTime<PickupDateTime THEN 0
					   ELSE Cast(CheckInDateTime AS FLOAT)-Cast(PickupDateTime AS FLOAT)
					 END
				 END CalendarPickuptoCheckin,
				 (CASE
					WHEN LogInDayID<29991231
						 AND CheckinDayID<29991231 THEN
					  CASE
						WHEN CheckInDateTime<LogInDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](LogInDateTime, CheckInDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessLogintoCheckin,
				 CASE
				   WHEN LogInDayID<29991231
						AND CheckinDayID<29991231 THEN
					 CASE
					   WHEN CheckInDateTime<LogInDateTime THEN 0
					   ELSE Cast(CheckInDateTime AS FLOAT)-Cast(LogInDateTime AS FLOAT)
					 END
				 END CalendarLogintoCheckin,
				 Cast(CASE
						WHEN ReleaseTimeZonedDayId<29991231
							 AND PickupTimezonedDayID<29991231 THEN
						  CASE
							WHEN PickupTimezonedDateTime<(CASE
												   WHEN Datepart(HOUR, ReleaseTimezonedDateTime)>=15 THEN Dateadd(DAY, 1, (CONVERT(VARCHAR(11),
																														   ReleaseTimezonedDateTime,
																														   110)))
												   ELSE (CONVERT(VARCHAR(11), ReleaseTimezonedDateTime, 110))
												 END) THEN 0
							ELSE [dbo].[Fn_effectivedaysbusiness2](CASE
																	WHEN Datepart(HOUR, ReleaseTimezonedDateTime)>=15 THEN Dateadd(DAY, 1, (
																														   CONVERT(VARCHAR(11),
																														   ReleaseTimezonedDateTime
																														   , 110))
																														   )
																	ELSE (CONVERT(VARCHAR(11), ReleaseTimezonedDateTime, 110))
																  END, CONVERT(VARCHAR(11), PickupTimezonedDateTime, 110))
						  END
					  END AS INT) AS EffectiveDaysReleasetoPickup,
				 (CASE
					WHEN LossTypeCode<>'TH' THEN
					  CASE
						WHEN LossDayID<29991231
							 AND PickupDayID<29991231 THEN
						  CASE
							WHEN PickupDatetime<lossDatetime THEN 0
							ELSE [dbo].[Fn_edwbusinesshours] (lossDatetime, PickupDatetime, AdministrativeBranchNumber)
						  END
					  END
					ELSE
					  CASE
						WHEN RecoveryDayID=29991231 THEN
						  CASE
							WHEN AssignmentDayID<29991231
								 AND PickupDayID<29991231 THEN
							  CASE
								WHEN PickupDatetime<AssignmentDatetime THEN 0
								ELSE [dbo].[Fn_edwbusinesshours] (AssignmentDatetime, PickupDatetime, AdministrativeBranchNumber)
							  END
						  END
						ELSE
						  CASE
							WHEN RecoveryDayID<29991231
								 AND PickupDayID<29991231 THEN
							  CASE
								WHEN PickupDatetime<RecoveryDatetime THEN 0
								ELSE [dbo].[Fn_edwbusinesshours] (RecoveryDatetime, PickupDatetime, AdministrativeBranchNumber)
							  END
						  END
					  END
				  END)/9 AS BusinessLossToPickupTheftRule,
				 CASE
				   WHEN LossTypeCode<>'TH' THEN
					 CASE
					   WHEN LossDayID<29991231
							AND PickupDayID<29991231 THEN
						 CASE
						   WHEN PickupDatetime<lossDatetime THEN 0
						   ELSE Cast (PickupDatetime-lossDatetime AS FLOAT)
						 END
					 END
				   ELSE
					 CASE
					   WHEN RecoveryDayID=29991231 THEN
						 CASE
						   WHEN AssignmentDayID<29991231
								AND PickupDayID<29991231 THEN
							 CASE
							   WHEN PickupDatetime<AssignmentDatetime THEN 0
							   ELSE Cast(PickupDatetime-AssignmentDatetime AS FLOAT)
							 END
						 END
					   ELSE
						 CASE
						   WHEN RecoveryDayID<29991231
								AND PickupDayID<29991231 THEN
							 CASE
							   WHEN PickupDatetime<RecoveryDatetime THEN 0
							   ELSE Cast(PickupDatetime-RecoveryDatetime AS FLOAT)
							 END
						 END
					 END
				 END AS CalendarLossToPickupTheftRule,
(CASE WHEN CertInDayID<29991231 AND OriginalAuctionDayID<29991231 THEN
	  CASE WHEN OriginalAuctionDatetime<CertInDatetime THEN 0
		ELSE [dbo].[Fn_edwbusinesshours](CertInDatetime, OriginalAuctionDatetime, AdministrativeBranchNumber)
	  END  END)/9 AS BusinessCertInToAuction,
 (CASE WHEN CertInDayID<29991231 AND OriginalAuctionDayID<29991231 THEN
	  CASE WHEN OriginalAuctionDatetime<CertInDatetime THEN 0
		ELSE Cast(OriginalAuctionDatetime AS FLOAT)-Cast(CertInDatetime AS FLOAT)
	  END END) AS CalendarCertInToAuction,
--Business Days Auciton to Settlement
 (CASE WHEN OriginalAuctionDayID<29991231 AND SettlementDayID<29991231 THEN
	  CASE WHEN SettlementDatetime<OriginalAuctionDatetime THEN 0
		ELSE [dbo].[Fn_edwbusinesshours](OriginalAuctionDatetime, SettlementDatetime, AdministrativeBranchNumber)
	  END END)/9 AS BusinessAuctionToSettlement,
--Calendar Days Auciton to Settlement  
 (CASE WHEN OriginalAuctionDayID<29991231 AND SettlementDayID<29991231 THEN
	  CASE WHEN SettlementDatetime<OriginalAuctionDatetime THEN 0
		ELSE Cast(SettlementDatetime AS FLOAT)-Cast(OriginalAuctionDatetime AS FLOAT)
	  END END) AS CalendarAuctionToSettlement,
	  CASE
					WHEN CertInDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN ProviderSoldDateTime<CertINDateTime THEN 0
						ELSE Cast(ProviderSoldDateTime AS FLOAT)-Cast(CertINDateTime AS FLOAT)
					  END
				  END AS CalendarCertInToSoldProvider
		, (CASE
					WHEN CertInDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN ProviderSoldDateTime<CertINDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](CertINDateTime, ProviderSoldDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessCertInToSoldProvider 	
		 ,CASE
					WHEN SettlementDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN SettlementDateTime<ProviderSoldDateTime THEN 0
						ELSE Cast(SettlementDateTime AS FLOAT)-Cast(ProviderSoldDateTime AS FLOAT)
					  END
				  END AS CalendarSoldProviderToSettlement
		 , (CASE
					WHEN SettlementDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN SettlementDateTime<ProviderSoldDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](ProviderSoldDateTime, SettlementDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessSoldProvidertoSettlement
			 ,CASE
					WHEN AssignmentDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN ProviderSoldDateTime<AssignmentDateTime THEN 0
						ELSE Cast(ProviderSoldDateTime AS FLOAT)-Cast(AssignmentDateTime AS FLOAT)
					  END
				  END AS CalendarAssigntmentToSoldProvider	
			, (CASE
					WHEN AssignmentDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN ProviderSoldDateTime<AssignmentDatetime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](AssignmentDatetime, ProviderSoldDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessAssigntmentToSoldProvider	
		    ,CASE
					WHEN BuyerPaymentDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN BuyerPaymentDateTime<ProviderSoldDateTime THEN 0
						ELSE Cast(BuyerPaymentDateTime AS FLOAT)-Cast(ProviderSoldDateTime AS FLOAT)
					  END
				  END AS CalendarSoldProviderToBuyerPayment
		      , (CASE
					WHEN BuyerPaymentDayID<29991231
						 AND ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN BuyerPaymentDateTime<ProviderSoldDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours](ProviderSoldDateTime, BuyerPaymentDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 AS BusinessSoldProvidertoBuyerPayment 	
			  ,0 
			  ,0
			  
	  
FROM   #TMP_FactStockCycleTime_Stage AS FSCT
LEFT OUTER JOIN #TMP_Release_Problem_Time AS RP
	ON FSCT.ASAPSalvageID=RP.Salvage_ID




		  CREATE UNIQUE CLUSTERED INDEX IX_TMP_FSCTDuraions_Stage
			ON #TMP_FSCTDuraions_Stage (StockID)
			WITH FILLFACTOR = 100;

---Inventory durations

UPDATE TFDS
SET  DaysInPhysicalInventoryProvider =CASE WHEN  LogInDayID <> 29991231 AND LogInDayID < = PickupDayID AND LogInDayID <= CheckinDayID THEN (DATEDIFF(dd, LogInDatetime ,ProviderSoldDateTime ) )
									  ELSE CASE  WHEN  pickupDayID <> 29991231 AND PickupDayID < = LogInDayID AND PickupDayID <= CheckinDayID THEN (DATEDIFF(dd, pickupDatetime ,ProviderSoldDateTime ) )
									  ELSE CASE  WHEN  CheckinDayID <> 29991231 AND CheckinDayID <= LogInDayID AND CheckinDayID < = PickupDayID THEN (DATEDIFF(dd, CheckinDatetime ,ProviderSoldDateTime ) )
									  ELSE 0 
									  END END END,
	 DaysInPipelineInventoryProvider = CASE WHEN assignmentdayid <> 29991231 AND assignmentdayid IS NOT NULL THEN (DATEDIFF(dd, TFS.assignmentDateTime,ProviderSoldDateTime) ) 
	 ELSE 0 END 
FROM  #TMP_FSCTDuraions_Stage TFDS WITH (NOLOCK)
INNER JOIN #TMP_FactStockCycleTime_Stage TFS WITH (NOLOCK) 
   ON TFDS.StockID = TFS.stockid
INNER JOIN DimStock DS WITH (NOLOCK) 
   ON TFDS.StockID = DS.stockid   
WHERE ProviderSoldDayID <> 29991231 AND DS.AssignmentTypeCode NOT IN ('TTL','TOW') 
			
			

		  /*********************************************************************************************
		  Update and insert the EDW FactStockCycleTime table with #TMP_FactStockCycleTime_Stage
		  *********************************************************************************************/
		  SELECT @ExtractRowCount_sp=Count(*)
		  FROM   #TMP_CT_SalvageIDs

		  --Run the Audit begin
		  EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactStockCycleTime'

		  BEGIN TRANSACTION
MERGE dbo.FactStockCycleTime AS DST
USING (SELECT Dates.StockID,
			  AssignmentDateTime,
			  AssignmentDayID,
			  ReleaseDateTime,
			  ReleaseDayID,
			  CheckInDateTime,
			  CheckinDayID,
			  LogInDateTime,
			  LogInDayID,
			  LogOutDateTime,
			  LogOutDayID,
			  BuyerPaymentDateTime,
			  BuyerPaymentDayID,
			  ReceivedAllDocumentsDateTime,
			  ReceivedAllDocumentsDayID,
			  MissingPartsDatetime,
			  MissingPartsDayID,
			  SaleDocumentReceivedDateTime,
			  SaleDocumentReceivedDayID,
			  TitleInDateTime,
			  TitleInDayID,
			  DispatchDateTime,
			  DispatchDayID,
			  PickupDateTime,
			  PickupDayID,
			  SettlementDateTime,
			  SettlementDayID,
			  EarliestSaleDate,
			  EarliestSaleDayId,
			  DispatchPermanentlyCancelledDateTime,
			  DispatchPermanentlyCancelledDayID,
			  HoldReleaseDateTime,
			  HoldReleaseDayID,
			  PreBillPaidDateTime,
			  PreBillPaidDayID,
			  SoldDateTime,
			  SoldDayId,
			  AdvanceStorageVerifiedDate,
			  AdvanceStorageVerifiedDayID,
			  DateOfLossReportedDateTime,
			  DateOfLossReportedDayID,
			  InspectionDateTime,
			  InspectionDayID,
			  ProviderQuotedReleaseDateTime,
			  ProviderQuotedReleaseDayID,
			  RecoveryDatetime,
			  RecoveryDayID,
			  SettlementPaidDateTime,
			  SettlementPaidDayID,
			  VICInspectionDateTime,
			  VICInspectionDayID,
			  CancelDateTime,
			  CancelDayID,
			  FirstSoldDateTime,
			  FirstSoldDayID,
			  LossDateTime,
			  LossDayID,
			  SalesDocumentAppliedDatetime,
			  SalesDocumentAppliedDayID,
			  TitleApprovedByProviderDateTime,
			  TitleApprovedByProviderDayID,
			  CertINDateTime,
			  CertInDayID,
			  BuyerPickupDateTime,
			  BuyerPickupDayID,
			  LastProviderRefundDate,
			  LastProviderRefundDayID,
			  CurrentProcessStatusStartDate,
			  CurrentProcessStatusStartDayID,
			  SMVDDatetime,
			  SMVDDayID,
			
			  ReleaseTimezonedDateTime,
			  ReleaseTimezonedDayID,
			  PickupTimezonedDateTime,
			  PickupTimezonedDayID,
			  BusinessHoldTime,
			  CalendarHoldTime,
			  BusinessAssignmentToCancel,
			  CalendarAssignmentToCancel,
			  BusinessAssignmentToPickup,
			  CalendarAssignmentToPickup,
			  BusinessAssignmentToRelease,
			  CalendarAssignmenttoRelease,
			  BusinessAssignmentToSold,
			  CalendarAssignmenttoSold,
			  BusinessAssignmentToTitleIn,
			  CalendarAssignmenttoTitleIn,
			  BusinessBuyerPaymentToPickup,
			  CalendarBuyerPaymenttoPickup,
			  BusinessCertIntoSold,
			  CalendarCertIntoSold,
			  BusinessFNOLtoAssignment,
			  CalendarFNOLtoAssignment,
			  BusinessLosstoAssignment,
			  CalendarLosstoAssignment,
			  BusinessLosstoFNOL,
			  CalendarLosstoFNOL,
			  BusinessLosstoPickup,
			  CalendarLosstoPickup,
			  BusinessPickuptoTitleIn,
			  CalendarPickuptoTitleIn,
			  BusinessReleasetoPickup,
			  CalendarReleasetoPickup,
			  BusinessSettledToPaid,
			  CalendarSettledtoPaid,
			  BusinessSMVDtoCertIN,
			  CalendarSMVDtoCertIn,
			  BusinessSoldtoBuyerPayment,
			  CalendarSoldtoBuyerPayment,
			  BusinessSoldtoSettlement,
			  CalendarSoldtoSettlement,
			  BusinessTitleIntoSMVD,
			  CalendarTitleIntoSMVD,
			  BusinessReleaseProblemTime,
			  CalendarReleaseProblemTime,
			  BusinessPickuptoCheckin,
			  CalendarPickuptoCheckin,
			  BusinessLogintoCheckin,
			  CalendarLogintoCheckin,
			  EffectiveDaysReleasetoPickup,
			  TitleProblemIdentifiedDate,
			  TitleProblemIdentifiedDayID,
			  TitleProblemResolvedDate,
			  TitleProblemResolvedDayID,
			  VehicleImagesLoadedDate,
			  VehicleImagesLoadedDayID,
			  CheckInImagesTakenDate,
			  CheckInImagesTakenDayID,
			  BuyerRenegeCancelSaleDate,
			  BuyerRenegeCancelSaleDayID,
			  CheckIssuedDate,
			  CheckIssuedDayID,
			  PlacedOnHoldDate,
			  PlacedOnHoldDayID,
			  AdvanceStorageStartDate,
			  AdvanceStorageStartDayID,
			  AdvanceStorageEndDate,
			  AdvanceStorageEndDayID,
			  IBuyFastDisplayStartDate,
			  IBuyFastDisplayStartDayID,
			  IBuyFastDisplayEndDate,
			  IBuyFastDisplayEndDayID,
			  AnticipatedSaleDate,
			  AnticipatedSaleDayID,
			  BusinessLossToPickupTheftRule,
			  CalendarLossToPickupTheftRule,
			  IBNDuration,
			  AllTitleDocumentsRecievedDatetime,
			  AllTitleDocumentsRecievedDayID,
			  StockClosedDatetime,
			  StockClosedDayID,
			  AuctionDateTime,
			  AuctionDayID,
			  OriginalAuctionDateTime,
			  OriginalAuctionDayID,
				BusinessCertInToAuction,
				CalendarCertInToAuction,
				BusinessAuctionToSettlement,
				CalendarAuctionToSettlement,
				ProviderSoldDateTime,
				ProviderSoldDayID,
				CalendarCertInToSoldProvider ,
				BusinessCertInToSoldProvider,
                CalendarSoldProviderToSettlement,
				BusinessSoldProviderToSettlement ,
				CalendarAssignmentToSoldProvider ,
				BusinessAssignmentToSoldProvider ,
				CalendarSoldProviderToBuyerPayment ,
				BusinessSoldProviderToBuyerPayment ,
				DaysInPhysicalInventoryProvider ,
				DaysInPipelineInventoryProvider,
					   FinalNMVTISDateTime,
					   FinalNMVTISDayID,
					   InitialNMVTISDateTime,
					   InitialNMVTISDayID ,
					     MinSMVDDatetime,
			  MinSMVDDayID, 
			  SellingBranchReceivedForwardedTitleDocs,
			  FirstReleaseDateTime,
			  FirstAllTitleDocumentsReceivedDateTime,
			  DateTimeReportedtoNICB,
           [PartialDocAwaitingTitleDirectForm],
           [PartialDocLienholderDocsRcvd],
           [PartialDocOwnerDocsRcvd],
		   [LastTitleAssistFormUploadDateTime]
	   FROM   #TMP_FactStockCycleTime_Stage AS Dates
			  INNER JOIN #TMP_FSCTDuraions_Stage AS Durations
				ON Dates.StockID=Durations.StockID)AS SRC
ON SRC.StockID=DST.StockID
WHEN MATCHED THEN
  UPDATE SET StockID=SRC.StockID,
			 AssignmentDateTime=SRC.AssignmentDateTime,
			 AssignmentDayID=SRC.AssignmentDayID,
			 ReleaseDateTime=SRC.ReleaseDateTime,
			 ReleaseDayID=SRC.ReleaseDayID,
			 CheckInDateTime=SRC.CheckInDateTime,
			 CheckinDayID=SRC.CheckinDayID,
			 LogInDateTime=SRC.LogInDateTime,
			 LogInDayID=SRC.LogInDayID,
			 LogOutDateTime=SRC.LogOutDateTime,
			 LogOutDayID=SRC.LogOutDayID,
			 BuyerPaymentDateTime=SRC.BuyerPaymentDateTime,
			 BuyerPaymentDayID=SRC.BuyerPaymentDayID,
			 ReceivedAllDocumentsDateTime=SRC.ReceivedAllDocumentsDateTime,
			 ReceivedAllDocumentsDayID=SRC.ReceivedAllDocumentsDayID,
			 MissingPartsDatetime=SRC.MissingPartsDatetime,
			 MissingPartsDayID=SRC.MissingPartsDayID,
			 SaleDocumentReceivedDateTime=SRC.SaleDocumentReceivedDateTime,
			 SaleDocumentReceivedDayID=SRC.SaleDocumentReceivedDayID,
			 TitleInDateTime=SRC.TitleInDateTime,
			 TitleInDayID=SRC.TitleInDayID,
			 DispatchDateTime=SRC.DispatchDateTime,
			 DispatchDayID=SRC.DispatchDayID,
			 PickupDateTime=SRC.PickupDateTime,
			 PickupDayID=SRC.PickupDayID,
			 SettlementDateTime=SRC.SettlementDateTime,
			 SettlementDayID=SRC.SettlementDayID,
			 EarliestSaleDate=SRC.EarliestSaleDate,
			 EarliestSaleDayId=SRC.EarliestSaleDayID,
			 DispatchPermanentlyCancelledDateTime=SRC.DispatchPermanentlyCancelledDateTime,
			 DispatchPermanentlyCancelledDayID=SRC.DispatchPermanentlyCancelledDayID,
			 HoldReleaseDateTime=SRC.HoldReleaseDateTime,
			 HoldReleaseDayID=SRC.HoldReleaseDayID,
			 PreBillPaidDateTime=SRC.PreBillPaidDateTime,
			 PreBillPaidDayID=SRC.PreBillPaidDayID,
			 SoldDateTime=SRC.SoldDateTime,
			 SoldDayId=SRC.SoldDayID,
			 AdvanceStorageVerifiedDate=SRC.AdvanceStorageVerifiedDate,
			 AdvanceStorageVerifiedDayID=SRC.AdvanceStorageVerifiedDayID,
			 DateOfLossReportedDateTime=SRC.DateOfLossReportedDateTime,
			 DateOfLossReportedDayID=SRC.DateOfLossReportedDayID,
			 InspectionDateTime=SRC.InspectionDateTime,
			 InspectionDayID=SRC.InspectionDayID,
			 ProviderQuotedReleaseDateTime=SRC.ProviderQuotedReleaseDateTime,
			 ProviderQuotedReleaseDayID=SRC.ProviderQuotedReleaseDayID,
			 RecoveryDatetime=SRC.RecoveryDatetime,
			 RecoveryDayID=SRC.RecoveryDayID,
			 SettlementPaidDateTime=SRC.SettlementPaidDateTime,
			 SettlementPaidDayID=SRC.SettlementPaidDayID,
			 VICInspectionDateTime=SRC.VICInspectionDateTime,
			 VICInspectionDayID=SRC.VICInspectionDayID,
			 CancelDateTime=SRC.CancelDateTime,
			 CancelDayID=SRC.CancelDayID,
			 FirstSoldDateTime=SRC.FirstSoldDateTime,
			 FirstSoldDayID=SRC.FirstSoldDayID,
			 LossDateTime=SRC.LossDateTime,
			 LossDayID=SRC.LossDayID,
			 SalesDocumentAppliedDatetime=SRC.SalesDocumentAppliedDatetime,
			 SalesDocumentAppliedDayID=SRC.SalesDocumentAppliedDayID,
			 TitleApprovedByProviderDateTime=SRC.TitleApprovedByProviderDateTime,
			 TitleApprovedByProviderDayID=SRC.TitleApprovedByProviderDayID,
			 CertINDateTime=SRC.CertINDateTime,
			 CertInDayID=SRC.CertInDayID,
			 BuyerPickupDateTime=SRC.BuyerPickupDateTime,
			 BuyerPickupDayID=SRC.BuyerPickupDayID,
			 LastProviderRefundDate=SRC.LastProviderRefundDate,
			 LastProviderRefundDayID=SRC.LastProviderRefundDayID,
			 CurrentProcessStatusStartDate=SRC.CurrentProcessStatusStartDate,
			 CurrentProcessStatusStartDayID=SRC.CurrentProcessStatusStartDayID,
			 SMVDDatetime=SRC.SMVDDatetime,
			 SMVDDayID=SRC.SMVDDayID,
			
			 ReleaseTimezonedDateTime=SRC.ReleaseTimezonedDateTime,
			 ReleaseTimezonedDayID=SRC.ReleaseTimezonedDayID,
			 PickupTimezonedDateTime=SRC.PickupTimezonedDateTime,
			 PickupTimezonedDayID=SRC.PickupTimezonedDayID,
			 BusinessHoldTime=SRC.BusinessHoldTime,
			 CalendarHoldTime=SRC.CalendarHoldTime,
			 BusinessAssignmentToCancel=SRC.BusinessAssignmentToCancel,
			 CalendarAssignmentToCancel=SRC.CalendarAssignmentToCancel,
			 BusinessAssignmentToPickup=SRC.BusinessAssignmentToPickup,
			 CalendarAssignmentToPickup=SRC.CalendarAssignmentToPickup,
			 BusinessAssignmentToRelease=SRC.BusinessAssignmentToRelease,
			 CalendarAssignmenttoRelease=SRC.CalendarAssignmenttoRelease,
			 BusinessAssignmentToSold=SRC.BusinessAssignmentToSold,
			 CalendarAssignmenttoSold=SRC.CalendarAssignmenttoSold,
			 BusinessAssignmentToTitleIn=SRC.BusinessAssignmentToTitleIn,
			 CalendarAssignmenttoTitleIn=SRC.CalendarAssignmenttoTitleIn,
			 BusinessBuyerPaymentToPickup=SRC.BusinessBuyerPaymentToPickup,
			 CalendarBuyerPaymenttoPickup=SRC.CalendarBuyerPaymenttoPickup,
			 BusinessCertIntoSold=SRC.BusinessCertIntoSold,
			 CalendarCertIntoSold=SRC.CalendarCertIntoSold,
			 BusinessFNOLtoAssignment=SRC.BusinessFNOLtoAssignment,
			 CalendarFNOLtoAssignment=SRC.CalendarFNOLtoAssignment,
			 BusinessLosstoAssignment=SRC.BusinessLosstoAssignment,
			 CalendarLosstoAssignment=SRC.CalendarLosstoAssignment,
			 BusinessLosstoFNOL=SRC.BusinessLosstoFNOL,
			 CalendarLosstoFNOL=SRC.CalendarLosstoFNOL,
			 BusinessLosstoPickup=SRC.BusinessLosstoPickup,
			 CalendarLosstoPickup=SRC.CalendarLosstoPickup,
			 BusinessPickuptoTitleIn=SRC.BusinessPickuptoTitleIn,
			 CalendarPickuptoTitleIn=SRC.CalendarPickuptoTitleIn,
			 BusinessReleasetoPickup=SRC.BusinessReleasetoPickup,
			 CalendarReleasetoPickup=SRC.CalendarReleasetoPickup,
			 BusinessSettledtoPaid=SRC.BusinessSettledtoPaid,
			 CalendarSettledtoPaid=SRC.CalendarSettledtoPaid,
			 BusinessSMVDtoCertIN=SRC.BusinessSMVDtoCertIN,
			 CalendarSMVDtoCertIn=SRC.CalendarSMVDtoCertIn,
			 BusinessSoldtoBuyerPayment=SRC.BusinessSoldtoBuyerPayment,
			 CalendarSoldtoBuyerPayment=SRC.CalendarSoldtoBuyerPayment,
			 BusinessSoldtoSettlement=SRC.BusinessSoldtoSettlement,
			 CalendarSoldtoSettlement=SRC.CalendarSoldtoSettlement,
			 BusinessTitleIntoSMVD=SRC.BusinessTitleIntoSMVD,
			 CalendarTitleIntoSMVD=SRC.CalendarTitleIntoSMVD,
			 BusinessReleaseProblemTime=SRC.BusinessReleaseProblemTime,
			 CalendarReleaseProblemTime=SRC.CalendarReleaseProblemTime,
			 BusinessPickuptoCheckin=SRC.BusinessPickuptoCheckin,
			 CalendarPickuptoCheckin=SRC.CalendarPickuptoCheckin,
			 BusinessLogintoCheckin=SRC.BusinessLogintoCheckin,
			 CalendarLogintoCheckin=SRC.CalendarLogintoCheckin,
			 EffectiveDaysReleasetoPickup=SRC.EffectiveDaysReleasetoPickup,
			 TitleProblemIdentifiedDate=SRC.TitleProblemIdentifiedDate,
			 TitleProblemIdentifiedDayID=SRC.TitleProblemIdentifiedDayID,
			 TitleProblemResolvedDate=SRC.TitleProblemResolvedDate,
			 TitleProblemResolvedDayID=SRC.TitleProblemResolvedDayID,
			 VehicleImagesLoadedDate=SRC.VehicleImagesLoadedDate,
			 VehicleImagesLoadedDayID=SRC.VehicleImagesLoadedDayID,
			 CheckInImagesTakenDate=SRC.CheckInImagesTakenDate,
			 CheckInImagesTakenDayID=SRC.CheckInImagesTakenDayID,
			 BuyerRenegeCancelSaleDate=SRC.BuyerRenegeCancelSaleDate,
			 BuyerRenegeCancelSaleDayID=SRC.BuyerRenegeCancelSaleDayID,
			 CheckIssuedDate=SRC.CheckIssuedDate,
			 CheckIssuedDayID=SRC.CheckIssuedDayID,
			 PlacedOnHoldDate=SRC.PlacedOnHoldDate,
			 PlacedOnHoldDayID=SRC.PlacedOnHoldDayID,
			 AdvanceStorageStartDate = SRC.AdvanceStorageStartDate,
			 AdvanceStorageStartDayID = SRC.AdvanceStorageStartDayID,
			 AdvanceStorageEndDate = SRC.AdvanceStorageEndDate,
			 AdvanceStorageEndDayID = SRC.AdvanceStorageEndDayID,
			 IBuyFastDisplayStartDate=SRC.IBuyFastDisplayStartDate,
			 IBuyFastDisplayStartDayID=SRC.IBuyFastDisplayStartDayID,
			 IBuyFastDisplayEndDate=SRC.IBuyFastDisplayEndDate,
			 IBuyFastDisplayEndDayID=SRC.IBuyFastDisplayEndDayID,
			 BusinessLossToPickupTheftRule=SRC.BusinessLossToPickupTheftRule,
			 CalendarLossToPickupTheftRule=SRC.CalendarLossToPickupTheftRule,
			 AnticipatedSaleDate=SRC.AnticipatedSaleDate,
			 AnticipatedSaleDayID=SRC.AnticipatedSaleDayID,
			 CalendarDaysSpentOnIBF=SRC.IBNDuration,
			 AllTitleDocumentsRecievedDatetime = SRC.AllTitleDocumentsRecievedDatetime,
			 AllTitleDocumentsRecievedDayID = SRC.AllTitleDocumentsRecievedDayID,
			 StockClosedDatetime = SRC.StockClosedDatetime,
			 StockClosedDayID = SRC.StockClosedDayID,
			  AuctionDateTime = ISNULL(SRC.AuctionDateTime,'12/31/2999'),
			  AuctionDayID = ISNULL(SRC.AuctionDayID,29991231),
			  OriginalAuctionDateTime = ISNULL(SRC.OriginalAuctionDateTime,'12/31/2999'),
			  OriginalAuctionDayID = ISNULL(SRC.OriginalAuctionDayID,29991231),
				BusinessCertInToAuction = SRC.BusinessCertInToAuction,
				CalendarCertInToAuction = SRC.CalendarCertInToAuction,
				BusinessAuctionToSettlement = SRC.BusinessAuctionToSettlement,
				CalendarAuctionToSettlement = SRC.CalendarAuctionToSettlement,
				ProviderSoldDateTime = SRC.ProviderSoldDateTime,
				ProviderSoldDayID  = SRC.ProviderSoldDayID,
				 CalendarCertInToSoldProvider = SRC.CalendarCertInToSoldProvider,
				 BusinessCertInToSoldProvider = SRC.BusinessCertInToSoldProvider,
                 CalendarSoldProviderToSettlement = SRC.CalendarSoldProviderToSettlement,
				 BusinessSoldProviderToSettlement = SRC.BusinessSoldProviderToSettlement,
				 CalendarAssignmentToSoldProvider = SRC.CalendarAssignmentToSoldProvider,
				 BusinessAssignmentToSoldProvider = SRC.BusinessAssignmentToSoldProvider,
				 CalendarSoldProviderToBuyerPayment = SRC.CalendarSoldProviderToBuyerPayment,
				 BusinessSoldProviderToBuyerPayment = SRC.BusinessSoldProviderToBuyerPayment,
				 DaysInPhysicalInventoryProvider  = SRC.DaysInPhysicalInventoryProvider,
				 DaysInPipelineInventoryProvider  = SRC.DaysInPipelineInventoryProvider,
				FinalNMVTISDateTime  = SRC.FinalNMVTISDateTime,
				FinalNMVTISDayID  = SRC.FinalNMVTISDayID,
				InitialNMVTISDateTime  = SRC.InitialNMVTISDateTime,
				InitialNMVTISDayID  = SRC.InitialNMVTISDayID,
				 MinSMVDDatetime=SRC.MinSMVDDatetime,
			 MinSMVDDayID=SRC.MinSMVDDayID, 
			 SellingBranchReceivedForwardedTitleDocs=SRC.SellingBranchReceivedForwardedTitleDocs,
			 FirstReleaseDateTime=SRC.FirstReleaseDateTime,
			 FirstAllTitleDocumentsReceivedDateTime=SRC.FirstAllTitleDocumentsReceivedDateTime,
			 			 DateTimeReportedtoNICB=SRC.DateTimeReportedtoNICB,
[PartialDocAwaitingTitleDirectForm]=SRC.[PartialDocAwaitingTitleDirectForm],
[PartialDocLienholderDocsRcvd]=SRC.[PartialDocLienholderDocsRcvd],
[PartialDocOwnerDocsRcvd]=SRC.[PartialDocOwnerDocsRcvd],
[LastTitleAssistFormUploadDateTime]=SRC.[LastTitleAssistFormUploadDateTime],
				 ETLLoadUpdateID=@ETLLoadID_sp
WHEN NOT MATCHED THEN
  INSERT (StockID,
		  AssignmentDateTime,
		  AssignmentDayID,
		  ReleaseDateTime,
		  ReleaseDayID,
		  CheckInDateTime,
		  CheckinDayID,
		  LogInDateTime,
		  LogInDayID,
		  LogOutDateTime,
		  LogOutDayID,
		  BuyerPaymentDateTime,
		  BuyerPaymentDayID,
		  ReceivedAllDocumentsDateTime,
		  ReceivedAllDocumentsDayID,
		  MissingPartsDatetime,
		  MissingPartsDayID,
		  SaleDocumentReceivedDateTime,
		  SaleDocumentReceivedDayID,
		  TitleInDateTime,
		  TitleInDayID,
		  DispatchDateTime,
		  DispatchDayID,
		  PickupDateTime,
		  PickupDayID,
		  SettlementDateTime,
		  SettlementDayID,
		  EarliestSaleDate,
		  EarliestSaleDayId,
		  DispatchPermanentlyCancelledDateTime,
		  DispatchPermanentlyCancelledDayID,
		  HoldReleaseDateTime,
		  HoldReleaseDayID,
		  PreBillPaidDateTime,
		  PreBillPaidDayID,
		  SoldDateTime,
		  SoldDayId,
		  AdvanceStorageVerifiedDate,
		  AdvanceStorageVerifiedDayID,
		  DateOfLossReportedDateTime,
		  DateOfLossReportedDayID,
		  InspectionDateTime,
		  InspectionDayID,
		  ProviderQuotedReleaseDateTime,
		  ProviderQuotedReleaseDayID,
		  RecoveryDatetime,
		  RecoveryDayID,
		  SettlementPaidDateTime,
		  SettlementPaidDayID,
		  VICInspectionDateTime,
		  VICInspectionDayID,
		  CancelDateTime,
		  CancelDayID,
		  FirstSoldDateTime,
		  FirstSoldDayID,
		  LossDateTime,
		  LossDayID,
		  SalesDocumentAppliedDatetime,
		  SalesDocumentAppliedDayID,
		  TitleApprovedByProviderDateTime,
		  TitleApprovedByProviderDayID,
		  CertINDateTime,
		  CertInDayID,
		  BuyerPickupDateTime,
		  BuyerPickupDayID,
		  LastProviderRefundDate,
		  LastProviderRefundDayID,
		  CurrentProcessStatusStartDate,
		  CurrentProcessStatusStartDayID,
		  SMVDDatetime,
		  SMVDDayID,
		 
		  ReleaseTimezonedDateTime,
		  ReleaseTimezonedDayID,
		  PickupTimezonedDateTime,
		  PickupTimezonedDayID,
		  BusinessHoldTime,
		  CalendarHoldTime,
		  BusinessAssignmentToCancel,
		  CalendarAssignmentToCancel,
		  BusinessAssignmentToPickup,
		  CalendarAssignmentToPickup,
		  BusinessAssignmentToRelease,
		  CalendarAssignmenttoRelease,
		  BusinessAssignmentToSold,
		  CalendarAssignmenttoSold,
		  BusinessAssignmentToTitleIn,
		  CalendarAssignmenttoTitleIn,
		  BusinessBuyerPaymentToPickup,
		  CalendarBuyerPaymenttoPickup,
		  BusinessCertIntoSold,
		  CalendarCertIntoSold,
		  BusinessFNOLtoAssignment,
		  CalendarFNOLtoAssignment,
		  BusinessLosstoAssignment,
		  CalendarLosstoAssignment,
		  BusinessLosstoFNOL,
		  CalendarLosstoFNOL,
		  BusinessLosstoPickup,
		  CalendarLosstoPickup,
		  BusinessPickuptoTitleIn,
		  CalendarPickuptoTitleIn,
		  BusinessReleasetoPickup,
		  CalendarReleasetoPickup,
		  BusinessSettledtoPaid,
		  CalendarSettledtoPaid,
		  BusinessSMVDtoCertIN,
		  CalendarSMVDtoCertIn,
		  BusinessSoldtoBuyerPayment,
		  CalendarSoldtoBuyerPayment,
		  BusinessSoldtoSettlement,
		  CalendarSoldtoSettlement,
		  BusinessTitleIntoSMVD,
		  CalendarTitleIntoSMVD,
		  BusinessReleaseProblemTime,
		  CalendarReleaseProblemTime,
		  BusinessPickuptoCheckin,
		  CalendarPickuptoCheckin,
		  BusinessLogintoCheckin,
		  CalendarLogintoCheckin,
		  EffectiveDaysReleasetoPickup,
		  ETLLoadID,
		  ETLLoadUpdateID,
		  TitleProblemIdentifiedDate,
		  TitleProblemIdentifiedDayID,
		  TitleProblemResolvedDate,
		  TitleProblemResolvedDayID,
		  VehicleImagesLoadedDate,
		  VehicleImagesLoadedDayID,
		  CheckInImagesTakenDate,
		  CheckInImagesTakenDayID,
		  BuyerRenegeCancelSaleDate,
		  BuyerRenegeCancelSaleDayID,
		  CheckIssuedDate,
		  CheckIssuedDayID,
		  PlacedOnHoldDate,
		  PlacedOnHoldDayID,
		  AdvanceStorageStartDate,
		  AdvanceStorageStartDayID,
		  AdvanceStorageEndDate,
		  AdvanceStorageEndDayID,
		  IBuyFastDisplayStartDate,
		  IBuyFastDisplayStartDayID,
		  IBuyFastDisplayEndDate,
		  IBuyFastDisplayEndDayID,
		  BusinessLossToPickupTheftRule,
		  CalendarLossToPickupTheftRule,
		  AnticipatedSaleDate,
		  AnticipatedSaleDayID,
		  CalendarDaysSpentOnIBF,
		  AllTitleDocumentsRecievedDatetime,
		  AllTitleDocumentsRecievedDayID,
		  StockClosedDatetime,
		  StockClosedDayID,
		  AuctionDateTime,
		  AuctionDayID,
		  OriginalAuctionDateTime,
		  OriginalAuctionDayID,
			BusinessCertInToAuction,
			CalendarCertInToAuction,
			BusinessAuctionToSettlement,
			CalendarAuctionToSettlement,
			ProviderSoldDateTime,
			ProviderSoldDayID,
			 CalendarCertInToSoldProvider ,
			 BusinessCertInToSoldProvider,
             CalendarSoldProviderToSettlement,
			 BusinessSoldProviderToSettlement ,
			 CalendarAssignmentToSoldProvider ,
			 BusinessAssignmentToSoldProvider ,
			 CalendarSoldProviderToBuyerPayment ,
			 BusinessSoldProviderToBuyerPayment ,
			 DaysInPhysicalInventoryProvider ,
			 DaysInPipelineInventoryProvider, 
				FinalNMVTISDateTime,
				FinalNMVTISDayID,
				InitialNMVTISDateTime,
				InitialNMVTISDayID,
				 MinSMVDDatetime,
		  MinSMVDDayID,
		  SellingBranchReceivedForwardedTitleDocs,
		  FirstReleaseDateTime,
		  FirstAllTitleDocumentsReceivedDateTime,
		  DateTimeReportedtoNICB,
[PartialDocAwaitingTitleDirectForm],
[PartialDocLienholderDocsRcvd],
[PartialDocOwnerDocsRcvd],
[LastTitleAssistFormUploadDateTime]
			)
  VALUES (StockID,
		  AssignmentDateTime,
		  AssignmentDayID,
		  ReleaseDateTime,
		  ReleaseDayID,
		  CheckInDateTime,
		  CheckinDayID,
		  LogInDateTime,
		  LogInDayID,
		  LogOutDateTime,
		  LogOutDayID,
		  BuyerPaymentDateTime,
		  BuyerPaymentDayID,
		  ReceivedAllDocumentsDateTime,
		  ReceivedAllDocumentsDayID,
		  MissingPartsDatetime,
		  MissingPartsDayID,
		  SaleDocumentReceivedDateTime,
		  SaleDocumentReceivedDayID,
		  TitleInDateTime,
		  TitleInDayID,
		  DispatchDateTime,
		  DispatchDayID,
		  PickupDateTime,
		  PickupDayID,
		  SettlementDateTime,
		  SettlementDayID,
		  EarliestSaleDate,
		  EarliestSaleDayId,
		  DispatchPermanentlyCancelledDateTime,
		  DispatchPermanentlyCancelledDayID,
		  HoldReleaseDateTime,
		  HoldReleaseDayID,
		  PreBillPaidDateTime,
		  PreBillPaidDayID,
		  SoldDateTime,
		  SoldDayId,
		  AdvanceStorageVerifiedDate,
		  AdvanceStorageVerifiedDayID,
		  DateOfLossReportedDateTime,
		  DateOfLossReportedDayID,
		  InspectionDateTime,
		  InspectionDayID,
		  ProviderQuotedReleaseDateTime,
		  ProviderQuotedReleaseDayID,
		  RecoveryDatetime,
		  RecoveryDayID,
		  SettlementPaidDateTime,
		  SettlementPaidDayID,
		  VICInspectionDateTime,
		  VICInspectionDayID,
		  CancelDateTime,
		  CancelDayID,
		  FirstSoldDateTime,
		  FirstSoldDayID,
		  LossDateTime,
		  LossDayID,
		  SalesDocumentAppliedDatetime,
		  SalesDocumentAppliedDayID,
		  TitleApprovedByProviderDateTime,
		  TitleApprovedByProviderDayID,
		  CertINDateTime,
		  CertInDayID,
		  BuyerPickupDateTime,
		  BuyerPickupDayID,
		  LastProviderRefundDate,
		  LastProviderRefundDayID,
		  CurrentProcessStatusStartDate,
		  CurrentProcessStatusStartDayID,
		  SMVDDatetime,
		  SMVDDayID,
		 
		  ReleaseTimezonedDateTime,
		  ReleaseTimezonedDayID,
		  PickupTimezonedDateTime,
		  PickupTimezonedDayID,
		  BusinessHoldTime,
		  CalendarHoldTime,
		  BusinessAssignmentToCancel,
		  CalendarAssignmentToCancel,
		  BusinessAssignmentToPickup,
		  CalendarAssignmentToPickup,
		  BusinessAssignmentToRelease,
		  CalendarAssignmenttoRelease,
		  BusinessAssignmentToSold,
		  CalendarAssignmenttoSold,
		  BusinessAssignmentToTitleIn,
		  CalendarAssignmenttoTitleIn,
		  BusinessBuyerPaymentToPickup,
		  CalendarBuyerPaymenttoPickup,
		  BusinessCertIntoSold,
		  CalendarCertIntoSold,
		  BusinessFNOLtoAssignment,
		  CalendarFNOLtoAssignment,
		  BusinessLosstoAssignment,
		  CalendarLosstoAssignment,
		  BusinessLosstoFNOL,
		  CalendarLosstoFNOL,
		  BusinessLosstoPickup,
		  CalendarLosstoPickup,
		  BusinessPickuptoTitleIn,
		  CalendarPickuptoTitleIn,
		  BusinessReleasetoPickup,
		  CalendarReleasetoPickup,
		  BusinessSettledtoPaid,
		  CalendarSettledtoPaid,
		  BusinessSMVDtoCertIN,
		  CalendarSMVDtoCertIn,
		  BusinessSoldtoBuyerPayment,
		  CalendarSoldtoBuyerPayment,
		  BusinessSoldtoSettlement,
		  CalendarSoldtoSettlement,
		  BusinessTitleIntoSMVD,
		  CalendarTitleIntoSMVD,
		  BusinessReleaseProblemTime,
		  CalendarReleaseProblemTime,
		  BusinessPickuptoCheckin,
		  CalendarPickuptoCheckin,
		  BusinessLogintoCheckin,
		  CalendarLogintoCheckin,
		  EffectiveDaysReleasetoPickup,
		  @ETLLoadID_sp,
		  @ETLLoadID_sp,
		  TitleProblemIdentifiedDate,
		  TitleProblemIdentifiedDayID,
		  TitleProblemResolvedDate,
		  TitleProblemResolvedDayID,
		  VehicleImagesLoadedDate,
		  VehicleImagesLoadedDayID,
		  CheckInImagesTakenDate,
		  CheckInImagesTakenDayID,
		  BuyerRenegeCancelSaleDate,
		  BuyerRenegeCancelSaleDayID,
		  CheckIssuedDate,
		  CheckIssuedDayID,
		  PlacedOnHoldDate,
		  PlacedOnHoldDayID,
		  AdvanceStorageStartDate,
		  AdvanceStorageStartDayID,
		  AdvanceStorageEndDate,
		  AdvanceStorageEndDayID,
		  IBuyFastDisplayStartDate,
		  IBuyFastDisplayStartDayID,
		  IBuyFastDisplayEndDate,
		  IBuyFastDisplayEndDayID,
		  BusinessLossToPickupTheftRule,
		  CalendarLossToPickupTheftRule,
		  AnticipatedSaleDate,
		  AnticipatedSaleDayID,
		  SRC.IBNDuration,
		  AllTitleDocumentsRecievedDatetime,
		  AllTitleDocumentsRecievedDayID,
		  StockClosedDatetime,
		  StockClosedDayID,
		  	  ISNULL(AuctionDateTime,'12/31/2999'),
			  ISNULL(AuctionDayID,29991231),
			  ISNULL(OriginalAuctionDateTime,'12/31/2999'),
			  ISNULL(OriginalAuctionDayID,29991231),
			BusinessCertInToAuction,
			CalendarCertInToAuction,
			BusinessAuctionToSettlement,
			CalendarAuctionToSettlement,
			ProviderSoldDateTime,
			ProviderSoldDayID,
			CalendarCertInToSoldProvider ,
			 BusinessCertInToSoldProvider,
             CalendarSoldProviderToSettlement,
			 BusinessSoldProviderToSettlement ,
			 CalendarAssignmentToSoldProvider ,
			 BusinessAssignmentToSoldProvider ,
			 CalendarSoldProviderToBuyerPayment ,
			 BusinessSoldProviderToBuyerPayment ,
			 DaysInPhysicalInventoryProvider ,
			 DaysInPipelineInventoryProvider,
			SRC.FinalNMVTISDateTime,
			SRC.FinalNMVTISDayID,
			SRC.InitialNMVTISDateTime,
			SRC.InitialNMVTISDayID,
			 MinSMVDDatetime,
		  MinSMVDDayID,
		  SellingBranchReceivedForwardedTitleDocs,
		  FirstReleaseDateTime,
		  FirstAllTitleDocumentsReceivedDateTime,
		  DateTimeReportedtoNICB,
		  [PartialDocAwaitingTitleDirectForm],
[PartialDocLienholderDocsRcvd],
[PartialDocOwnerDocsRcvd],
[LastTitleAssistFormUploadDateTime])
OUTPUT $action
INTO #temp_FSCTRecordCount; 


		  COMMIT TRAN

		  SELECT @InsertRowCount_sp=Count(*)
		  FROM   #temp_FSCTRecordCount
		  WHERE  ChangeType='INSERT'

		  SELECT @UpdateRowCount_sp=Count(*)
		  FROM   #temp_FSCTRecordCount
		  WHERE  ChangeType='UPDATE'

		  EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
		  @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp


---Inventory Provider durations for ProviderSoldDate NULL

SET @currentDateTime = (
						SELECT CASE WHEN datepart (dw,MaxDatetime) between 2 and 6 THEN MaxDatetime
									WHEN datepart(hh,MaxDatetime) between 0 and 14 THEN DateAdd(HOUR,23,CONVERT(DATETIME,CONVERT(varchar(8), MaxDatetime - 1,112),114))
							   ELSE MaxDatetime
						END 
						FROM dbo.ETLRUN WITH (NOLOCK)
						WHERE ETLJobName = 'DailyETLJob'
						)

UPDATE fst
SET  DaysInPhysicalInventoryProvider =  CASE WHEN  LogInDayID <> 29991231 AND LogInDayID < = PickupDayID AND LogInDayID <= CheckinDayID THEN (DATEDIFF(dd, LogInDatetime ,@currentDateTime ) )
	 ELSE CASE  WHEN  pickupDayID <> 29991231 AND PickupDayID < = LogInDayID AND PickupDayID <= CheckinDayID THEN (DATEDIFF(dd, pickupDatetime ,@currentDateTime ) )
	 ELSE CASE  WHEN  CheckinDayID <> 29991231 AND CheckinDayID <= LogInDayID AND CheckinDayID < = PickupDayID THEN (DATEDIFF(dd, CheckinDatetime ,@currentDateTime ) )
	 ELSE 0 
	 END END END ,
	DaysInPipelineInventoryProvider = CASE WHEN assignmentdayid <> 29991231 AND assignmentdayid IS NOT NULL THEN (DATEDIFF(dd,assignmentDateTime,@currentDateTime) ) 
	 ELSE 0 END 
FROM  FactStockCycleTime fst WITH (NOLOCK)
INNER JOIN DimStock ds WITH (NOLOCK) 
   ON ds.StockID = fst.stockid
WHERE (ProviderSoldDayID = 29991231) AND ds.AssignmentTypeCode NOT IN ('TTL','TOW')

		  
/*********************************************************************************************
Update DaysInPhysicalInventory and DaysInPipelineInventory
*********************************************************************************************/  
SET @currentDateTime = (
						SELECT CASE WHEN datepart (dw,MaxDatetime) between 2 and 6 THEN MaxDatetime
									WHEN datepart(hh,MaxDatetime) between 0 and 14 THEN DateAdd(HOUR,23,CONVERT(DATETIME,CONVERT(varchar(8), MaxDatetime - 1,112),114))
							   ELSE MaxDatetime
						END 
						FROM dbo.ETLRUN WITH (NOLOCK)
						WHERE ETLJobName = 'DailyETLJob'
						)

UPDATE FactStockCycleTime
SET DaysInPhysicalInventory =  CASE WHEN  fstt.LogInDayID <> 29991231 THEN (DATEDIFF(dd, fstt.LogInDatetime ,@currentDateTime ) )
									WHEN  fstt.pickupDayID <> 29991231 THEN (DATEDIFF(dd, fstt.pickupDatetime ,@currentDateTime ) )
									WHEN  fstt.CheckinDayID <> 29991231 THEN (DATEDIFF(dd, fstt.CheckinDatetime ,@currentDateTime ) )
							  ELSE 0 END,
	DaysInPipelineInventory = CASE WHEN assignmentdayid <> 29991231 AND assignmentdayid IS NOT NULL THEN (DATEDIFF(dd, fstt.assignmentDateTime,@currentDateTime) ) 
							  ELSE 0 END
FROM  FactStockCycleTime fstt WITH (NOLOCK)
INNER JOIN DimStock ds WITH (NOLOCK) 
   ON ds.StockID = fstt.stockid
WHERE IsStockSold=0 
AND   IsCancel=0   

UPDATE FactStockCycleTime
set OriginalProviderTransferDateTime = Update_Datetime
from  FactStockCycleTime inner join Dimstock DS 
on (DS.Stockid= FactStockCycleTime.Stockid) 
inner join #TMP_CT_Salvage_Provider_Info   
on (DS.Stockid=#TMP_CT_Salvage_Provider_Info.stockid 
and DS.originalProviderid <> #TMP_CT_Salvage_Provider_Info.Providerid 
and OriginalProviderTransferDateTime > '2999-12-30'
and DS.IsProviderOfficeTransfer=1)     


INSERT INTO #TMP_CT_Salavage_Event_REAB
([StockID]  ,
 [Event_DateTime] )

SELECT  DS.StockID,
     Min(SER.event_Datetime)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge' ) AS SER
INNER JOIN Dimstock  DS WITH (NOLOCK) 
 ON (DS.ASAPSalvageID = SER.Salvage_id and Salvage_Event_Code in ('REAB'))
INNER JOIN FactStockCycleTime FS with (nolock) 
on (FS.StockID = DS.StockID)
WHERE [__$operation] = 5 
and  FS.CheckinDayID = 29991231
	 GROUP BY DS.StockID


INSERT INTO #TMP_CT_Salavage_Event_REAB_IBTR_IBND
([StockID]  ,
 [Event_DateTime] )

SELECT  DS.StockID,
     Max(SER.event_Datetime)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge' ) AS SER
INNER JOIN Dimstock  DS WITH (NOLOCK) 
 ON (DS.ASAPSalvageID = SER.Salvage_id and Salvage_Event_Code in ('IBTR','IBND'))
LEFT OUTER JOIN  #TMP_CT_Salavage_Event_Checkin TCSEC WITH (NOLOCK)
ON DS.StockID = TCSEC.STOCKID
INNER JOIN FactStockCycleTime FS with (nolock) 
on (FS.StockID = DS.StockID)
WHERE [__$operation] = 5 
and (	SER.event_DateTime  <  TCSEC.Checkin_Date
		or FS.CheckinDayID = 29991231
	 )
GROUP BY DS.StockID


UPDATE FactStockCycleTime
set OriginalBranchChangeDatetime = (TCSERIB.Event_Datetime)
from  FactStockCycleTime FSCT
inner join Dimstock DS 
on DS.Stockid= FSCT.Stockid
inner join #TMP_CT_Salavage_Event_REAB TCSERIB
on DS.Stockid=TCSERIB.stockid 
and DS.IsBranchTransfer = 1 
and FSCT.CheckInDateTime > TCSERIB.event_datetime
and FSCT.OriginalBranchChangeDatetime = '2999-12-31'

UPDATE FactStockCycleTime
set OriginalBranchChangeDatetime = (TCSERII.Event_Datetime)
from  FactStockCycleTime FSCT
inner join Dimstock DS 
on DS.Stockid= FSCT.Stockid
inner join #TMP_CT_Salavage_Event_REAB_IBTR_IBND TCSERII
on DS.Stockid=TCSERII.stockid 
and DS.IsBranchTransfer = 1 
and FSCT.CheckInDateTime > TCSERII.event_datetime
and FSCT.OriginalBranchChangeDatetime = '2999-12-31'
and TCSERII.STOCKID not in (select distinct STOCKID from  #TMP_CT_Salavage_Event_REAB)


INSERT INTO #TMP_CT_Salavage_Event_Checkin_Future
([StockID]  ,
[Checkin_Date_Fut] 
)
SELECT  DS.StockID,
    min(event_Datetime ) 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge' ) AS SER
           INNER JOIN Dimstock  DS WITH (NOLOCK) 
              ON (DS.ASAPSalvageID = SER.Salvage_id and Salvage_Event_Code in ('CKI1'))	
              INNER JOIN FactStockCycleTime FS with (nolock) on (FS.StockID = DS.StockID)
WHERE [__$operation] = 5 and ds.IsBranchTransfer =1 and fs.OriginalBranchChangeDatetime ='2999-12-31'
GROUP BY DS.StockID  

INSERT INTO #TMP_CT_Salavage_Event_Checkin_Future_MaxDate
([StockID]  ,
[Checkin_Date_Fut_MaxDate]
)
select	DS.Stockid,
		max(Event_Datetime)
from  #TMP_CT_Salavage_Event_Checkin_Future SECF  
inner join Dimstock DS with (nolock) 
on (DS.Stockid= SECF.Stockid) 
inner join [BI_ASAP_Rep].[dbo].Salvage_Event_History SEH  with (nolock) 
on (DS.asapsalvageid=SEH.Salvage_ID and Salvage_Event_Code in ('IBTR','IBND'))
 INNER JOIN FactStockCycleTime FS with (nolock) 
 on (FS.StockID = DS.StockID)
where  DS.IsBranchTransfer=1 
and SECF.Checkin_Date_Fut > (event_datetime) 
and fs.OriginalBranchChangeDatetime ='2999-12-31'  
group by ds.StockID


UPDATE FactStockCycleTime
SET OriginalBranchChangeDatetime =  Checkin_Date_Fut_MaxDate
FROM FactStockCycleTime FSCT
INNER JOIN #TMP_CT_Salavage_Event_Checkin_Future_MaxDate TSECFMD
ON FSCT.StockID = TSECFMD.StockID


---Tower Kiosk DropOffDateTime
INSERT INTO #TMP_SEHAU
(Salvage_ID,
DropOffDateTime,
DropOffDayID)
SELECT  Salvage_ID
	    ,MAX(Event_DateTime) AS DropOffDateTime
	    ,Isnull(CONVERT(INT, CONVERT(CHAR(8), MAX(Event_DateTime) , 112)), 29991231) AS DropOffDayID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN, @MaxLSN, 'all with merge') SEH
INNER JOIN [BI_ASAP_Rep].[dbo].User_Info UI WITH (NOLOCK)
 ON SEH.Update_User_ID = UI.User_ID
WHERE Salvage_Event_Code = 'SDRO' 
--AND UI.USER_NAME = 'Tower_Kiosk'
AND [__$operation] = 5
GROUP BY  Salvage_ID

INSERT INTO #TMP_SEHAU
(Salvage_ID,
DropOffDateTime,
DropOffDayID)
		  SELECT ST.Salvage_ID,
				 Max(SEH.Event_DateTime) AS Pickup_DateTime,
				 CONVERT(INT, CONVERT(CHAR(8), Max(SEH.Event_Datetime), 112)) AS Pickup_Day_ID
		  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge') AS SEH
				 INNER JOIN BI_ASAP_Rep.dbo.Salvage_Tow AS ST WITH(NOLOCK)
				   ON ST.TowBill_Number=SEH.TowBill_Number
					  AND SEH.Salvage_Event_Code='DROP'
					  AND ST.Tow_Type_Code='PK'
				INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
					ON SEH.Salvage_ID = DS.ASAPSalvageID
			WHERE [__$operation] = 5
			AND DS.IsDeliveredToBranch = 1
			AND ST.Salvage_ID NOT IN (SELECT Salvage_ID FROM #TMP_SEHAU WITH (NOLOCK))
		  GROUP  BY ST.Salvage_ID

UPDATE FST
SET	 DropOffDateTime	=
			CASE
				WHEN Isnull(TSE.DropOffDateTime , '2999-12-31') 
						BETWEEN @MinAllowableDate AND @MaxAllowableDate
									THEN Isnull(TSE.DropOffDateTime , '2999-12-31')
				ELSE '2999-12-31' END
	, DropOffDayID		=
			CASE
				WHEN Isnull(TSE.DropOffDayID , 29991231)
						BETWEEN @MinAllowableDayID AND @MaxAllowableDayID
									THEN Isnull(TSE.DropOffDayID , 29991231)
				ELSE 29991231 END
	,  ETLLoadUpdateID = @ETLLoadID_sp
FROM FactStockCycleTime FST WITH(NOLOCK)
INNER JOIN DimStock DS  WITH(NOLOCK) ON FST.StockID = DS.StockID
INNER JOIN #TMP_SEHAU TSE ON TSE.Salvage_ID = DS.ASAPSalvageID

CREATE TABLE #TMP_DT
( ASAPSalvageID				INT,
  StockID					INT,
  DropOffTimezonedDateTime	DATETIME,
  DropOffTimezonedDayID		INT )

INSERT INTO #TMP_DT
( ASAPSalvageID,
  StockID,
  DropOffTimezonedDateTime,
  DropOffTimezonedDayID
)
SELECT DS.ASAPSalvageID
	, DS.StockID
	, dbo.Fn_timezoneconvert(TS.Dropoffdatetime, DS.AdministrativeBranchNumber) AS DropOffTimezonedDateTime
	, CONVERT(CHAR(8), dbo.Fn_timezoneconvert(TS.Dropoffdatetime, DS.AdministrativeBranchNumber), 112) AS DropOffTimezonedDayID
FROM FactStockCycleTime FSCT 
INNER JOIN DimStock DS WITH (NOLOCK) ON DS.StockID = FSCT.StockID
INNER JOIN #TMP_SEHAU TS WITH (NOLOCK) ON TS.Salvage_ID = DS.ASAPSalvageID

-- Time Zoned Drop Off Date and ID Fields
UPDATE FST
SET	  TimeZonedStockDropOffDateTime	= dt.DropOffTimezonedDateTime
	, TimeZonedStockDropOffDayID	= dt.DropOffTimezonedDayID
	
FROM FactStockCycleTime		FST WITH(NOLOCK)
	INNER JOIN #TMP_DT		dt  WITH(NOLOCK)
				ON FST.StockID = DT.StockID

-- Time Zoned FIRST Drop Off Date and ID Fields
UPDATE FST
SET	  TimeZonedStockFirstDropOffDateTime	= CASE WHEN ISNULL(FST.TimeZonedStockFirstDropOffDateTime,'12/31/2999') = '12/31/2999' THEN dt.DropOffTimezonedDateTime ELSE FST.TimeZonedStockFirstDropOffDateTime END
	, TimeZonedStockFirstDropOffDayID	= CASE WHEN ISNULL(FST.TimeZonedStockFirstDropOffDayID,29991231) = 29991231 THEN dt.DropOffTimezonedDayID ELSE FST.TimeZonedStockFirstDropOffDayID END
FROM FactStockCycleTime		FST WITH(NOLOCK)
	INNER JOIN #TMP_DT		dt  WITH(NOLOCK)
				ON FST.StockID = DT.StockID
 
 /*TimeZonedInspectionServicesCompletedDateTime*/ --Commented Old logic
 
/*		  DECLARE @MinLSN Binary(10)
		  		,@MaxLSN Binary (10)
		  SELECT @MinLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Salvage_info')
		  SELECT @MaxLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()
--*/

-- CREATE TABLE #TMP_ISCDT
--( ASAPSalvageID				INT,
--  StockID					INT,
--  TimeZonedInspectionServicesCompletedDateTime	DATETIME)

--INSERT INTO #TMP_ISCDT
--( ASAPSalvageID,
--  StockID,
--  TimeZonedInspectionServicesCompletedDateTime
--)
--SELECT ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesCompletedDateTime
----UPDATE FSCT SET [TimeZonedInspectionServicesCompletedDateTime]= dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber), [TimeZonedInspectionServicesCompletedDayID] = CAST(CONVERT(CHAR(8),dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber), 112) AS INT)
----SELECT Salvage_ID, Update_Datetime as Work_Completed_Date 
--FROM EDW.dbo.DimStock DS WITH (NOLOCK)
--INNER JOIN (SELECT Salvage_ID, MAX(Update_Datetime) as Work_Completed_Date 
--			FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
--			WHERE [__$operation] = 5 	
--			AND Salvage_Event_Category_Code = 'IS'
--				AND (Note_Text Like '%Vehicle images are now available for viewing%' OR Note_Text LIKE '%Inspection Services Photos enhancement marked complete%')
--				GROUP BY Salvage_ID
--		  ) SN
--ON DS.ASAPSalvageID = SN.Salvage_ID


--INSERT INTO #TMP_ISCDT
--( ASAPSalvageID,
--  StockID,
--  TimeZonedInspectionServicesCompletedDateTime
--)
--SELECT DS.ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesCompletedDateTime
--FROM EDW.dbo.FactStockCycleTime FSCT
--INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
--	ON FSCT.StockID = DS.StockID
--INNER JOIN (SELECT SEL.Salvage_ID, MAX(Work_Completed_Date) as Work_Completed_Date FROM BI_ASAP_Rep.[dbo].[Salvage_Enhancement_Level] SEL
--			INNER JOIN (SELECT Salvage_ID,Work_Completed_Date
--						  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_enhancement](@MinLSN, @MaxLSN, 'all with merge')
--						  WHERE [__$operation] = 5 
--								AND (Charge_Type_ID = 542 AND Status_Code = 'WCP') 
--						 ) SE 
--			ON SE.Salvage_ID = SEL.Salvage_ID 
--			GROUP BY SEL.Salvage_ID) SEL
--	ON DS.ASAPSalvageID = SEL.Salvage_ID
--LEFT OUTER JOIN #TMP_ISCDT TMP WITH (NOLOCK)
--	ON DS.StockID = TMP.StockID
--WHERE SEL.Work_Completed_Date IS NOT NULL AND TMP.StockID IS NULL

--UPDATE FST
--SET	  TimeZonedInspectionServicesCompletedDateTime	= dt.TimeZonedInspectionServicesCompletedDateTime
--	, TimeZonedInspectionServicesCompletedDayID	= CAST(CONVERT(CHAR(8),dt.TimeZonedInspectionServicesCompletedDateTime,112) AS INT)
--FROM FactStockCycleTime		FST WITH(NOLOCK)
--	INNER JOIN #TMP_ISCDT		dt  WITH(NOLOCK)
--				ON FST.StockID = DT.StockID



--/*TimeZonedInspectionServicesReportsCompletedDateTime*/ 
 

-- CREATE TABLE #TMP_ISRCDT
--( ASAPSalvageID				INT,
--  StockID					INT,
--  TimeZonedInspectionServicesReportsCompletedDateTime	DATETIME)

--INSERT INTO #TMP_ISRCDT
--( ASAPSalvageID,
--  StockID,
--  TimeZonedInspectionServicesReportsCompletedDateTime
--)
--SELECT ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesCompletedDateTime
--FROM EDW.dbo.DimStock DS WITH (NOLOCK)
--INNER JOIN (SELECT Salvage_ID, MAX(Update_Datetime) as Work_Completed_Date 
--			FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
--			WHERE [__$operation] = 5 
--				AND Salvage_Event_Category_Code = 'IS'
--				AND (Note_Text Like '%Inspection Services Report enhancement marked complete%')
--				GROUP BY Salvage_ID) SN
--ON DS.ASAPSalvageID = SN.Salvage_ID

--INSERT INTO #TMP_ISRCDT
--( ASAPSalvageID,
--  StockID,
--  TimeZonedInspectionServicesReportsCompletedDateTime
--)
--SELECT DS.ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesReportsCompletedDateTime
--FROM EDW.dbo.FactStockCycleTime FSCT
--INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
--	ON FSCT.StockID = DS.StockID
--INNER JOIN (SELECT SEL.Salvage_ID, MAX(Work_Completed_Date) as Work_Completed_Date FROM BI_ASAP_Rep.[dbo].[Salvage_Enhancement_Level] SEL
--			INNER JOIN (SELECT Salvage_ID,Work_Completed_Date
--						  FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_enhancement](@MinLSN, @MaxLSN, 'all with merge')
--						  WHERE [__$operation] = 5 
--						  AND (Charge_Type_ID = 540 AND Status_Code = 'WCP') 
--						 )SE 
--			ON SE.Salvage_ID = SEL.Salvage_ID 
--			GROUP BY SEL.Salvage_ID) SEL
--	ON DS.ASAPSalvageID = SEL.Salvage_ID
--LEFT OUTER JOIN #TMP_ISRCDT TMP WITH (NOLOCK)
--	ON DS.StockID = TMP.StockID
--WHERE SEL.Work_Completed_Date IS NOT NULL AND TMP.StockID IS NULL


--UPDATE FST
--SET TimeZonedInspectionServicesReportsCompletedDateTime = IT.TimeZonedInspectionServicesReportsCompletedDateTime
--	, TimeZonedInspectionServicesReportsCompletedDayID	= CAST(CONVERT(CHAR(8),IT.TimeZonedInspectionServicesReportsCompletedDateTime,112) AS INT)
----SELECT *
--FROM FactStockCycleTime		FST WITH(NOLOCK)
--	INNER JOIN #TMP_ISRCDT	IT  WITH(NOLOCK)
--				ON FST.StockID = IT.StockID

-----Logic to Update 'TimeZonedInspectionServicesCompletedDateTime' default if InspectionServicePhotos is Cancelled----------------


--CREATE TABLE #TMP_InspectionServices_Enhancement_Cancelled(
--	  [Salvage_ID] [int] NOT NULL
--     , [Status_Code] [char](3) NULL
--	 , [Max_Work_Request_date] [datetime] NULL)

--CREATE TABLE #TMP_InspectionServices_Enhancement_Completed(
--	  [Salvage_ID] [int] NOT NULL
--     , [Status_Code] [char](3) NULL
--	 , [Max_Work_Request_date1] [datetime] NULL)


--INSERT INTO #TMP_InspectionServices_Enhancement_Cancelled 
--      (
--       Salvage_ID,
--       Max_Work_Request_date
--      )

--SELECT	SE.Salvage_ID,
--        MAX(SE.Work_Request_Date) AS Max_Work_Request_date
--FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
--INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
--	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
--INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
--    ON C.Charge_ID = CT.Charge_ID
--WHERE CT.Charge_ID IN (281)
--    AND SE.Status_Code IN ('WCN','WDC')
--    AND [__$operation]  = 5 
--GROUP BY SE.Salvage_ID,SE.Status_Code


--INSERT INTO #TMP_InspectionServices_Enhancement_Completed 
--      (
--       Salvage_ID,
--       Max_Work_Request_date1
--      )

--SELECT	SE.Salvage_ID,
--		MAX(SE.Work_Request_Date) AS  Max_Work_Request_date1
--FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
--INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
--	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
--INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
--	ON C.Charge_ID = CT.Charge_ID
--WHERE (CT.Charge_ID IN (281)) 
--    AND SE.Status_Code IN ('WCP','APN','WPN')
--    AND SE.Salvage_ID IN ( SELECT Salvage_id FROM #TMP_InspectionServices_Enhancement_Cancelled)
--    AND [__$operation]  = 5  
--GROUP BY SE.Salvage_ID,SE.Status_Code

--UPDATE FS
--SET FS.TimeZonedInspectionServicesCompletedDateTime = '2999-12-31 00:00:00.000'
----SELECT FS.TimeZonedInspectionServicesCompletedDateTime,FS.* 
--FROM Edw.dbo.FactStockCycleTime FS WITH(NOLOCK)
--INNER JOIN EDW.dbo.DimStock DS WITH(NOLOCK)
--	ON FS.StockID = DS.StockID
--INNER JOIN ( select TMP1.* FROM #TMP_InspectionServices_Enhancement_Cancelled TMP1 WITH(NOLOCK)
--			 LEFT OUTER JOIN #TMP_InspectionServices_Enhancement_Completed TMP2 WITH(NOLOCK)
--				ON  TMP1.Salvage_Id = TMP2.Salvage_Id   
--			 WHERE TMP1.Max_Work_Request_date > ISNULL(Max_Work_Request_date1, '2000-01-01')
--            ) TS
--    ON DS.ASAPSalvageID = TS.Salvage_ID
--WHERE FS.TimeZonedInspectionServicesCompletedDateTime <> '2999-12-31 00:00:00.000'

-----Logic to Update 'TimeZonedInspectionServicesReportsCompletedDateTime' default if InspectionServiceReports is Cancelled----------------


--CREATE TABLE #TMP_InspectionServices_Reports_Cancelled(
--	  [Salvage_ID] [int] NOT NULL
--     , [Status_Code] [char](3) NULL
--	 , [Max_Work_Request_date] [datetime] NULL)

--CREATE TABLE #TMP_InspectionServices_Reports_Completed(
--	  [Salvage_ID] [int] NOT NULL
--     , [Status_Code] [char](3) NULL
--	 , [Max_Work_Request_date1] [datetime] NULL)


--INSERT INTO #TMP_InspectionServices_Reports_Cancelled 
--      (
--       Salvage_ID,
--       Max_Work_Request_date
--      )

--SELECT	SE.Salvage_ID,
--        MAX(SE.Work_Request_Date) AS Max_Work_Request_date
--FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
--INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
--	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
--INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
--    ON C.Charge_ID = CT.Charge_ID
--WHERE CT.Charge_ID IN (280)
--    AND SE.Status_Code IN ('WCN','WDC')
--    AND [__$operation]  = 5 
--GROUP BY SE.Salvage_ID,SE.Status_Code


--INSERT INTO #TMP_InspectionServices_Reports_Completed 
--      (
--       Salvage_ID,
--       Max_Work_Request_date1
--      )

--SELECT	SE.Salvage_ID,
--		MAX(SE.Work_Request_Date) AS  Max_Work_Request_date1
--FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN, @MaxLSN, 'all with merge') AS SE 
--INNER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CT  WITH(NOLOCK)
--	ON SE.Charge_Type_ID = CT.Charge_Type_ID 
--INNER JOIN [BI_ASAP_Rep].[dbo].[Charge] C  WITH(NOLOCK)
--	ON C.Charge_ID = CT.Charge_ID
--WHERE (CT.Charge_ID IN (280)) 
--    AND SE.Status_Code IN ('WCP','APN','WPN')
--    AND SE.Salvage_ID IN ( SELECT Salvage_id FROM #TMP_InspectionServices_Reports_Cancelled)
--    AND [__$operation]  = 5  
--GROUP BY SE.Salvage_ID,SE.Status_Code

--UPDATE FS
--SET FS.TimeZonedInspectionServicesReportsCompletedDateTime = '2999-12-31 00:00:00.000'
----SELECT FS.TimeZonedInspectionServicesCompletedDateTime,FS.* 
--FROM Edw.dbo.FactStockCycleTime FS WITH(NOLOCK)
--INNER JOIN EDW.dbo.DimStock DS WITH(NOLOCK)
--	ON FS.StockID = DS.StockID
--INNER JOIN ( select TMP1.* FROM #TMP_InspectionServices_Reports_Cancelled TMP1 WITH(NOLOCK)
--			 LEFT OUTER JOIN #TMP_InspectionServices_Reports_Completed TMP2 WITH(NOLOCK)
--				ON  TMP1.Salvage_Id = TMP2.Salvage_Id   
--			 WHERE TMP1.Max_Work_Request_date > ISNULL(Max_Work_Request_date1, '2000-01-01')
--            ) TS
--    ON DS.ASAPSalvageID = TS.Salvage_ID
--WHERE FS.TimeZonedInspectionServicesReportsCompletedDateTime <> '2999-12-31 00:00:00.000'

--/*TimeZonedInspectionServices Dates New logic*/ 
--/*Images*/ 

CREATE TABLE #TMP_ISCDT
( ASAPSalvageID				INT,
  StockID					INT,
  TimeZonedInspectionServicesCompletedDateTime	DATETIME)

INSERT INTO #TMP_ISCDT
( ASAPSalvageID,
  StockID,
  TimeZonedInspectionServicesCompletedDateTime
)
SELECT ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesCompletedDateTime
FROM EDW.dbo.DimStock DS WITH (NOLOCK)
INNER JOIN (SELECT Salvage_ID, MAX(Update_Datetime) as Work_Completed_Date 
			FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
			WHERE [__$operation] = 5 	
			AND Salvage_Event_Category_Code = 'IS'
				AND (Note_Text Like '%Vehicle images are now available for viewing%' OR Note_Text LIKE '%Inspection Services Photos enhancement marked complete%')
				GROUP BY Salvage_ID
		  ) SN
ON DS.ASAPSalvageID = SN.Salvage_ID

UPDATE FST
SET	  FST.TimeZonedInspectionServicesCompletedDateTime	= ISNULL(IT.TimeZonedInspectionServicesCompletedDateTime,'2999-12-31')
	, FST.TimeZonedInspectionServicesCompletedDayID	= CAST(CONVERT(CHAR(8),IT.TimeZonedInspectionServicesCompletedDateTime,112) AS INT)
--SELECT FST.StockId,FST.TimeZonedInspectionServicesCompletedDateTime,IT.TimeZonedInspectionServicesCompletedDateTime 
FROM EDW.dbo.FactStockCycleTime		FST WITH(NOLOCK)
	INNER JOIN #TMP_ISCDT IT WITH(NOLOCK)
	ON FST.StockID = IT.StockID

--/*Reports*/ 

CREATE TABLE #TMP_ISRCDT
( ASAPSalvageID				INT,
  StockID					INT,
  TimeZonedInspectionServicesReportsCompletedDateTime	DATETIME)

INSERT INTO #TMP_ISRCDT
( ASAPSalvageID,
  StockID,
  TimeZonedInspectionServicesReportsCompletedDateTime
)
SELECT ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesReportsCompletedDateTime
FROM EDW.dbo.DimStock DS WITH (NOLOCK)
INNER JOIN (SELECT Salvage_ID, MAX(Update_Datetime) as Work_Completed_Date 
			FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
			WHERE [__$operation] = 5 
				AND Salvage_Event_Category_Code = 'IS'
				AND (Note_Text Like '%Vehicle condition report is now available for viewing%' OR Note_Text LIKE '%Inspection Services Report enhancement marked complete%')
				GROUP BY Salvage_ID) SN
ON DS.ASAPSalvageID = SN.Salvage_ID

UPDATE FST
SET	  FST.TimeZonedInspectionServicesReportsCompletedDateTime	= ISNULL(IT.TimeZonedInspectionServicesReportsCompletedDateTime,'2999-12-31')
	, FST.TimeZonedInspectionServicesReportsCompletedDayID	= CAST(CONVERT(CHAR(8),IT.TimeZonedInspectionServicesReportsCompletedDateTime,112) AS INT)
--SELECT FST.StockId,FST.TimeZonedInspectionServicesReportsCompletedDateTime,IT.TimeZonedInspectionServicesReportsCompletedDateTime 
FROM EDW.dbo.FactStockCycleTime		FST WITH(NOLOCK)
	INNER JOIN #TMP_ISRCDT	IT WITH(NOLOCK)
	ON FST.StockID = IT.StockID
--WHERE FST.TimeZonedInspectionServicesReportsCompletedDateTime <> IT.TimeZonedInspectionServicesReportsCompletedDateTime


--/*Appraisal*/ 

CREATE TABLE #TMP_ISACDT
( ASAPSalvageID				INT,
  StockID					INT,
  TimeZonedInspectionServicesAppraisalCompletedDateTime	DATETIME)

INSERT INTO #TMP_ISACDT
( ASAPSalvageID,
  StockID,
  TimeZonedInspectionServicesAppraisalCompletedDateTime
)
SELECT ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesAppraisalCompletedDateTime
FROM EDW.dbo.DimStock DS WITH (NOLOCK)
INNER JOIN (SELECT Salvage_ID, MAX(Update_Datetime) as Work_Completed_Date 
			FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
			WHERE [__$operation] = 5 
				AND Salvage_Event_Category_Code = 'IS'
				AND (Note_Text Like '%Vehicle appraisal is now available for viewing%' OR Note_Text LIKE '%Inspection Services Appraisal enhancement marked complete%')
				GROUP BY Salvage_ID) SN
ON DS.ASAPSalvageID = SN.Salvage_ID

UPDATE FST
SET	  FST.TimeZonedInspectionServicesAppraisalCompletedDateTime	= ISNULL(IT.TimeZonedInspectionServicesAppraisalCompletedDateTime,'2999-12-31')
	, FST.TimeZonedInspectionServicesAppraisalCompletedDayID = CAST(CONVERT(CHAR(8),IT.TimeZonedInspectionServicesAppraisalCompletedDateTime,112) AS INT)
--SELECT FST.StockId,FST.TimeZonedInspectionServicesAppraisalCompletedDateTime,IT.TimeZonedInspectionServicesAppraisalCompletedDateTime
FROM EDW.dbo.FactStockCycleTime		FST WITH(NOLOCK)
	INNER JOIN #TMP_ISACDT	IT WITH(NOLOCK)
	ON FST.StockID = IT.StockID

-----/*Logic to Update 'TimeZonedInspectionServicesCompletedDateTime' default if InspectionServiceImages are Cancelled*/----------------

SELECT Salvage_ID
       ,MAX(CASE WHEN Note_Text like '%Inspection Services Photos enhancement marked cancelled%' THEN (Update_Datetime) END) AS Work_Cancelled_Date
INTO #TMP_ImagesCancelled	   	
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5 
AND Salvage_Event_Category_Code = 'IS' 
GROUP BY Salvage_ID

SELECT Salvage_ID
       ,MAX(CASE WHEN Note_Text Like '%Vehicle images are now available for viewing%' OR Note_Text LIKE '%Inspection Services Photos enhancement marked complete%' THEN (Update_Datetime) END) AS Work_Completed_Date
INTO #TMP_ImagesCompleted	   	
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5 
AND Salvage_Event_Category_Code = 'IS' 
GROUP BY Salvage_ID
	   
UPDATE FS
SET FS.TimeZonedInspectionServicesCompletedDateTime = '2999-12-31'
    ,FS.TimeZonedInspectionServicesCompletedDayID = '29991231'
--SELECT FS.TimeZonedInspectionServicesCompletedDateTime,FS.* 
FROM Edw.dbo.FactStockCycleTime FS WITH(NOLOCK)
INNER JOIN EDW.dbo.DimStock DS WITH(NOLOCK)
	ON FS.StockID = DS.StockID
INNER JOIN ( SELECT TCL.* FROM #TMP_ImagesCancelled TCL	  
                LEFT OUTER JOIN #TMP_ImagesCompleted TCT
                  ON TCL.Salvage_Id = TCT.Salvage_Id
                  WHERE TCL.Work_Cancelled_Date > TCT.Work_Completed_Date
            ) TS
    ON DS.ASAPSalvageID = TS.Salvage_ID
WHERE FS.TimeZonedInspectionServicesCompletedDateTime <> '2999-12-31'

-----/*Logic to Update 'TimeZonedInspectionServicesReportsCompletedDateTime' default if InspectionServiceReports are Cancelled*/----------------

SELECT Salvage_ID
       ,MAX(CASE WHEN Note_Text like '%Inspection Services Report enhancement marked cancelled%' THEN (Update_Datetime) END) AS Work_Cancelled_Date
INTO #TMP_ReportsCancelled	   	
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5 
AND Salvage_Event_Category_Code = 'IS' 
GROUP BY Salvage_ID

SELECT Salvage_ID
       ,MAX(CASE WHEN Note_Text Like '%Vehicle condition report is now available for viewing%' OR Note_Text LIKE '%Inspection Services Report enhancement marked complete%' THEN (Update_Datetime) END) AS Work_Completed_Date
INTO #TMP_ReportsCompleted	   	
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5 
AND Salvage_Event_Category_Code = 'IS' 
GROUP BY Salvage_ID
	   
UPDATE FS
SET FS.TimeZonedInspectionServicesReportsCompletedDateTime = '2999-12-31'
    ,FS.TimeZonedInspectionServicesReportsCompletedDayID = '29991231'
--SELECT FS.TimeZonedInspectionServicesReportsCompletedDateTime,FS.* 
FROM Edw.dbo.FactStockCycleTime FS WITH(NOLOCK)
INNER JOIN EDW.dbo.DimStock DS WITH(NOLOCK)
	ON FS.StockID = DS.StockID
INNER JOIN ( SELECT TCL.* FROM #TMP_ReportsCancelled TCL	  
                LEFT OUTER JOIN #TMP_ReportsCompleted TCT
                  ON TCL.Salvage_Id = TCT.Salvage_Id
                  WHERE TCL.Work_Cancelled_Date > TCT.Work_Completed_Date
            ) TS
    ON DS.ASAPSalvageID = TS.Salvage_ID
WHERE FS.TimeZonedInspectionServicesReportsCompletedDateTime <> '2999-12-31'


-----/*Logic to Update 'TimeZonedInspectionServicesAppraisalCompletedDateTime' default if InspectionServiceAppraisal are Cancelled/*----------------

SELECT Salvage_ID
       ,MAX(CASE WHEN Note_Text like '%Inspection Services Appraisal enhancement marked cancelled%' THEN (Update_Datetime) END) AS Work_Cancelled_Date
INTO #TMP_AppraisalCancelled	   	
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5 
AND Salvage_Event_Category_Code = 'IS' 
GROUP BY Salvage_ID

SELECT Salvage_ID
       ,MAX(CASE WHEN Note_Text Like '%Vehicle appraisal is now available for viewing%' OR Note_Text LIKE '%Inspection Services Appraisal enhancement marked complete%' THEN (Update_Datetime) END) AS Work_Completed_Date
INTO #TMP_AppraisalCompleted	   	
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
WHERE [__$operation] = 5 
AND Salvage_Event_Category_Code = 'IS' 
GROUP BY Salvage_ID
	   
UPDATE FS
SET FS.TimeZonedInspectionServicesAppraisalCompletedDateTime = '2999-12-31'
    ,FS.TimeZonedInspectionServicesAppraisalCompletedDayID = '29991231'
--SELECT FS.TimeZonedInspectionServicesAppraisalCompletedDateTime,FS.* 
FROM Edw.dbo.FactStockCycleTime FS WITH(NOLOCK)
INNER JOIN EDW.dbo.DimStock DS WITH(NOLOCK)
	ON FS.StockID = DS.StockID
INNER JOIN ( SELECT TCL.* FROM #TMP_AppraisalCancelled TCL	  
                LEFT OUTER JOIN #TMP_AppraisalCompleted TCT
                  ON TCL.Salvage_Id = TCT.Salvage_Id
                  WHERE TCL.Work_Cancelled_Date > TCT.Work_Completed_Date
            ) TS
    ON DS.ASAPSalvageID = TS.Salvage_ID
WHERE FS.TimeZonedInspectionServicesAppraisalCompletedDateTime <> '2999-12-31'

--/*TimeZonedInspectionServicesFlippedDateTime New logic*/

CREATE TABLE #TMP_ISFDT
( ASAPSalvageID				INT,
  StockID					INT,
  TimeZonedInspectionServicesFlippedDateTime	DATETIME)

INSERT INTO #TMP_ISFDT
( ASAPSalvageID,
  StockID,
  TimeZonedInspectionServicesFlippedDateTime)

SELECT ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesFlippedDateTime
FROM EDW.dbo.DimStock DS WITH (NOLOCK)
INNER JOIN (SELECT Salvage_ID, MAX(Update_Datetime) as Work_Completed_Date 
			FROM    [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge')
                 WHERE [__$operation] = 5 
			AND  Salvage_Event_Category_Code = 'IS'
				AND (Note_Text Like '%Service level updated from Empty to Images%' OR Note_Text LIKE '%Service level updated from None to Images%')
				GROUP BY Salvage_ID) SN
ON DS.ASAPSalvageID = SN.Salvage_ID

UPDATE FST
SET	  FST.TimeZonedInspectionServicesFlippedDateTime = ISNULL(IT.TimeZonedInspectionServicesFlippedDateTime,'2999-12-31')
	, FST.TimeZonedInspectionServicesFlippedDayID = CAST(CONVERT(CHAR(8),IT.TimeZonedInspectionServicesFlippedDateTime,112) AS INT)
--SELECT FST.StockId,FST.TimeZonedInspectionServicesFlippedDateTime,IT.TimeZonedInspectionServicesFlippedDateTime
FROM EDW.dbo.FactStockCycleTime		FST WITH(NOLOCK)
	INNER JOIN #TMP_ISFDT	IT WITH(NOLOCK)
	ON FST.StockID = IT.StockID



--/*TimeZonedInspectionServicesFlippedDateTime*/

--CREATE TABLE #TMP_ISFDT
--( ASAPSalvageID				INT,
--  StockID					INT,
--  TimeZonedInspectionServicesFlippedDateTime	DATETIME)

--INSERT INTO #TMP_ISFDT
--( ASAPSalvageID,
--  StockID,
--  TimeZonedInspectionServicesFlippedDateTime)
--SELECT ASAPSalvageID,DS.StockID,dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber) AS TimeZonedInspectionServicesFlippedDateTime
----UPDATE FSCT SET TimeZonedInspectionServicesFlippedDateTime= EDW.dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber), TimeZonedInspectionServicesFlippedDayID = CAST(CONVERT(CHAR(8),EDW.dbo.Fn_timezoneconvert(Work_Completed_Date, DS.AdministrativeBranchNumber), 112) AS INT)
----SELECT ASAPSalvageID, Work_Completed_Date --* 
--FROM EDW.dbo.FactStockCycleTime FSCT
--INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
--	ON FSCT.StockID = DS.StockID
--INNER JOIN (SELECT Salvage_ID, MIN(Update_DateTime) as Work_Completed_Date 
--			FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Enhancement_Level](@MinLSN, @MaxLSN, 'all with merge')
--			WHERE [__$operation] = 5 	
--				AND Update_DateTime IS NOT NULL 
--				AND Enhancement_Level_ID IN (1,2,3)
--			GROUP BY Salvage_ID) SEL
--	ON DS.ASAPSalvageID = SEL.Salvage_ID

--UPDATE FST
--SET	  TimeZonedInspectionServicesFlippedDateTime = dt.TimeZonedInspectionServicesFlippedDateTime
--	, TimeZonedInspectionServicesFlippedDayID	= CAST(CONVERT(CHAR(8),dt.TimeZonedInspectionServicesFlippedDateTime,112) AS INT)
--FROM FactStockCycleTime		FST WITH(NOLOCK)
--	INNER JOIN #TMP_ISFDT		dt  WITH(NOLOCK)
--				ON FST.StockID = DT.StockID
--WHERE TimeZonedInspectionServicesFlippedDayID IS NULL OR TimeZonedInspectionServicesFlippedDayID = 29991231

--UPDATE FSCT SET TimeZonedInspectionServicesFlippedDateTime= '12/31/2999', TimeZonedInspectionServicesFlippedDayID = 29991231,
--TimeZonedInspectionServicesCompletedDateTime = '12/31/2999', TimeZonedInspectionServicesCompletedDayID	= 29991231
----SELECT ASAPSalvageID, Work_Completed_Date --* 
--FROM EDW.dbo.FactStockCycleTime FSCT
--INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
--	ON FSCT.StockID = DS.StockID
--INNER JOIN (SELECT Salvage_ID, MIN(Update_DateTime) as Work_Completed_Date 
--			FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Enhancement_Level](@MinLSN, @MaxLSN, 'all with merge')
--			WHERE [__$operation] = 5 	
--				AND Update_DateTime IS NOT NULL AND Enhancement_Level_ID IN (0)
--			GROUP BY Salvage_ID) SEL
--	ON DS.ASAPSalvageID = SEL.Salvage_ID



--Tower Kiosk Durations

CREATE TABLE #TMP_TKD
( StockID INT,
  CalendarDaysReleaseToDropOff DECIMAL(18,9) NULL,
  BusinessDaysReleaseToDropOff  DECIMAL(18,9) NULL,
  EffectiveDaysReleaseToDropOff DECIMAL(18,9) NULL )

INSERT INTO #TMP_TKD
( StockID,
  CalendarDaysReleaseToDropOff,
  BusinessDaysReleaseToDropOff,
  EffectiveDaysReleaseToDropOff
)
SELECT DS.StockID
			  ,CASE
					WHEN ReleaseDayID<29991231
						 AND TS.DropOffDayID<29991231 THEN
					  CASE
						WHEN TS.DropOffDateTime<ReleaseDateTime THEN 0
						ELSE Cast(TS.DropOffDateTime AS FLOAT)-Cast(ReleaseDateTime AS FLOAT)
					  END
				  END CalendarDaysReleaseToDropOff
			  
  ,(CASE
					WHEN ReleaseDayID<29991231
						 AND TS.DropOffDayID<29991231 THEN
					  CASE
						WHEN TS.DropOffDateTime<ReleaseDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours2](ReleaseDateTime, TS.DropOffDateTime, AdministrativeBranchNumber)
					  END
				  END)/9 BusinessDaysReleaseToDropOff 
 , Cast(CASE
						WHEN ReleaseTimeZonedDayId<29991231
							 AND TD.DropOffTimezonedDayID<29991231 THEN
						  CASE
							WHEN TD.DropOffTimezonedDateTime<(CASE
												   WHEN Datepart(HOUR, ReleaseTimezonedDateTime)>=15 THEN Dateadd(DAY, 1, (CONVERT(VARCHAR(11),
																														   ReleaseTimezonedDateTime,
																														   110)))
												   ELSE (CONVERT(VARCHAR(11), ReleaseTimezonedDateTime, 110))
												 END) THEN 0
							ELSE [dbo].[Fn_effectivedaysbusiness2](CASE
																	WHEN Datepart(HOUR, ReleaseTimezonedDateTime)>=15 THEN Dateadd(DAY, 1, (
																														   CONVERT(VARCHAR(11),
																														   ReleaseTimezonedDateTime
																														   , 110))
																														   )
																	ELSE (CONVERT(VARCHAR(11), ReleaseTimezonedDateTime, 110))
																  END, CONVERT(VARCHAR(11), TD.DropOffTimezonedDateTime, 110))
						  END
					  END AS INT) AS EffectiveDaysReleaseToDropOff	
FROM 	#TMP_SEHAU TS
INNER JOIN #TMP_DT TD ON TS.Salvage_ID = TD.ASAPSalvageID
INNER JOIN DimStock DS WITH(NOLOCK) ON TS.Salvage_ID = DS.ASAPSalvageID
INNER JOIN FactStockCycleTime FS WITH(NOLOCK) ON FS.StockID = DS.StockID


UPDATE FS
SET CalendarDaysReleaseToDropOff = TKD.CalendarDaysReleaseToDropOff,
BusinessDaysReleaseToDropOff = TKD.BusinessDaysReleaseToDropOff,
EffectiveDaysReleaseToDropOff = TKD.EffectiveDaysReleaseToDropOff,
ETLLoadUpdateID=@ETLLoadID_sp
From FactStockCycleTime FS
INNER JOIN 	#TMP_TKD TKD ON FS.StockID = TKD.StockID				  
					  
-- Gets the date from Salvage_Buyer_Finance table using the CDC function and inserts it into a Temp table. 
  CREATE TABLE #TMP_SBF_Data(
	  Salvage_ID [int] NOT NULL
	,Finance_DateTime DATETIME NULL)
INSERT INTO #TMP_SBF_Data (Salvage_ID ,Finance_DateTime)
SELECT Salvage_ID 
   ,CASE WHEN Finance_status = 'can' THEN '12/31/2999' ELSE Finance_DateTime END AS Finance_DateTime
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Buyer_Finance](
   @MinLSN,@MaxLSN,'all with merge')  
   WHERE [__$operation] = 5 

UPDATE FST
SET	 BuyerFinanceDateTime	=
			CASE
				WHEN Isnull(TSE.Finance_DateTime , '2999-12-31') 
						BETWEEN @MinAllowableDate AND @MaxAllowableDate
									THEN Isnull(TSE.Finance_DateTime , '2999-12-31')
				ELSE '2999-12-31' END
	,  ETLLoadUpdateID = @ETLLoadID_sp
FROM FactStockCycleTime FST WITH(NOLOCK)
INNER JOIN DimStock DS  WITH(NOLOCK) ON FST.StockID = DS.StockID
INNER JOIN #TMP_SBF_Data TSE ON TSE.Salvage_ID = DS.ASAPSalvageID

UPDATE FSCT
SET ImageAddedDateTime = COALESCE(newcddate,ImageAddedDateTime),
	ImageFinalizedDateTime = COALESCE(Finalizedcddate,ImageFinalizedDateTime),
	ImageLinkedDateTime = COALESCE(Approvedcddate,ImageLinkedDateTime)
FROM dbo.FactStockCycleTime FSCT WITH(NOLOCK)
INNER JOIN dbo.DimStock DS
	ON FSCT.StockID = DS.StockID
INNER JOIN (select audit.stocknumber
			, Min(Case when audit.statusnew=3 then audit.cddate end ) Approvedcddate 
			, Min(Case when audit.statusnew=2 then audit.cddate end ) Finalizedcddate 
			, Min(Case when audit.statusnew=4 then audit.cddate end ) newcddate 
			from Staging.dbo.statuschangeaudit audit with(Nolock)
			Join Staging.dbo.statusref ref with(Nolock)
			On audit.statusnew = ref.statusref_id 
			--New=4, Open=1, Finalized=2, Approved=3
			where audit.statusnew in (2,3,4) AND cddate > GetDate() - 3
			Group by audit.stocknumber) ImageDates
	ON DS.StockNumber = ImageDates.StockNumber


/*PartialTitleDocsReceivedDateTime*/

CREATE TABLE #TMP_PartialDocsReceivedDateTime
             ( 
			  ASAPSalvageID INT
             ,StockID		INT
             ,PartialDocumentsReceivedDateTime	DATETIME
			 )

INSERT INTO #TMP_PartialDocsReceivedDateTime
            ( 
			  ASAPSalvageID
             ,StockID
             ,PartialDocumentsReceivedDateTime
            )
SELECT PDT.Salvage_ID
	  ,DS.StockID
	  ,PDT.Update_DateTime 
FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Salvage_Title_Partial_Document_Detail](@MinLSN, @MaxLSN, 'all with merge') AS PDT
INNER JOIN [BI_ASAP_Rep].[dbo].[Title_Partial_Document_detail] TP WITH (NOLOCK)
   ON PDT.[Received_Partial_Document_ID] = TP.[Received_Partial_Document_ID]
INNER JOIN EDW.dbo.DimStock DS WITH (NoLock)
   ON PDT.Salvage_ID = DS.ASAPSalvageID
INNER JOIN EDW.dbo.FactStockCycleTime FSCT WITH (NoLock)
   ON FSCT.StockID = DS.StockID
WHERE [__$operation] = 5 
  AND DS.IsTitleDirect = 1 
  AND TP.[Received_Partial_Document_Type] <> 'No'	


UPDATE FS
SET FS.PartialDocumentsReceivedDateTime = T.PartialDocumentsReceivedDateTime
   ,FS.PartialDocumentsReceivedDayID = CAST(CONVERT(CHAR(8),T.PartialDocumentsReceivedDateTime,112) AS INT)
FROM EDW.dbo.FactStockCycleTime FS
INNER JOIN #TMP_PartialDocsReceivedDateTime T 
ON FS.StockID = T.StockID	

/*StateFarm Durations*/

CREATE TABLE #TMP_StateFarmDurations
( StockID INT,
  CalendarCertInToTitleApprovedByProvider DECIMAL(18,9) NULL,
  BusinessCertInToTitleApprovedByProvider DECIMAL(18,9) NULL,
  CalendarTitleApprovedByProviderToSoldProvider DECIMAL(18,9) NULL, 
  BusinessTitleApprovedByProviderToSoldProvider DECIMAL(18,9) NULL,
  CalendarTitleApprovedByProviderToSold DECIMAL(18,9) NULL,
  BusinessTitleApprovedByProviderToSold DECIMAL(18,9) NULL  
)

INSERT INTO #TMP_StateFarmDurations
( StockID,
  CalendarCertInToTitleApprovedByProvider,
  BusinessCertInToTitleApprovedByProvider,
  CalendarTitleApprovedByProviderToSoldProvider, 
  BusinessTitleApprovedByProviderToSoldProvider,
  CalendarTitleApprovedByProviderToSold,
  BusinessTitleApprovedByProviderToSold 
)
SELECT DS.StockID
			  ,CASE
					WHEN TS.CertInDayID<29991231
						 AND TS.TitleApprovedByProviderDayID<29991231 THEN
					  CASE
						WHEN TS.TitleApprovedByProviderDateTime<TS.CertINDateTime THEN 0
						ELSE Cast(TS.TitleApprovedByProviderDateTime AS FLOAT)-Cast(TS.CertINDateTime AS FLOAT)
					  END
				  END CalendarCertInToTitleApprovedByProvider
			  
   ,(CASE
					WHEN TS.CertInDayID<29991231
						 AND TS.TitleApprovedByProviderDayID<29991231 THEN
					  CASE
						WHEN TS.TitleApprovedByProviderDateTime<TS.CertINDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours2](TS.CertINDateTime, TS.TitleApprovedByProviderDateTime, TS.AdministrativeBranchNumber)
					  END
				  END)/9 BusinessCertInToTitleApprovedByProvider 
    ,CASE
					WHEN TS.TitleApprovedByProviderDayID<29991231
						 AND TS.ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN TS.ProviderSoldDateTime<TS.TitleApprovedByProviderDateTime THEN 0
						ELSE Cast(TS.ProviderSoldDateTime AS FLOAT)-Cast(TS.TitleApprovedByProviderDateTime AS FLOAT)
					  END
				  END CalendarTitleApprovedByProviderToSoldProvider
			  
  ,(CASE
					WHEN TS.TitleApprovedByProviderDayID<29991231
						 AND TS.ProviderSoldDayID<29991231 THEN
					  CASE
						WHEN TS.ProviderSoldDateTime<TS.TitleApprovedByProviderDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours2](TS.TitleApprovedByProviderDateTime,TS.ProviderSoldDateTime,TS.AdministrativeBranchNumber)
					  END
				  END)/9 BusinessTitleApprovedByProviderToSoldProvider
,CASE
					WHEN TS.TitleApprovedByProviderDayID<29991231
						 AND TS.SoldDayID<29991231 THEN
					  CASE
						WHEN TS.SoldDateTime<TS.TitleApprovedByProviderDateTime THEN 0
						ELSE Cast(TS.SoldDateTime AS FLOAT)-Cast(TS.TitleApprovedByProviderDateTime AS FLOAT)
					  END
				  END CalendarTitleApprovedByProviderToSold
			  
  ,(CASE
					WHEN TS.TitleApprovedByProviderDayID<29991231
						 AND TS.SoldDayID<29991231 THEN
					  CASE
						WHEN TS.SoldDateTime<TS.TitleApprovedByProviderDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours2](TS.TitleApprovedByProviderDateTime,TS.SoldDateTime, TS.AdministrativeBranchNumber)
					  END
				  END)/9 BusinessTitleApprovedByProviderToSold

FROM  #TMP_FactStockCycleTime_Stage TS WITH(NOLOCK)
INNER JOIN DimStock DS WITH(NOLOCK) ON TS.ASAPSalvageID = DS.ASAPSalvageID
INNER JOIN FactStockCycleTime FS WITH(NOLOCK) ON FS.StockID = DS.StockID
INNER JOIN EDW.dbo.DimProvider DP WITH (NOLOCK) ON DP.ProviderID = DS.CurrentProviderID
WHERE DP.ProviderGroupName = 'State Farm'
		

		UPDATE FSCT
SET CalendarCertInToTitleApprovedByProvider = SF.CalendarCertInToTitleApprovedByProvider 
    ,BusinessCertInToTitleApprovedByProvider = SF.BusinessCertInToTitleApprovedByProvider
    ,CalendarTitleApprovedByProviderToSoldProvider = SF.CalendarTitleApprovedByProviderToSoldProvider
    ,BusinessTitleApprovedByProviderToSoldProvider = SF.BusinessTitleApprovedByProviderToSoldProvider
    ,CalendarTitleApprovedByProviderToSold = SF.CalendarTitleApprovedByProviderToSold
    ,BusinessTitleApprovedByProviderToSold = SF.BusinessTitleApprovedByProviderToSold
FROM EDW.dbo.FactStockCycleTime FSCT WITH(NOLOCK)
INNER JOIN #TMP_StateFarmDurations SF WITH(NOLOCK)
ON FSCT.StockId = SF.StockID

----*TitleProcessingSLADates*-------------------------------

SELECT StockId,MAX(LastUpdateDate) TitleProcessingSLAFromDateTime
             ,Isnull(CONVERT(INT, CONVERT(CHAR(8), Max(LastUpdateDate), 112)), 29991231)TitleProcessingSLAFromDayID
INTO #TMP_MaxTitleDates
FROM
(   
   SELECT StockId,CASE WHEN TitleInDateTime <> '2999-12-31 00:00:00.000' THEN TitleInDateTime ELSE 0 END AS LastUpdateDate
   FROM #TMP_FactStockCycleTime_Stage WITH(NOLOCK)
   UNION
   SELECT StockId,CASE WHEN ReceivedAllDocumentsDateTime <> '2999-12-31 00:00:00.000' THEN ReceivedAllDocumentsDateTime ELSE 0 END AS LastUpdateDate
   FROM #TMP_FactStockCycleTime_Stage WITH(NOLOCK) 
   UNION
   SELECT StockId,CASE WHEN TitleProblemResolvedDate <> '2999-12-31 00:00:00.000' THEN TitleProblemResolvedDate ELSE 0 END AS LastUpdateDate
   FROM #TMP_FactStockCycleTime_Stage WITH(NOLOCK)
   UNION
   SELECT StockId,CASE WHEN SellingBranchReceivedForwardedTitleDocs <> '2999-12-31 00:00:00.000' THEN SellingBranchReceivedForwardedTitleDocs ELSE 0 END AS LastUpdateDate
   FROM #TMP_FactStockCycleTime_Stage WITH(NOLOCK)
  ) MaxDate
GROUP BY StockID

 SELECT FS.StockId,MD.TitleProcessingSLAFromDayID,SMVDDayID,MinSMVDDayID
        ,CASE WHEN MD.TitleProcessingSLAFromDayID <29991231 AND SMVDDayID<29991231 THEN
			CASE WHEN SMVDDateTime < MD.TitleProcessingSLAFromDateTime THEN 0
				ELSE Cast(SMVDDateTime AS FLOAT)-Cast(MD.TitleProcessingSLAFromDateTime  AS FLOAT)
			END
			END CalendarSMVDToTitleProcessingFromDate
	,CASE WHEN MD.TitleProcessingSLAFromDayID <29991231 AND MinSMVDDayID<29991231 THEN
				CASE WHEN MinSMVDDateTime< MD.TitleProcessingSLAFromDateTime THEN 0
				ELSE Cast(MinSMVDDatetime AS FLOAT)-Cast(MD.TitleProcessingSLAFromDateTime AS FLOAT)
			END
			END CalendarMinSMVDToTitleProcessingFromDate
INTO #TMP_Durations					
  FROM [EDW].[dbo].[FactStockCycleTime] FS WITH(NOLOCK)
  INNER JOIN [EDW].[dbo].[DimStock] DS WITH(NOLOCK)
  ON DS.StockID = FS.StockID
  INNER JOIN #TMP_MaxTitleDates MD WITH(NOLOCK)
  ON DS.StockID = MD.StockID
   WHERE FS.TitleProcessingSLAFromDateTime <> MD.TitleProcessingSLAFromDateTime

SELECT FS.StockId, CASE WHEN [TitleProcessingSLAFromDateTime] <> '2999-12-31' AND TitleProcessingSLAFromDateTime < FS.SMVDDatetime THEN
                       CASE WHEN CalendarMinSMVDToTitleProcessingFromDate <> 0 
                       AND CalendarMinSMVDToTitleProcessingFromDate < CalendarSMVDToTitleProcessingFromDate THEN FS.MinSMVDDatetime 
					      ELSE FS.SMVDDatetime END
					   ELSE 0
                       END AS TitleProcessingSLAToDateTime
  INTO #TMP_ToDates
FROM [EDW].[dbo].[FactStockCycleTime] FS WITH(NOLOCK)
INNER JOIN #TMP_Durations TT
ON FS.StockID = TT.StockID  

UPDATE FSCT
SET  FSCT.TitleProcessingSLAFromDateTime = ISNULL(MT.TitleProcessingSLAFromDateTime,'2999-12-31')
    ,FSCT.TitleProcessingSLAFromDayID = CAST(CONVERT(CHAR(8),MT.TitleProcessingSLAFromDateTime,112) AS INT) 
  --SELECT * 
FROM [EDW].[dbo].[FactStockCycleTime] FSCT WITH(NOLOCK)
INNER JOIN #TMP_MaxTitleDates MT
ON FSCT.StockId = MT.StockID

UPDATE FSCT
SET  FSCT.TitleProcessingSLAToDateTime = ISNULL(TD.TitleProcessingSLAToDateTime,'2999-12-31')
    ,FSCT.TitleProcessingSLAToDayID = CAST(CONVERT(CHAR(8),TD.TitleProcessingSLAToDateTime,112) AS INT) 
--SELECT * 
FROM [EDW].[dbo].[FactStockCycleTime] FSCT WITH(NOLOCK)
INNER JOIN #TMP_ToDates TD
ON FSCT.StockId = TD.StockID

----BusinessDaysInspectionServices----

UPDATE FST
SET FST.BusinessDaysInspectionServices = (edw.[dbo].[Fn_edwbusinesshours](
CASE WHEN FST.TimeZonedInspectionServicesFlippedDateTime <> '2999-12-31'
 and FST.TimeZonedInspectionServicesFlippedDateTime > FST.TimeZonedStockFirstDropOffDateTime THEN FST.TimeZonedInspectionServicesFlippedDateTime
 WHEN FST.TimeZonedStockFirstDropOffDateTime <> '2999-12-31' THEN FST.TimeZonedStockFirstDropOffDateTime
ELSE FST.TimeZonedInspectionServicesFlippedDateTime 
END 
,CASE WHEN FST.ImageLinkedDateTime <> '2999-12-31' THEN FST.ImageLinkedDateTime
 ELSE FST.TimeZonedInspectionServicesCompletedDateTime
END 
,DS.AdministrativeBranchNumber)
 /9 )

FROM EDW.dbo.FactStockCycleTime		FST WITH(NOLOCK)
INNER JOIN #TMP_ISFDT	IT WITH(NOLOCK)
ON FST.StockID = IT.StockID
INNER JOIN #TMP_ISRCDT	ITT WITH(NOLOCK)
ON FST.StockID = ITT.StockID
INNER JOIN #TMP_DT	dt  WITH(NOLOCK)
ON FST.StockID = DT.StockID
INNER JOIN DimStock DS WITH(NOLOCK) 
ON FST.Stockid = DS.Stockid
where FST.TimeZonedInspectionServicesFlippedDateTime <> '2999-12-31'
or FST.ImageLinkedDateTime <> '2999-12-31'
Or FST.TimeZonedInspectionServicesCompletedDateTime  <> '2999-12-31'


		  IF OBJECT_ID('tempdb..#TMP_StockIDnASAPSalvageID') IS NOT NULL DROP TABLE #TMP_StockIDnASAPSalvageID
		  IF OBJECT_ID('#tempdb..#TMP_ASAPSalvageID_CertInDate') IS NOT NULL DROP TABLE #TMP_ASAPSalvageID_CertInDate
		  IF Object_id('tempdb..#TMP_CT_Salvage_Info') IS NOT NULL DROP TABLE #TMP_CT_Salvage_Info
		  IF Object_id('tempdb..#TMP_CT_Dispatch_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Dispatch_DateTime
		  IF Object_id('tempdb..#TMP_CT_Pickup_DateTime') IS NOT NULL   DROP TABLE #TMP_CT_Pickup_DateTime
		  IF Object_id('tempdb..#TMP_CT_Settlement_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_Settlement_SalvageIDs
		  IF Object_id('tempdb..#TMP_CT_Settlement_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Settlement_DateTime
		  IF Object_id('tempdb..#TMP_CT_Earliest_Sale_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Earliest_Sale_DateTime
		  IF Object_id('tempdb..#TMP_CT_SEH_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SEH_SalvageIDs
		  IF Object_id('tempdb..#TMP_CT_SEH_Dates') IS NOT NULL DROP TABLE #TMP_CT_SEH_Dates
		  IF Object_id('tempdb..#TMP_CT_SEH_Dates') IS NOT NULL DROP TABLE #TMP_CT_SEH_Dates_Min
		  IF Object_id('tempdb..#TMP_CT_Cert_In_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Cert_In_DateTime
		  IF Object_id('tempdb..#TMP_CT_Cert_In_DateTime_Min') IS NOT NULL DROP TABLE #TMP_CT_Cert_In_DateTime_Min

		  IF Object_id('tempdb..#TMP_CT_dups') IS NOT NULL DROP TABLE #TMP_CT_dups
		  IF Object_id('tempdb..#TMP_CT_BuyerPickup') IS NOT NULL DROP TABLE #TMP_CT_BuyerPickup
		  IF Object_id('tempdb..#TMP_CT_Provider_Refund') IS NOT NULL DROP TABLE #TMP_CT_Provider_Refund
		  IF Object_id('tempdb..#TMP_CT_CPSD_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_CPSD_SalvageIDs
		  IF Object_id('tempdb..#TMP_CT_CPSS_Datetime') IS NOT NULL  DROP TABLE #TMP_CT_CPSS_Datetime
		  IF Object_id('tempdb..#TMP_CT_SMVD') IS NOT NULL DROP TABLE #TMP_CT_SMVD
		  IF Object_id('tempdb..#TMP_CT_SMVD_Datetime') IS NOT NULL DROP TABLE #TMP_CT_SMVD_Datetime
		  IF Object_id('tempdb..#TMP_CT_SMVD_Datetime_Min') IS NOT NULL DROP TABLE #TMP_CT_SMVD_Datetime_Min

		  IF Object_id('tempdb..#TMP_CT_ASI') IS NOT NULL DROP TABLE #TMP_CT_ASI
		  IF Object_id('tempdb..#TMP_CT_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SalvageIDs
		  IF Object_id('tempdb..#TMP_FactStockCycleTime_Stage') IS NOT NULL DROP TABLE #TMP_FactStockCycleTime_Stage
		  IF Object_id('tempdb..#TMP_Release_Problem_DateTime_By_SalvageID') IS NOT NULL DROP TABLE #TMP_Release_Problem_DateTime_By_SalvageID
		  IF Object_id('tempdb..#TMP_Release_Problem_Dates') IS NOT NULL DROP TABLE #TMP_Release_Problem_Dates
		  IF Object_id('tempdb..#TMP_Release_Problem_Time') IS NOT NULL DROP TABLE #TMP_Release_Problem_Time
		  IF Object_id('tempdb..#TMP_FSCTDuraions_Stage') IS NOT NULL  DROP TABLE #TMP_FSCTDuraions_Stage
		  IF Object_id('tempdb..#temp_FSCTRecordCount') IS NOT NULL  DROP TABLE #temp_FSCTRecordCount
		  IF Object_id('tempdb..#TMP_IBNStarted') IS NOT NULL DROP TABLE #TMP_IBNStarted
		  IF Object_id('tempdb..#TMP_IBNEnded') IS NOT NULL DROP TABLE #TMP_IBNEnded
		  IF Object_id('tempdb..#TMP_CT_ASD') IS NOT NULL DROP TABLE #TMP_CT_ASD
		  IF OBJECT_ID('tempdb..#TMP_FactStockEvent') IS NOT NULL DROP TABLE #TMP_FactStockEvent
		  IF OBJECT_ID('tempdb..#TMP_Hold_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Dates
		  IF OBJECT_ID('tempdb..#TMP_Hold_Final_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Final_Dates
		  IF OBJECT_ID('tempdb..#TMP_Hold_Durations') IS NOT NULL DROP TABLE #TMP_Hold_Durations
		  IF Object_id('tempdb..#TMP_Duration') IS NOT NULL DROP TABLE #TMP_Duration		  
		  IF Object_id('tempdb..#TMP_CT_Salvage_Provider_Info') IS NOT NULL DROP TABLE #TMP_CT_Salvage_Provider_Info
		  IF Object_id('tempdb..#TMP_CT_Salavage_Event_IBTR_IBCN') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_IBTR_IBCN
		  IF Object_id('tempdb..#TMP_CT_Salavage_Event_Checkin') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_Checkin
		  IF Object_id('tempdb..#TMP_CT_Salavage_Event_Checkin_Future') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_Checkin_Future
		  IF Object_id('tempdb..#TMP_CT_Salavage_Event_REAB') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_REAB
		  IF Object_id('tempdb..#TMP_Anticipated_SalvageIDs') IS NOT NULL DROP TABLE #TMP_Anticipated_SalvageIDs
		  IF Object_id('tempdb..#TMP_PSD_FINAL') IS NOT NULL DROP TABLE #TMP_PSD_FINAL
		  IF Object_id('tempdb..#TMP_SEHA') IS NOT NULL DROP TABLE #TMP_SEHA
		  IF Object_id('tempdb..#TMP_PSD') IS NOT NULL DROP TABLE #TMP_PSD
		  IF Object_id('tempdb..#TMP_STLV') IS NOT NULL DROP TABLE #TMP_STLV
		  IF Object_id('tempdb..#TMP_BRC') IS NOT NULL DROP TABLE #TMP_BRC
		  IF Object_id('tempdb..#TMP_DT') IS NOT NULL DROP TABLE #TMP_DT		  
		  IF Object_id('tempdb..#TMP_SEHAU') IS NOT NULL DROP TABLE #TMP_SEHAU	  
		  IF Object_id('tempdb..#TMP_TKD') IS NOT NULL DROP TABLE #TMP_TKD
		  IF OBJECT_ID('tempdb..#TMP_Salvage_Provider_Info_AdvanceChargePrepaidThruDateTime') IS NOT NULL DROP TABLE #TMP_Salvage_Provider_Info_AdvanceChargePrepaidThruDateTime
		  IF Object_id('tempdb..#TMP_SBF_Data') IS NOT NULL DROP TABLE #TMP_SBF_Data	
		  IF OBJECT_ID('tempdb..#TMP_ISCDT') IS NOT NULL DROP TABLE #TMP_ISCDT
		  IF OBJECT_ID('tempdb..#TMP_ISRCDT') IS NOT NULL DROP TABLE #TMP_ISRCDT
		  IF OBJECT_ID('tempdb..#TMP_ISACDT') IS NOT NULL DROP TABLE #TMP_ISACDT
		  --IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Cancelled') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Cancelled
    --      IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Completed') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Completed
		  IF OBJECT_ID('tempdb..#TMP_PartialDocsReceivedDateTime') IS NOT NULL DROP TABLE #TMP_PartialDocsReceivedDateTime	
		  IF OBJECT_ID('tempdb..#TMP_StateFarmDurations') IS NOT NULL DROP TABLE #TMP_StateFarmDurations 
		  IF OBJECT_ID('tempdb..#TMP_ImagesCompleted') IS NOT NULL DROP TABLE #TMP_ImagesCompleted
		  IF OBJECT_ID('tempdb..#TMP_ImagesCancelled') IS NOT NULL DROP TABLE #TMP_ImagesCancelled
		  IF OBJECT_ID('tempdb..#TMP_ReportsCompleted') IS NOT NULL DROP TABLE #TMP_ReportsCompleted
		  IF OBJECT_ID('tempdb..#TMP_ReportsCancelled') IS NOT NULL DROP TABLE #TMP_ReportsCancelled
		  IF OBJECT_ID('tempdb..#TMP_AppraisalCompleted') IS NOT NULL DROP TABLE #TMP_AppraisalCompleted
		  IF OBJECT_ID('tempdb..#TMP_AppraisalCancelled') IS NOT NULL DROP TABLE #TMP_AppraisalCancelled
		  IF OBJECT_ID('tempdb..#TMP_MaxTitleDates') IS NOT NULL DROP TABLE #TMP_MaxTitleDates
		  IF OBJECT_ID('tempdb..#TMP_Durations') IS NOT NULL DROP TABLE #TMP_Durations
		  IF OBJECT_ID('tempdb..#TMP_ToDates') IS NOT NULL DROP TABLE #TMP_ToDates				
				  	  
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

		  IF Object_id('tempdb..#TMP_CT_Salvage_Info') IS NOT NULL DROP TABLE #TMP_CT_Salvage_Info
		  IF Object_id('tempdb..#TMP_CT_Dispatch_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Dispatch_DateTime
		  IF Object_id('tempdb..#TMP_CT_Pickup_DateTime') IS NOT NULL   DROP TABLE #TMP_CT_Pickup_DateTime
		  IF Object_id('tempdb..#TMP_CT_Settlement_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_Settlement_SalvageIDs
		  IF Object_id('tempdb..#TMP_CT_Settlement_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Settlement_DateTime
		  IF Object_id('tempdb..#TMP_CT_Earliest_Sale_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Earliest_Sale_DateTime
		  IF Object_id('tempdb..#TMP_CT_SEH_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SEH_SalvageIDs
		  IF Object_id('tempdb..#TMP_CT_SEH_Dates') IS NOT NULL DROP TABLE #TMP_CT_SEH_Dates
		  IF Object_id('tempdb..#TMP_CT_SEH_Dates_Min') IS NOT NULL DROP TABLE #TMP_CT_SEH_Dates_Min
				 

		  IF Object_id('tempdb..#TMP_CT_Cert_In_DateTime') IS NOT NULL DROP TABLE #TMP_CT_Cert_In_DateTime
		  IF Object_id('tempdb..#TMP_CT_Cert_In_DateTime_Min') IS NOT NULL DROP TABLE #TMP_CT_Cert_In_DateTime_Min

		  IF Object_id('tempdb..#TMP_CT_dups') IS NOT NULL DROP TABLE #TMP_CT_dups
		  IF Object_id('tempdb..#TMP_CT_BuyerPickup') IS NOT NULL DROP TABLE #TMP_CT_BuyerPickup
		  IF Object_id('tempdb..#TMP_CT_Provider_Refund') IS NOT NULL DROP TABLE #TMP_CT_Provider_Refund
		  IF Object_id('tempdb..#TMP_CT_CPSD_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_CPSD_SalvageIDs
		  IF Object_id('tempdb..#TMP_CT_CPSS_Datetime') IS NOT NULL  DROP TABLE #TMP_CT_CPSS_Datetime
		  IF Object_id('tempdb..#TMP_CT_SMVD') IS NOT NULL DROP TABLE #TMP_CT_SMVD
		  IF Object_id('tempdb..#TMP_CT_SMVD_Datetime') IS NOT NULL DROP TABLE #TMP_CT_SMVD_Datetime
		  IF Object_id('tempdb..#TMP_CT_SMVD_Datetime_Min') IS NOT NULL DROP TABLE #TMP_CT_SMVD_Datetime_Min

		  IF Object_id('tempdb..#TMP_CT_ASI') IS NOT NULL DROP TABLE #TMP_CT_ASI
		  IF Object_id('tempdb..#TMP_CT_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SalvageIDs
		  IF Object_id('tempdb..#TMP_FactStockCycleTime_Stage') IS NOT NULL DROP TABLE #TMP_FactStockCycleTime_Stage
		  IF Object_id('tempdb..#TMP_Release_Problem_DateTime_By_SalvageID') IS NOT NULL DROP TABLE #TMP_Release_Problem_DateTime_By_SalvageID
		  IF Object_id('tempdb..#TMP_Release_Problem_Dates') IS NOT NULL DROP TABLE #TMP_Release_Problem_Dates
		  IF Object_id('tempdb..#TMP_Release_Problem_Time') IS NOT NULL DROP TABLE #TMP_Release_Problem_Time
		  IF Object_id('tempdb..#TMP_FSCTDuraions_Stage') IS NOT NULL  DROP TABLE #TMP_FSCTDuraions_Stage
		  IF Object_id('tempdb..#temp_FSCTRecordCount') IS NOT NULL  DROP TABLE #temp_FSCTRecordCount
		  IF Object_id('tempdb..#TMP_IBNStarted') IS NOT NULL DROP TABLE #TMP_IBNStarted
		  IF Object_id('tempdb..#TMP_IBNEnded') IS NOT NULL DROP TABLE #TMP_IBNEnded
		  IF Object_id('tempdb..#TMP_CT_ASD') IS NOT NULL DROP TABLE #TMP_CT_ASD
		  IF OBJECT_ID('tempdb..#TMP_FactStockEvent') IS NOT NULL DROP TABLE #TMP_FactStockEvent
		  IF OBJECT_ID('tempdb..#TMP_Hold_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Dates
		  IF OBJECT_ID('tempdb..#TMP_Hold_Final_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Final_Dates
		  IF OBJECT_ID('tempdb..#TMP_Hold_Durations') IS NOT NULL DROP TABLE #TMP_Hold_Durations
		  IF Object_id('tempdb..#TMP_Duration') IS NOT NULL DROP TABLE #TMP_Duration
		  IF Object_id('tempdb..#TMP_CT_Salvage_Provider_Info') IS NOT NULL DROP TABLE #TMP_CT_Salvage_Provider_Info
		  IF Object_id('tempdb..#TMP_CT_Salavage_Event_IBTR_IBCN') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_IBTR_IBCN
		  IF Object_id('tempdb..#TMP_CT_Salavage_Event_Checkin') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_Checkin
		  IF Object_id('tempdb..#TMP_CT_Salavage_Event_Checkin_Future') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_Checkin_Future
	  	  IF Object_id('tempdb..#TMP_CT_Salavage_Event_REAB') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_REAB
	  	  IF Object_id('tempdb..#TMP_Anticipated_SalvageIDs') IS NOT NULL DROP TABLE #TMP_Anticipated_SalvageIDs
		  IF Object_id('tempdb..#TMP_PSD_FINAL') IS NOT NULL DROP TABLE #TMP_PSD_FINAL
		  IF Object_id('tempdb..#TMP_SEHA') IS NOT NULL DROP TABLE #TMP_SEHA
		  IF Object_id('tempdb..#TMP_PSD') IS NOT NULL DROP TABLE #TMP_PSD
		  IF Object_id('tempdb..#TMP_STLV') IS NOT NULL DROP TABLE #TMP_STLV
		  IF Object_id('tempdb..#TMP_BRC') IS NOT NULL DROP TABLE #TMP_BRC
		  IF Object_id('tempdb..#TMP_DT') IS NOT NULL DROP TABLE #TMP_DT
		  IF Object_id('tempdb..#TMP_SEHAU') IS NOT NULL DROP TABLE #TMP_SEHAU	
		  IF Object_id('tempdb..#TMP_TKD') IS NOT NULL DROP TABLE #TMP_TKD		
		  IF OBJECT_ID('tempdb..#TMP_Salvage_Provider_Info_AdvanceChargePrepaidThruDateTime') IS NOT NULL DROP TABLE #TMP_Salvage_Provider_Info_AdvanceChargePrepaidThruDateTime
		  IF Object_id('tempdb..#TMP_SBF_Data') IS NOT NULL DROP TABLE #TMP_SBF_Data
		--  IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Cancelled') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Cancelled
         -- IF OBJECT_ID('tempdb..#TMP_InspectionServices_Enhancement_Completed') IS NOT NULL DROP TABLE #TMP_InspectionServices_Enhancement_Completed		  
		  IF OBJECT_ID('tempdb..#TMP_ISCDT') IS NOT NULL DROP TABLE #TMP_ISCDT
		  IF OBJECT_ID('tempdb..#TMP_ISRCDT') IS NOT NULL DROP TABLE #TMP_ISRCDT
		  IF OBJECT_ID('tempdb..#TMP_ISACDT') IS NOT NULL DROP TABLE #TMP_ISACDT
		  --IF OBJECT_ID('tempdb..#TMP_InspectionServices_Reports_Cancelled') IS NOT NULL DROP TABLE #TMP_InspectionServices_Reports_Cancelled
		  --IF OBJECT_ID('tempdb..#TMP_InspectionServices_Reports_Completed') IS NOT NULL DROP TABLE #TMP_InspectionServices_Reports_Completed
		  IF OBJECT_ID('tempdb..#TMP_PartialDocsReceivedDateTime') IS NOT NULL DROP TABLE #TMP_PartialDocsReceivedDateTime
		  IF OBJECT_ID('tempdb..#TMP_StateFarmDurations') IS NOT NULL DROP TABLE #TMP_StateFarmDurations 
		  IF OBJECT_ID('tempdb..#TMP_ImagesCompleted') IS NOT NULL DROP TABLE #TMP_ImagesCompleted
		  IF OBJECT_ID('tempdb..#TMP_ImagesCancelled') IS NOT NULL DROP TABLE #TMP_ImagesCancelled
		  IF OBJECT_ID('tempdb..#TMP_ReportsCompleted') IS NOT NULL DROP TABLE #TMP_ReportsCompleted
		  IF OBJECT_ID('tempdb..#TMP_ReportsCancelled') IS NOT NULL DROP TABLE #TMP_ReportsCancelled
		  IF OBJECT_ID('tempdb..#TMP_AppraisalCompleted') IS NOT NULL DROP TABLE #TMP_AppraisalCompleted
		  IF OBJECT_ID('tempdb..#TMP_AppraisalCancelled') IS NOT NULL DROP TABLE #TMP_AppraisalCancelled
		  IF OBJECT_ID('tempdb..#TMP_MaxTitleDates') IS NOT NULL DROP TABLE #TMP_MaxTitleDates
		  IF OBJECT_ID('tempdb..#TMP_Durations') IS NOT NULL DROP TABLE #TMP_Durations
		  IF OBJECT_ID('tempdb..#TMP_ToDates') IS NOT NULL DROP TABLE #TMP_ToDates				
		  IF OBJECT_ID('tempdb..#TMP_LastTitleAssistForm') IS NOT NULL DROP TABLE #TMP_LastTitleAssistForm		

		  
		  RETURN -1
	  END CATCH

  END







GO


