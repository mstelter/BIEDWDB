
/* 
USE EDW
GO 
=========================================================================================================================
Author		: Matt Stelter
Create date	: 09/08/2011
Description	: Populate the EDW FactStockTowCycleTime table from CDC	
=========================================================================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
10/3/2011	Matt Stelter	
10/24/2011	Matt Stelter	Changed calender durations to use minus instead of date diff & eliminate negative durations
10/24/2011	Juan X. Jacome		-- Added code to drop temp Tables (DBA Code Review)
11/28/2011 Matt Stelter		Added updates for Timezoned fields & fix for Towbill = 0
12/15/2011	Matt Stelter	Fix for Tow completed date
12/16/2011  Juan X. JAcome	--Added nolock (DBA Review)
02/03/2011 Preetham Duvva   Changed the BranchNumber to AdministrativeBranchNumber from DimStock 
12/15/2011	Matt Stelter	Fix for Pickup date to only use DROP event if there is a Towbill associated
11/19/2012  Preetham Duvva  Added Tower Kiosk DropOffDateTime and Durations
12/12/2012  Preetham Duvva    Added date validation check for Tower Kiosk DropOffDate
2/4/2015	Matt Stelter	Changed Release to Pickup and Release to Drop Off business and effective to use Holiday2Flag
==========================================================================================================================
Usage Example:
Exec [usp_ETLFactStockTowCycleTime]
==========================================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactStockTowCycleTime]
AS     
BEGIN

DECLARE @ETLLoadID      INT,
        @InsertRowCount INT,
        @UpdateRowCount INT,
        @DeleteRowCount INT,
        @StartLSN       BINARY(10),
        @EndLSN         BINARY(10),

		@MinAllowableDate		Datetime,
		@MaxAllowableDate		Datetime

SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactStockTowCycleTime'

--set @ETLLoadID = 1000000

--Get the start and end LSNs
SELECT @StartLSN=MinLSN,
       @EndLSN=MaxLSN
FROM   dbo.ETLRun
WHERE  ETLJobName='DailyETLJob'

SELECT @MinAllowableDate = MIN(DayDate), @MaxAllowableDate = MAX(DayDate) from DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231

--SELECT @StartLSN = MIN(__$start_lsn), @EndLSN = MAX(__$start_lsn) FROM BI_ASAP_REP.cdc.dbo_Salvage_Event_History_CT

IF Object_id('tempdb..#FSTCT_Changes') IS NOT NULL
  DROP TABLE #FSTCT_Changes

CREATE TABLE #FSTCT_Changes
  (Salvage_ID          INT NULL,
   StockID             INT NULL,
   Towbill_Number      INT NULL,
   Tow_Days            SMALLINT NULL,
   Travel_Time_Minutes SMALLINT NULL,
   Operation           INT NULL)

CREATE INDEX IX_Salvage_ID_Towbill_Number
  ON #FSTCT_Changes ( StockID, Towbill_Number )
  WITH FILLFACTOR = 100;


IF Object_id('tempdb..#All_Changes') IS NOT NULL
  DROP TABLE #All_Changes

CREATE TABLE #All_Changes
  (Salvage_ID         INT NULL,
   StockID            INT NULL,
   Towbill_Number     INT NULL)

CREATE INDEX IX_ALL_Changes_Salvage_ID_Towbill_Number
  ON #All_Changes ( Salvage_ID, Towbill_Number )
  WITH FILLFACTOR = 100;

IF Object_id('tempdb..#SEH_Changes') IS NOT NULL
  DROP TABLE #SEH_Changes

CREATE TABLE #SEH_Changes
  (Salvage_ID         INT NULL,
   StockID            INT NULL,
   Event_DateTime     DATETIME NULL,
   Salvage_Event_Code CHAR(4) NULL,
   Towbill_Number     INT NULL,
   Operation          INT NULL)

CREATE INDEX IX_SEH_Changes_Salvage_ID_Towbill_Number
  ON #SEH_Changes ( StockID, Towbill_Number )
  WITH FILLFACTOR = 100;

CREATE INDEX IX_SEH_Changes_Code
  ON #SEH_Changes (Salvage_Event_Code)
  WITH FILLFACTOR = 100;
  
CREATE TABLE #TMP_SEH
( StockID INT NOT NULL,
  Towbill_Number INT, 
  Event_DateTime DATETIME)

CREATE TABLE #TMP_DT
( StockID INT NOT NULL,
  Towbill_Number INT, 
  DropOffTimezonedDateTime DATETIME,
  DropOffTimezonedDayID INT) 

IF Object_id('tempdb..#CycleTimeDates') IS NOT NULL
  DROP TABLE #CycleTimeDates

CREATE TABLE #CycleTimeDates(
	[StockID] [int] NOT NULL,
	[TowBillNumber] [int] NULL,
	[ActualReleaseDayID] [int] NULL,
	[ActualReleaseDateTime] [datetime] NULL,
	[AssignmentDayID] [int] NULL,
	[AssignmentDateTime] [datetime] NULL,
	[AssignToPickupBusinessHours] [int] NULL,
	[BuyerTowDispatchCompletedDayID] [int] NULL,
	[BuyerTowDispatchCompletedDateTime] [datetime] NULL,
	[BuyerTowRequestDayID] [int] NULL,
	[BuyerTowRequestDateTime] [datetime] NULL,
	[DispatchCancelledDayID] [int] NULL,
	[DispatchCancelledDateTime] [datetime] NULL,
	[DispatchPermanentlyCancelledDayID] [int] NULL,
	[DispatchPermanentlyCancelledDateTime] [datetime] NULL,
	[DispatchToLoginBusiness] [decimal](18, 9) NULL,
	[DispatchToLoginCalendar] [decimal](18, 9) NULL,
	[DispatchToPickupBusiness] [decimal](18, 9) NULL,
	[DispatchToPickupCalendar] [decimal](18, 9) NULL,
	[DispatchTowerAssignedDayID] [int] NULL,
	[DispatchTowerAssignedDateTime] [datetime] NULL,
	[DispatchTowerDayID] [int] NULL,
	[DispatchTowerDateTime] [datetime] NULL,
	[DispatchTowerLoggedInDayID] [int] NULL,
	[DispatchTowerLoggedInDateTime] [datetime] NULL,
	[DispatchTowerLoggedOutDayID] [int] NULL,
	[DispatchTowerLoggedOutDateTime] [datetime] NULL,
	[DryRunDayID] [int] NULL,
	[DryRunDateTime] [datetime] NULL,
	[DryRunEventDayID] [int] NULL,
	[DryRunEventDateTime] [datetime] NULL,
	[InterbranchtransferBusinessHours] [int] NULL,
	[InterbranchTransferDayID] [int] NULL,
	[InterbranchTransferDateTime] [datetime] NULL,
	[OffsitePickupDayID] [int] NULL,
	[OffsitePickupDateTime] [datetime] NULL,
	[OffsiteTowDayID] [int] NULL,
	[OffsiteTowDateTime] [datetime] NULL,
	[PickupEnrouteToPickupBusiness] [decimal](18, 9) NULL,
	[PickupEnrouteToPickupCalendar] [decimal](18, 9) NULL,
	[PickupToLoginBusiness] [decimal](18, 9) NULL,
	[PickupToLoginCalendar] [decimal](18, 9) NULL,
	[ReassignAdministrativeBranchDayID] [int] NULL,
	[ReassignAdministrativeBranchDateTime] [datetime] NULL,
	[ReleaseToPickupBusiness] [decimal](18, 9) NULL,
	[ReleaseToPickupBusinessHours] [int] NULL,
	[ReleaseToPickupCalendar] [decimal](18, 9) NULL,
	[ReleaseToPickupEffectiveDaysBusiness] [decimal](18, 9) NULL,
	[ReleaseToPickupEffectiveDaysCalendar] [decimal](18, 9) NULL,
	[ReturnTowDayID] [int] NULL,
	[ReturnTowDateTime] [datetime] NULL,
	[SalvageDroppedOffDayID] [int] NULL,
	[SalvageDroppedOffDateTime] [datetime] NULL,
	[SoldDayID] [int] NULL,
	[SoldDateTime] [datetime] NULL,
	[TowCompletedDayID] [int] NULL,
	[TowCompletedDateTime] [datetime] NULL,
	[TowDays] [int] NULL,
	[TowInitiatedDayID] [int] NULL,
	[TowInitiatedDateTime] [datetime] NULL,
	[TravelTimeMinutes] [int] NULL,
	[WaitDispatchToWaitDriverBusiness] [decimal](18, 9) NULL,
	[WaitDispatchToWaitDriverCalendar] [decimal](18, 9) NULL,
	[WaitDriverToPickupEnrouteBusiness] [decimal](18, 9) NULL,
	[WaitDriverToPickupEnrouteCalendar] [decimal](18, 9) NULL,
	[WaitReleaseToWaitDispatchBusiness] [decimal](18, 9) NULL,
	[WaitReleaseToWaitDispatchCalendar] [decimal](18, 9) NULL,
	BranchNumber [int] NULL,
	TowTypeCode [nchar](2) NULL,
	[ReturnTowBusinessHours] [decimal](18, 9) NULL,
	ActualReleaseTimezonedDayId [int] NULL,
	SalvageDroppedOffTimezonedDayId [int] NULL,
	ActualReleaseTimezonedDateTime [datetime] NULL, 
	SalvageDroppedOffTimezonedDatetime [datetime] NULL,
	DispatchTowerLoggedInTimezonedDayId INT NULL,
	DispatchTowerLoggedInTimezonedDatetime Datetime NULL,
	DropOffDateTime Datetime NULL,
	DropOffDayID INT NULL,
	[CalendarDaysReleaseToDropOff] [decimal](18, 9) NULL,
	[BusinessDaysReleaseToDropOff] [decimal](18, 9) NULL,
	[EffectiveDaysReleaseToDropOff] [decimal](18, 9) NULL
	)
	
	CREATE INDEX IX_CycleTimeDates_Salvage_ID_Towbill_Number
  ON #CycleTimeDates ( StockID, TowbillNumber )
  WITH FILLFACTOR = 100;

--This table is used to map the relationship between the Tow Type and the Salvage Event Code for Non-Buyer tows
IF Object_id('tempdb..#TowMapping') IS NOT NULL DROP TABLE #TowMapping
CREATE TABLE #TowMapping(TowTypeCode varchar(2),SalvageEventCode varchar(4))
	INSERT INTO #TowMapping(TowTypeCode ,SalvageEventCode) VALUES('IB','IBTR')
	INSERT INTO #TowMapping(TowTypeCode ,SalvageEventCode) VALUES('RT','RTRQ')
	INSERT INTO #TowMapping(TowTypeCode ,SalvageEventCode) VALUES('OS','OSIA')
	INSERT INTO #TowMapping(TowTypeCode ,SalvageEventCode) VALUES('OP','OSPU')
	INSERT INTO #TowMapping(TowTypeCode ,SalvageEventCode) VALUES('OS','OSPU')
	--INSERT INTO #TowMapping(TowTypeCode ,SalvageEventCode) VALUES('PK','RLPD')
	INSERT INTO #TowMapping(TowTypeCode ,SalvageEventCode) VALUES('PK','DROP')

	CREATE INDEX IX_TowMapping
  ON #TowMapping ( TowTypeCode, SalvageEventCode )
  WITH FILLFACTOR = 100;

--This table is used to map the relationship between the Tow Type and the Salvage Event Code for Buyer tows
IF Object_id('tempdb..#TowMappingBY') IS NOT NULL DROP TABLE #TowMappingBY
CREATE TABLE #TowMappingBY(TowTypeCode varchar(2),SalvageEventCode varchar(4))
	INSERT INTO #TowMappingBY(TowTypeCode ,SalvageEventCode) VALUES('BY','BYTR')
	INSERT INTO #TowMappingBY(TowTypeCode ,SalvageEventCode) VALUES('BY','SA16')

	CREATE INDEX IX_TowMappingBY
  ON #TowMappingBY ( TowTypeCode, SalvageEventCode )
  WITH FILLFACTOR = 100;
  
IF Object_id('tempdb..#INIT') IS NOT NULL DROP TABLE #INIT
CREATE TABLE #INIT(Init_DateTime DATETIME NULL,Compl_DateTime DATETIME NULL,Salvage_ID INT,Towbill_Number INT, StockID int)
	CREATE INDEX IX_INIT
  ON #INIT ( StockID, Towbill_Number )
  WITH FILLFACTOR = 100;

IF Object_id('tempdb..#INITBY') IS NOT NULL DROP TABLE #INITBY
CREATE TABLE #INITBY(Init_DateTime DATETIME NULL,Compl_DateTime DATETIME NULL,Salvage_ID INT,Towbill_Number INT, StockID int)
	CREATE INDEX IX_INITBY
  ON #INITBY ( StockID, Towbill_Number )
  WITH FILLFACTOR = 100;


IF OBJECT_ID('tempdb..#recordCount') IS NOT NULL DROP TABLE #recordCount
CREATE TABLE #recordCount (ChangeType VARCHAR(10)) 
/*********************************************************************************************
Get the values from the CDC Current Salvage_Tow table into the temp table
*********************************************************************************************/
INSERT INTO #FSTCT_Changes
            (Salvage_ID,
			StockID,	
             Towbill_Number,
             Tow_Days,
             Travel_Time_Minutes,
             Operation)
SELECT Salvage_ID,
		DS.StockID,
       Towbill_Number,
       Tow_Days,
       Travel_Time_Minutes,
       __$operation
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_tow] (@StartLSN, @EndLSN, 'all with merge') ST
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON ST.Salvage_ID=DS.ASAPSalvageID
WHERE  [__$operation]=5
/*********************************************************************************************
Get the values from the CDC Salvage Event History table into the temp table
*********************************************************************************************/
--Towbill Not Null
INSERT INTO #All_Changes
  (Salvage_ID,
   StockID,
   Towbill_Number)
SELECT DISTINCT Salvage_ID,
		DS.StockID,
       Towbill_Number
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history] (@StartLSN, @EndLSN, 'all with merge') SEH
       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON SEH.Salvage_ID=DS.ASAPSalvageID
WHERE  [__$operation]=5 
   AND Salvage_Event_Code IN ('BYTC', 'DPCN', 'DPPC', 'DPAS',
                              'DPDP', 'DPLI', 'DPLO', 'DPDR',
                              'REAB', 'DROP', 'RLPD', 'ASGN',
                              'SAL6', 'IBTR', 'OSPU', 'OSIA',
                              'RTRQ', 'SAL4','SDRO')
                              
INSERT INTO #All_Changes
  (Salvage_ID,
   StockID,
   Towbill_Number)
SELECT DISTINCT FSTCTC.Salvage_ID,
		FSTCTC.StockID,
       FSTCTC.Towbill_Number
FROM   #FSTCT_Changes FSTCTC
LEFT OUTER JOIN #All_Changes AC
ON  FSTCTC.Salvage_ID = AC.Salvage_ID                             
AND FSTCTC.Towbill_Number = AC.Towbill_Number
WHERE AC.Towbill_Number IS NULL
                              
--INSERT INTO #SEH_Changes
--            (Salvage_ID,
--			StockID,
--             Event_DateTime,
--             Salvage_Event_Code,
--             Towbill_Number,
--             Operation)
--SELECT Salvage_ID,
--		DS.StockID,
--       Event_DateTime,
--       Salvage_Event_Code,
--       Towbill_Number,
--       __$operation
--FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history] (@StartLSN, @EndLSN, 'all with merge') SEH
--       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
--         ON SEH.Salvage_ID=DS.ASAPSalvageID
--WHERE  [__$operation]=5 
--   AND Salvage_Event_Code IN ('BYTC', 'DPCN', 'DPPC', 'DPAS',
--                              'DPDP', 'DPLI', 'DPLO', 'DPDR',
--                              'REAB', 'DROP', 'RLPD', 'ASGN',
--                              'SAL6', 'IBTR', 'OSPU', 'OSIA',
--                              'RTRQ', 'SAL4')

INSERT INTO #SEH_Changes
            (Salvage_ID,
			StockID,
             Event_DateTime,
             Salvage_Event_Code,
             Towbill_Number)
SELECT DISTINCT SEH.Salvage_ID,
		AC.StockID,
       SEH.Event_DateTime,
       SEH.Salvage_Event_Code,
       SEH.Towbill_Number
FROM BI_ASAP_REP.dbo.Salvage_Event_History SEH WITH (NOLOCK)
INNER JOIN #All_Changes AC
ON SEH.Salvage_ID = AC.Salvage_ID 
WHERE Salvage_Event_Code IN ('BYTC', 'DPCN', 'DPPC', 'DPAS',
                              'DPDP', 'DPLI', 'DPLO', 'DPDR',
                              'REAB', --'DROP',
							   'RLPD', 'ASGN',
                              'SAL6', 'IBTR', 'OSPU', 'OSIA',
                              'RTRQ', 'SAL4','SDRO')

INSERT INTO #SEH_Changes
            (Salvage_ID,
			StockID,
             Event_DateTime,
             Salvage_Event_Code,
             Towbill_Number)
SELECT DISTINCT SEH.Salvage_ID,
		AC.StockID,
       SEH.Event_DateTime,
       SEH.Salvage_Event_Code,
       SEH.Towbill_Number
FROM BI_ASAP_REP.dbo.Salvage_Event_History SEH WITH (NOLOCK)
INNER JOIN #All_Changes AC
ON SEH.Salvage_ID = AC.Salvage_ID 
WHERE Salvage_Event_Code IN ('DROP')
AND ISNULL(SEH.Towbill_Number,0) > 0
                                                            
UPDATE SEH_Changes SET Towbill_Number = MaxTowbillNumber
FROM #SEH_Changes SEH_Changes INNER JOIN 
(SELECT Salvage_ID, MAX(Towbill_Number) as MaxTowbillNumber FROM  #SEH_Changes WHERE Towbill_Number is not null GROUP BY Salvage_ID) MTB
ON SEH_Changes.Salvage_ID = MTB.Salvage_ID
WHERE Towbill_Number is null

UPDATE SEH_Changes SET Towbill_Number = MaxTowbillNumber
FROM #SEH_Changes SEH_Changes INNER JOIN 
(SELECT StockID, MAX(TowbillNumber) as MaxTowbillNumber FROM dbo.FactStockTowCycleTime WITH (NOLOCK)  WHERE TowbillNumber is not null GROUP BY StockID) MTB
ON SEH_Changes.StockID = MTB.StockID
WHERE Towbill_Number is null

INSERT INTO #SEH_Changes(Salvage_ID,
			StockID,
             Event_DateTime,
             Salvage_Event_Code,
             Towbill_Number)
Select SEH_Changes.Salvage_ID, SEH_Changes.StockID,SEH_Changes.Event_DateTime,SEH_Changes.Salvage_Event_Code,TowBillNumber
FROM #SEH_Changes SEH_Changes LEFT OUTER JOIN 
(SELECT StockID, FSTCT.TowbillNumber FROM dbo.FactStockTowCycleTime FSTCT WITH (NOLOCK) WHERE FSTCT.TowbillNumber is not null) MTB
ON SEH_Changes.StockID = MTB.StockID --AND SEH_Changes.Towbill_Number <> MTB.TowbillNumber
Where SEH_Changes.Towbill_Number <> TowBillNumber
AND Salvage_Event_Code IN ('RLPD', 'ASGN','SAL4')
order by 2,3

--Select * FROM #SEH_Changes

INSERT INTO #CycleTimeDates
            (StockID,
             TowbillNumber,
             DispatchPermanentlyCancelledDatetime,
             DispatchCancelledDatetime,
             DryRunDatetime,
             ReassignAdministrativeBranchDatetime,
             BuyerTowDispatchCompletedDatetime,
             SalvageDroppedOffDatetime,
             DispatchTowerLoggedInDatetime,
             DispatchTowerAssignedDatetime,
             DispatchTowerLoggedOutDatetime,
             DispatchTowerDatetime,
             AssignmentDatetime,
             ActualReleaseDatetime,
             SoldDatetime,
             BuyerTowRequestDatetime,
             InterbranchTransferDatetime,
             OffsitePickupDatetime,
             OffsiteTowDatetime,
             ReturnTowDatetime)
SELECT ISNULL(StockID,0),
       ISNULL(Towbill_Number,0),
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DPPC' THEN SEHChanges.Event_Datetime
           END) AS Dispatch_Permanently_Cancelled_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DPCN' THEN SEHChanges.Event_Datetime
           END) AS Dispatch_Cancelled_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DPDR' THEN SEHChanges.Event_Datetime
           END) AS Dry_Run_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='REAB' THEN SEHChanges.Event_Datetime
           END) AS Reassign_Administrative_Branch_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='BYTC' THEN SEHChanges.Event_Datetime
           END) AS Buyer_Tow_Dispatch_Completed_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DROP' THEN SEHChanges.Event_Datetime
           END) AS Salvage_Dropped_Off_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DPLI' THEN SEHChanges.Event_Datetime
           END) AS Dispatch_Tower_Logged_In_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DPAS' THEN SEHChanges.Event_Datetime
           END) AS Dispatch_Tower_Assigned_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DPLO' THEN SEHChanges.Event_Datetime
           END) AS Dispatch_Tower_Logged_Out_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='DPDP' THEN SEHChanges.Event_Datetime
           END) AS Dispatch_Tower_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='ASGN' THEN SEHChanges.Event_Datetime
           END) AS Assignment_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='RLPD' THEN SEHChanges.Event_Datetime
           END) AS Actual_Release_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='SAL4' --Tow_Type_Code = 'BY'
           THEN SEHChanges.Event_Datetime
           END) AS Sold_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='SAL6' --Tow_Type_Code = 'BY'
           THEN SEHChanges.Event_Datetime
           END) AS Buyer_Tow_Request_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='IBTR' --Tow_Type_Code = 'IB'
           THEN SEHChanges.Event_Datetime
           END) AS Interbranch_Transfer_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='OSPU' --Tow_Type_Code = 'OP'
           THEN SEHChanges.Event_Datetime
           END) AS Offsite_Pickup_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='OSIA' --Tow_Type_Code = 'OS'
           THEN SEHChanges.Event_Datetime
           END) AS Offsite_Tow_Datetime,
       Max(CASE
             WHEN SEHChanges.Salvage_Event_Code='RTRQ' --Tow_Type_Code = 'RT'
           THEN SEHChanges.Event_Datetime
           END) AS Return_Tow_Datetime
FROM   #SEH_Changes SEHChanges
WHERE Towbill_Number IS NOT NULL
GROUP  BY StockID,
          Towbill_Number
 
 
  ---Tower Kiosk DropOffDateTime
  
 INSERT INTO #TMP_SEH
 (StockID,Towbill_Number,Event_DateTime)
  SELECT  DS.StockID
      ,[Towbill_Number]
      ,MAX([Event_DateTime]) AS Event_DateTime
      
  FROM  [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history] (@StartLSN, @EndLSN, 'all with merge') SEH
  INNER JOIN [BI_ASAP_Rep].[dbo].User_Info UI WITH (NOLOCK)
   ON SEH.Update_User_ID = UI.User_ID
  INNER JOIN DimStock DS WITH (NOLOCK)
   ON SEH.Salvage_ID = DS.ASAPSalvageID
  where  Salvage_Event_Code = 'SDRO' 
  AND   UI.USER_NAME = 'Tower_Kiosk'
  AND [__$operation]=5 
  GROUP BY  [Salvage_ID]
      ,StockID
      ,[Towbill_Number] 
/**************************************************************************************************************/          
/****************************Get Initiation and Completion Dates***********************************************/          
/**************************************************************************************************************/          

INSERT INTO #INIT(Init_DateTime,Compl_DateTime,Salvage_ID,Towbill_Number, StockID)
SELECT MAX(Event_DateTime) Init_DateTime, COMPL.Compl_DateTime, COMPL.Salvage_ID, COMPL.Towbill_Number, StockID
FROM BI_ASAP_Rep.dbo.Salvage_Event_History SEH WITH (NOLOCK)
INNER JOIN 
		(SELECT MAX(Event_DateTime) Compl_DateTime, Salvage_ID, Towbill_Number, StockID
			FROM #SEH_Changes
			WHERE Salvage_Event_Code IN ('DROP') 
			AND Event_DateTime > '12/1/2010'
			GROUP BY Salvage_ID, Towbill_Number, StockID) COMPL
	ON SEH.Salvage_ID = COMPL.Salvage_ID
	AND SEH.Event_DateTime <= COMPL.Compl_DateTime
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Tow ST WITH (NOLOCK)
	ON COMPL.Towbill_Number = ST.Towbill_Number AND COMPL.Salvage_ID = ST.Salvage_ID
INNER JOIN #TowMapping TM
ON ST.Tow_Type_Code = TM.TowTypeCode AND SEH.Salvage_Event_Code = TM.SalvageEventCode
GROUP BY COMPL.Compl_DateTime, COMPL.Salvage_ID, COMPL.Towbill_Number, StockID

--SELECT * FROM #INIT

INSERT INTO #INITBY(Init_DateTime,Compl_DateTime,Salvage_ID,Towbill_Number, StockID)
SELECT MAX(Event_DateTime) Init_DateTime, COMPL.Compl_DateTime, COMPL.Salvage_ID, COMPL.Towbill_Number, StockID
FROM BI_ASAP_Rep.dbo.Salvage_Event_History SEH WITH (NOLOCK)
INNER JOIN 
		(SELECT MAX(Event_DateTime) Compl_DateTime, Salvage_ID, Towbill_Number, StockID
			FROM #SEH_Changes
			WHERE Salvage_Event_Code IN ('DPLI') 
			AND Event_DateTime > '12/1/2010'
			GROUP BY Salvage_ID, Towbill_Number, StockID) COMPL
	ON SEH.Salvage_ID = COMPL.Salvage_ID
	AND SEH.Event_DateTime < COMPL.Compl_DateTime
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Tow ST
	ON COMPL.Towbill_Number = ST.Towbill_Number AND COMPL.Salvage_ID = ST.Salvage_ID
INNER JOIN #TowMappingBY TM
ON ST.Tow_Type_Code = TM.TowTypeCode AND SEH.Salvage_Event_Code = TM.SalvageEventCode
GROUP BY COMPL.Compl_DateTime, COMPL.Salvage_ID, COMPL.Towbill_Number, StockID

--SELECT * FROM #INITBY

UPDATE  #CycleTimeDates
	SET [TowInitiatedDateTime] = Init_DateTime, [TowCompletedDateTime] = Compl_DateTime
FROM #CycleTimeDates CTD WITH (NOLOCK)
INNER JOIN #INIT INI WITH (NOLOCK)
	ON CTD.StockID = INI.StockID AND CTD.TowBillNumber = INI.Towbill_Number
	
UPDATE  #CycleTimeDates
	SET [TowInitiatedDateTime] = Init_DateTime, [TowCompletedDateTime] = Compl_DateTime
FROM #CycleTimeDates CTD WITH (NOLOCK)
INNER JOIN #INITBY INI WITH (NOLOCK)
	ON CTD.StockID = INI.StockID AND CTD.TowBillNumber = INI.Towbill_Number

UPDATE  #CycleTimeDates
	SET [TowCompletedDateTime] = DryRunDatetime
FROM #CycleTimeDates CTD WITH (NOLOCK)
WHERE ISNULL(DryRunDatetime,'12/31/2999') < '12/31/2999'
 
 UPDATE  CTD
	SET [TowCompletedDateTime] = ST.Update_Datetime
FROM #CycleTimeDates CTD WITH (NOLOCK)
INNER JOIN BI_ASAP_REP.dbo.Salvage_Tow ST WITH (NOLOCK)
ON CTD.TowbillNumber = ST.Towbill_Number
WHERE ISNULL(TowCompletedDateTime,'12/31/2999') = '12/31/2999'

 --Tower Kiosk DropOffDateTime
 UPDATE CTD
 SET DropOffDateTime = CASE WHEN Isnull(TS.Event_DateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(TS.Event_DateTime , '2999-12-31') ELSE '2999-12-31' END,
  DropOffDayID = CASE WHEN Isnull(TS.Event_DateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), TS.Event_DateTime , 112)), 29991231) ELSE 29991231 END
  FROM #CycleTimeDates CTD WITH(NOLOCK)
 INNER JOIN #TMP_SEH TS ON CTD.StockID = TS.StockID and CTD.TowBillNumber = TS.Towbill_Number

 
--Update DayIDs for Datetimes
UPDATE #CycleTimeDates
SET    
--DispatchPermanentlyCancelledDayID=Cast(CONVERT(CHAR(8), DispatchPermanentlyCancelledDatetime, 112) AS INT),
--       DispatchCancelledDayID=Cast(CONVERT(CHAR(8), DispatchCancelledDatetime, 112) AS INT),
--       DryRunDayID=Cast(CONVERT(CHAR(8), DryRunDatetime, 112) AS INT),
--       ReassignAdministrativeBranchDayID=Cast(CONVERT(CHAR(8), ReassignAdministrativeBranchDatetime, 112) AS INT),
--       BuyerTowDispatchCompletedDayID=Cast(CONVERT(CHAR(8), BuyerTowDispatchCompletedDatetime, 112) AS INT),
--       SalvageDroppedOffDayID=Cast(CONVERT(CHAR(8), SalvageDroppedOffDatetime, 112) AS INT),
--       DispatchTowerLoggedInDayID=Cast(CONVERT(CHAR(8), DispatchTowerLoggedInDatetime, 112) AS INT),
--       DispatchTowerAssignedDayID=Cast(CONVERT(CHAR(8), DispatchTowerAssignedDatetime, 112) AS INT),
--       DispatchTowerLoggedOutDayID=Cast(CONVERT(CHAR(8), DispatchTowerLoggedOutDatetime, 112) AS INT),
--       DispatchTowerDayID=Cast(CONVERT(CHAR(8), DispatchTowerDatetime, 112) AS INT),
--       AssignmentDayID=Cast(CONVERT(CHAR(8), AssignmentDatetime, 112) AS INT),
--       ActualReleaseDayID=Cast(CONVERT(CHAR(8), ActualReleaseDatetime, 112) AS INT),
--       SoldDayID=Cast(CONVERT(CHAR(8), SoldDatetime, 112) AS INT),
--       BuyerTowRequestDayID=Cast(CONVERT(CHAR(8), BuyerTowRequestDatetime, 112) AS INT),
--       InterbranchTransferDayID=Cast(CONVERT(CHAR(8), InterbranchTransferDatetime, 112) AS INT),
--       OffsitePickupDayID=Cast(CONVERT(CHAR(8), OffsitePickupDatetime, 112) AS INT),
--       OffsiteTowDayID=Cast(CONVERT(CHAR(8), OffsiteTowDatetime, 112) AS INT),
--       ReturnTowDayID=Cast(CONVERT(CHAR(8), ReturnTowDatetime, 112) AS INT),
--       TowInitiatedDayID = Cast(CONVERT(CHAR(8), TowInitiatedDateTime, 112) AS INT),
--       TowCompletedDayID = Cast(CONVERT(CHAR(8), TowCompletedDateTime, 112) AS INT)

ActualReleaseDayID =CASE WHEN Isnull(ActualReleaseDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), ActualReleaseDateTime , 112)), 29991231) ELSE 29991231 END,
ActualReleaseDateTime =CASE WHEN Isnull(ActualReleaseDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(ActualReleaseDateTime , '2999-12-31') ELSE '2999-12-31' END, 
AssignmentDayID =CASE WHEN Isnull(AssignmentDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), AssignmentDateTime , 112)), 29991231) ELSE 29991231 END,
AssignmentDateTime =CASE WHEN Isnull(AssignmentDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(AssignmentDateTime , '2999-12-31') ELSE '2999-12-31' END, 
BuyerTowDispatchCompletedDayID =CASE WHEN Isnull(BuyerTowDispatchCompletedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), BuyerTowDispatchCompletedDateTime , 112)), 29991231) ELSE 29991231 END,
BuyerTowDispatchCompletedDateTime =CASE WHEN Isnull(BuyerTowDispatchCompletedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(BuyerTowDispatchCompletedDateTime , '2999-12-31') ELSE '2999-12-31' END, 
BuyerTowRequestDayID =CASE WHEN Isnull(BuyerTowRequestDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), BuyerTowRequestDateTime , 112)), 29991231) ELSE 29991231 END,
BuyerTowRequestDateTime =CASE WHEN Isnull(BuyerTowRequestDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(BuyerTowRequestDateTime , '2999-12-31') ELSE '2999-12-31' END, 
DispatchCancelledDayID =CASE WHEN Isnull(DispatchCancelledDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), DispatchCancelledDateTime , 112)), 29991231) ELSE 29991231 END,
DispatchCancelledDateTime =CASE WHEN Isnull(DispatchCancelledDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(DispatchCancelledDateTime , '2999-12-31') ELSE '2999-12-31' END, 
DispatchPermanentlyCancelledDayID =CASE WHEN Isnull(DispatchPermanentlyCancelledDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), DispatchPermanentlyCancelledDateTime , 112)), 29991231) ELSE 29991231 END,
DispatchPermanentlyCancelledDateTime =CASE WHEN Isnull(DispatchPermanentlyCancelledDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(DispatchPermanentlyCancelledDateTime , '2999-12-31') ELSE '2999-12-31' END, 
DispatchTowerAssignedDayID =CASE WHEN Isnull(DispatchTowerAssignedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), DispatchTowerAssignedDateTime , 112)), 29991231) ELSE 29991231 END,
DispatchTowerAssignedDateTime =CASE WHEN Isnull(DispatchTowerAssignedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(DispatchTowerAssignedDateTime , '2999-12-31') ELSE '2999-12-31' END, 
DispatchTowerDayID =CASE WHEN Isnull(DispatchTowerDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), DispatchTowerDateTime , 112)), 29991231) ELSE 29991231 END,
DispatchTowerDateTime =CASE WHEN Isnull(DispatchTowerDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(DispatchTowerDateTime , '2999-12-31') ELSE '2999-12-31' END, 
DispatchTowerLoggedInDayID =CASE WHEN Isnull(DispatchTowerLoggedInDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), DispatchTowerLoggedInDateTime , 112)), 29991231) ELSE 29991231 END,
DispatchTowerLoggedInDateTime =CASE WHEN Isnull(DispatchTowerLoggedInDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(DispatchTowerLoggedInDateTime , '2999-12-31') ELSE '2999-12-31' END, 
DispatchTowerLoggedOutDayID =CASE WHEN Isnull(DispatchTowerLoggedOutDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), DispatchTowerLoggedOutDateTime , 112)), 29991231) ELSE 29991231 END,
DispatchTowerLoggedOutDateTime =CASE WHEN Isnull(DispatchTowerLoggedOutDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(DispatchTowerLoggedOutDateTime , '2999-12-31') ELSE '2999-12-31' END, 
DryRunDayID =CASE WHEN Isnull(DryRunDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), DryRunDateTime , 112)), 29991231) ELSE 29991231 END,
DryRunDateTime =CASE WHEN Isnull(DryRunDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(DryRunDateTime , '2999-12-31') ELSE '2999-12-31' END, 
InterbranchTransferDayID =CASE WHEN Isnull(InterbranchTransferDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), InterbranchTransferDateTime , 112)), 29991231) ELSE 29991231 END,
InterbranchTransferDateTime =CASE WHEN Isnull(InterbranchTransferDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(InterbranchTransferDateTime , '2999-12-31') ELSE '2999-12-31' END, 
OffsitePickupDayID =CASE WHEN Isnull(OffsitePickupDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), OffsitePickupDateTime , 112)), 29991231) ELSE 29991231 END,
OffsitePickupDateTime =CASE WHEN Isnull(OffsitePickupDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(OffsitePickupDateTime , '2999-12-31') ELSE '2999-12-31' END, 
OffsiteTowDayID =CASE WHEN Isnull(OffsiteTowDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), OffsiteTowDateTime , 112)), 29991231) ELSE 29991231 END,
OffsiteTowDateTime =CASE WHEN Isnull(OffsiteTowDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(OffsiteTowDateTime , '2999-12-31') ELSE '2999-12-31' END, 
ReassignAdministrativeBranchDayID =CASE WHEN Isnull(ReassignAdministrativeBranchDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), ReassignAdministrativeBranchDateTime , 112)), 29991231) ELSE 29991231 END,
ReassignAdministrativeBranchDateTime =CASE WHEN Isnull(ReassignAdministrativeBranchDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(ReassignAdministrativeBranchDateTime , '2999-12-31') ELSE '2999-12-31' END, 
ReturnTowDayID =CASE WHEN Isnull(ReturnTowDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), ReturnTowDateTime , 112)), 29991231) ELSE 29991231 END,
ReturnTowDateTime =CASE WHEN Isnull(ReturnTowDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(ReturnTowDateTime , '2999-12-31') ELSE '2999-12-31' END, 
SalvageDroppedOffDayID =CASE WHEN Isnull(SalvageDroppedOffDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), SalvageDroppedOffDateTime , 112)), 29991231) ELSE 29991231 END,
SalvageDroppedOffDateTime =CASE WHEN Isnull(SalvageDroppedOffDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(SalvageDroppedOffDateTime , '2999-12-31') ELSE '2999-12-31' END, 
SoldDayID =CASE WHEN Isnull(SoldDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), SoldDateTime , 112)), 29991231) ELSE 29991231 END,
SoldDateTime =CASE WHEN Isnull(SoldDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(SoldDateTime , '2999-12-31') ELSE '2999-12-31' END, 
TowCompletedDayID =CASE WHEN Isnull(TowCompletedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), TowCompletedDateTime , 112)), 29991231) ELSE 29991231 END,
TowCompletedDateTime =CASE WHEN Isnull(TowCompletedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(TowCompletedDateTime , '2999-12-31') ELSE '2999-12-31' END, 
TowInitiatedDayID =CASE WHEN Isnull(TowInitiatedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), TowInitiatedDateTime , 112)), 29991231) ELSE 29991231 END,
TowInitiatedDateTime =CASE WHEN Isnull(TowInitiatedDateTime , '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(TowInitiatedDateTime , '2999-12-31') ELSE '2999-12-31' END


--SELECT * FROM   #CycleTimeDates 

--/*********************************************************************************************
--MERGE the data from the temp table into the EDW DimStorageLocation table
--This does Update and Insert only.  No Deletes
--*********************************************************************************************/


BEGIN TRANSACTION    

MERGE dbo.FactStockTowCycleTime AS DST
USING #CycleTimeDates AS SRC
         ON SRC.StockID = DST.StockID AND SRC.TowBillNumber = DST.TowBillNumber
WHEN MATCHED THEN
UPDATE SET 
	DST.ActualReleaseDayID = COALESCE(SRC.ActualReleaseDayID,DST.ActualReleaseDayID),
	DST.ActualReleaseDateTime = COALESCE(SRC.ActualReleaseDateTime,DST.ActualReleaseDateTime),
	DST.AssignmentDayID = COALESCE(SRC.AssignmentDayID,DST.AssignmentDayID),
	DST.AssignmentDateTime = COALESCE(SRC.AssignmentDateTime,DST.AssignmentDateTime),
	DST.BuyerTowDispatchCompletedDayID = COALESCE(SRC.BuyerTowDispatchCompletedDayID,DST.BuyerTowDispatchCompletedDayID),
	DST.BuyerTowDispatchCompletedDateTime = COALESCE(SRC.BuyerTowDispatchCompletedDateTime,DST.BuyerTowDispatchCompletedDateTime),
	DST.BuyerTowRequestDayID = COALESCE(SRC.BuyerTowRequestDayID,DST.BuyerTowRequestDayID),
	DST.BuyerTowRequestDateTime = COALESCE(SRC.BuyerTowRequestDateTime,DST.BuyerTowRequestDateTime),
	DST.DispatchCancelledDayID = COALESCE(SRC.DispatchCancelledDayID,DST.DispatchCancelledDayID),
	DST.DispatchCancelledDateTime = COALESCE(SRC.DispatchCancelledDateTime,DST.DispatchCancelledDateTime),
	DST.DispatchPermanentlyCancelledDayID = COALESCE(SRC.DispatchPermanentlyCancelledDayID,DST.DispatchPermanentlyCancelledDayID),
	DST.DispatchPermanentlyCancelledDateTime = COALESCE(SRC.DispatchPermanentlyCancelledDateTime,DST.DispatchPermanentlyCancelledDateTime),
	DST.DispatchTowerAssignedDayID = COALESCE(SRC.DispatchTowerAssignedDayID,DST.DispatchTowerAssignedDayID),
	DST.DispatchTowerAssignedDateTime = COALESCE(SRC.DispatchTowerAssignedDateTime,DST.DispatchTowerAssignedDateTime),
	DST.DispatchTowerDayID = COALESCE(SRC.DispatchTowerDayID,DST.DispatchTowerDayID),
	DST.DispatchTowerDateTime = COALESCE(SRC.DispatchTowerDateTime,DST.DispatchTowerDateTime),
	DST.DispatchTowerLoggedInDayID = COALESCE(SRC.DispatchTowerLoggedInDayID,DST.DispatchTowerLoggedInDayID),
	DST.DispatchTowerLoggedInDateTime = COALESCE(SRC.DispatchTowerLoggedInDateTime,DST.DispatchTowerLoggedInDateTime),
	DST.DispatchTowerLoggedOutDayID = COALESCE(SRC.DispatchTowerLoggedOutDayID,DST.DispatchTowerLoggedOutDayID),
	DST.DispatchTowerLoggedOutDateTime = COALESCE(SRC.DispatchTowerLoggedOutDateTime,DST.DispatchTowerLoggedOutDateTime),
	DST.DryRunDayID = COALESCE(SRC.DryRunDayID,DST.DryRunDayID),
	DST.DryRunDateTime = COALESCE(SRC.DryRunDateTime,DST.DryRunDateTime),
	DST.InterbranchTransferDayID = COALESCE(SRC.InterbranchTransferDayID,DST.InterbranchTransferDayID),
	DST.InterbranchTransferDateTime = COALESCE(SRC.InterbranchTransferDateTime,DST.InterbranchTransferDateTime),
	DST.OffsitePickupDayID = COALESCE(SRC.OffsitePickupDayID,DST.OffsitePickupDayID),
	DST.OffsitePickupDateTime = COALESCE(SRC.OffsitePickupDateTime,DST.OffsitePickupDateTime),
	DST.OffsiteTowDayID = COALESCE(SRC.OffsiteTowDayID,DST.OffsiteTowDayID),
	DST.OffsiteTowDateTime = COALESCE(SRC.OffsiteTowDateTime,DST.OffsiteTowDateTime),
	DST.ReassignAdministrativeBranchDayID = COALESCE(SRC.ReassignAdministrativeBranchDayID,DST.ReassignAdministrativeBranchDayID),
	DST.ReassignAdministrativeBranchDateTime = COALESCE(SRC.ReassignAdministrativeBranchDateTime,DST.ReassignAdministrativeBranchDateTime),
	DST.ReturnTowDayID = COALESCE(SRC.ReturnTowDayID,DST.ReturnTowDayID),
	DST.ReturnTowDateTime = COALESCE(SRC.ReturnTowDateTime,DST.ReturnTowDateTime),
	DST.SalvageDroppedOffDayID = COALESCE(SRC.SalvageDroppedOffDayID,DST.SalvageDroppedOffDayID),
	DST.SalvageDroppedOffDateTime = COALESCE(SRC.SalvageDroppedOffDateTime,DST.SalvageDroppedOffDateTime),
	DST.SoldDayID = COALESCE(SRC.SoldDayID,DST.SoldDayID),
	DST.SoldDateTime = COALESCE(SRC.SoldDateTime,DST.SoldDateTime),
	DST.TowCompletedDayID = COALESCE(SRC.TowCompletedDayID,DST.TowCompletedDayID),
	DST.TowCompletedDateTime = COALESCE(SRC.TowCompletedDateTime,DST.TowCompletedDateTime),
	DST.TowInitiatedDayID = COALESCE(SRC.TowInitiatedDayID,DST.TowInitiatedDayID),
	DST.TowInitiatedDateTime = COALESCE(SRC.TowInitiatedDateTime,DST.TowInitiatedDateTime),
	DST.DropOffDayID = COALESCE(SRC.DropOffDayID,DST.DropOffDayID),
	DST.DropOffDateTime = COALESCE(SRC.DropOffDateTime,DST.DropOffDateTime),
	--[ETLLoadID]=0,
	[ETLLoadUpdateID]=@ETLLoadID
WHEN NOT MATCHED THEN
INSERT (StockID,
		TowBillNumber,
		ActualReleaseDayID,
		ActualReleaseDateTime,
		AssignmentDayID,
		AssignmentDateTime,
		BuyerTowDispatchCompletedDayID,
		BuyerTowDispatchCompletedDateTime,
		BuyerTowRequestDayID,
		BuyerTowRequestDateTime,
		DispatchCancelledDayID,
		DispatchCancelledDateTime,
		DispatchPermanentlyCancelledDayID,
		DispatchPermanentlyCancelledDateTime,
		DispatchTowerAssignedDayID,
		DispatchTowerAssignedDateTime,
		DispatchTowerDayID,
		DispatchTowerDateTime,
		DispatchTowerLoggedInDayID,
		DispatchTowerLoggedInDateTime,
		DispatchTowerLoggedOutDayID,
		DispatchTowerLoggedOutDateTime,
		DryRunDayID,
		DryRunDateTime,
		InterbranchTransferDayID,
		InterbranchTransferDateTime,
		OffsitePickupDayID,
		OffsitePickupDateTime,
		OffsiteTowDayID,
		OffsiteTowDateTime,
		ReassignAdministrativeBranchDayID,
		ReassignAdministrativeBranchDateTime,
		ReturnTowDayID,
		ReturnTowDateTime,
		SalvageDroppedOffDayID,
		SalvageDroppedOffDateTime,
		SoldDayID,
		SoldDateTime,
		TowCompletedDayID,
		TowCompletedDateTime,
		TowInitiatedDayID,
		TowInitiatedDateTime,
		DropOffDayID,
		DropOffDateTime,
		ETLLoadID,
		ETLLoadUpdateID)
      VALUES (ISNULL(StockID,0),
				ISNULL(TowBillNumber,0),
				ISNULL(SRC.ActualReleaseDayID,29991231),
				ISNULL(SRC.ActualReleaseDateTime,'12/31/2999'),
				ISNULL(SRC.AssignmentDayID,29991231),
				ISNULL(SRC.AssignmentDateTime,'12/31/2999'),
				ISNULL(SRC.BuyerTowDispatchCompletedDayID,29991231),
				ISNULL(SRC.BuyerTowDispatchCompletedDateTime,'12/31/2999'),
				ISNULL(SRC.BuyerTowRequestDayID,29991231),
				ISNULL(SRC.BuyerTowRequestDateTime,'12/31/2999'),
				ISNULL(SRC.DispatchCancelledDayID,29991231),
				ISNULL(SRC.DispatchCancelledDateTime,'12/31/2999'),
				ISNULL(SRC.DispatchPermanentlyCancelledDayID,29991231),
				ISNULL(SRC.DispatchPermanentlyCancelledDateTime,'12/31/2999'),
				ISNULL(SRC.DispatchTowerAssignedDayID,29991231),
				ISNULL(SRC.DispatchTowerAssignedDateTime,'12/31/2999'),
				ISNULL(SRC.DispatchTowerDayID,29991231),
				ISNULL(SRC.DispatchTowerDateTime,'12/31/2999'),
				ISNULL(SRC.DispatchTowerLoggedInDayID,29991231),
				ISNULL(SRC.DispatchTowerLoggedInDateTime,'12/31/2999'),
				ISNULL(SRC.DispatchTowerLoggedOutDayID,29991231),
				ISNULL(SRC.DispatchTowerLoggedOutDateTime,'12/31/2999'),
				ISNULL(SRC.DryRunDayID,29991231),
				ISNULL(SRC.DryRunDateTime,'12/31/2999'),
				ISNULL(SRC.InterbranchTransferDayID,29991231),
				ISNULL(SRC.InterbranchTransferDateTime,'12/31/2999'),
				ISNULL(SRC.OffsitePickupDayID,29991231),
				ISNULL(SRC.OffsitePickupDateTime,'12/31/2999'),
				ISNULL(SRC.OffsiteTowDayID,29991231),
				ISNULL(SRC.OffsiteTowDateTime,'12/31/2999'),
				ISNULL(SRC.ReassignAdministrativeBranchDayID,29991231),
				ISNULL(SRC.ReassignAdministrativeBranchDateTime,'12/31/2999'),
				ISNULL(SRC.ReturnTowDayID,29991231),
				ISNULL(SRC.ReturnTowDateTime,'12/31/2999'),
				ISNULL(SRC.SalvageDroppedOffDayID,29991231),
				ISNULL(SRC.SalvageDroppedOffDateTime,'12/31/2999'),
				ISNULL(SRC.SoldDayID,29991231),
				ISNULL(SRC.SoldDateTime,'12/31/2999'),
				ISNULL(SRC.TowCompletedDayID,29991231),
				ISNULL(SRC.TowCompletedDateTime,'12/31/2999'),
				ISNULL(SRC.TowInitiatedDayID,29991231),
				ISNULL(SRC.TowInitiatedDateTime,'12/31/2999'),
				ISNULL(SRC.DropOffDayID,29991231),
				ISNULL(SRC.DropOffDateTime,'12/31/2999'),
				@ETLLoadID,@ETLLoadID)
   OUTPUT $action INTO #recordCount;

--/*********************************************************************************************
--Get the counts from the MERGE operation
--*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #recordCount WITH (NOLOCK)

/*********************************************************************************************
Merge changes to TowDays and TravelTimeMinutes
*********************************************************************************************/
MERGE dbo.FactStockTowCycleTime AS DST
USING #FSTCT_Changes AS SRC
         ON SRC.StockID = DST.StockID AND SRC.TowBill_Number = DST.TowBillNumber
WHEN MATCHED THEN
UPDATE SET 
	TowDays = Tow_Days,
	TravelTimeMinutes = Travel_Time_Minutes,
	[ETLLoadUpdateID]=@ETLLoadID
WHEN NOT MATCHED THEN
INSERT (StockID,
		TowBillNumber,
		TowDays,
		TravelTimeMinutes,
		ETLLoadID,
		ETLLoadUpdateID)
	VALUES (ISNULL(StockID,0),
			ISNULL(TowBill_Number,0),
            ISNULL(Tow_Days,0),
            ISNULL(Travel_Time_Minutes,0),
			@ETLLoadID,@ETLLoadID);

/*********************************************************************************************
Calculate Durations
*********************************************************************************************/
--DECLARE @ETLLoadID int
--SELECT @ETLLoadID = MAX(ETLLoadUpdateID) FROM dbo.FactStockTowCycleTime

--SELECT * FROM dbo.FactStockTowCycleTime WHERE ETLLoadUpdateID = (SELECT MAX(ETLLoadUpdateID) FROM dbo.FactStockTowCycleTime) --@ETLLoadID

DELETE FROM #CycleTimeDates

INSERT INTO #CycleTimeDates(StockID,
	TowBillNumber,
	ActualReleaseDayID,
	ActualReleaseDateTime,
	AssignmentDayID,
	AssignmentDateTime,
	BuyerTowDispatchCompletedDayID,
	BuyerTowDispatchCompletedDateTime,
	BuyerTowRequestDayID,
	BuyerTowRequestDateTime,
	DispatchCancelledDayID,
	DispatchCancelledDateTime,
	DispatchPermanentlyCancelledDayID,
	DispatchPermanentlyCancelledDateTime,
	DispatchTowerAssignedDayID,
	DispatchTowerAssignedDateTime,
	DispatchTowerDayID,
	DispatchTowerDateTime,
	DispatchTowerLoggedInDayID,
	DispatchTowerLoggedInDateTime,
	DispatchTowerLoggedOutDayID,
	DispatchTowerLoggedOutDateTime,
	DryRunDayID,
	DryRunDateTime,
	DryRunEventDayID,
	DryRunEventDateTime,
	InterbranchTransferDayID,
	InterbranchTransferDateTime,
	OffsitePickupDayID,
	OffsitePickupDateTime,
	OffsiteTowDayID,
	OffsiteTowDateTime,
	ReassignAdministrativeBranchDayID,
	ReassignAdministrativeBranchDateTime,
	ReturnTowDayID,
	ReturnTowDateTime,
	SalvageDroppedOffDayID,
	SalvageDroppedOffDateTime,
	SoldDayID,
	SoldDateTime,
	TowCompletedDayID,
	TowCompletedDateTime,
	TowInitiatedDayID,
	TowInitiatedDateTime,
	BranchNumber,
	TowTypeCode,
	DropOffDayID,
	DropOffDateTime )
SELECT FSTCT.StockID,
	FSTCT.TowBillNumber,
	ActualReleaseDayID,
	ActualReleaseDateTime,
	AssignmentDayID,
	AssignmentDateTime,
	BuyerTowDispatchCompletedDayID,
	BuyerTowDispatchCompletedDateTime,
	BuyerTowRequestDayID,
	BuyerTowRequestDateTime,
	DispatchCancelledDayID,
	DispatchCancelledDateTime,
	DispatchPermanentlyCancelledDayID,
	DispatchPermanentlyCancelledDateTime,
	DispatchTowerAssignedDayID,
	DispatchTowerAssignedDateTime,
	DispatchTowerDayID,
	DispatchTowerDateTime,
	DispatchTowerLoggedInDayID,
	DispatchTowerLoggedInDateTime,
	DispatchTowerLoggedOutDayID,
	DispatchTowerLoggedOutDateTime,
	DryRunDayID,
	DryRunDateTime,
	DryRunEventDayID,
	DryRunEventDateTime,
	InterbranchTransferDayID,
	InterbranchTransferDateTime,
	OffsitePickupDayID,
	OffsitePickupDateTime,
	OffsiteTowDayID,
	OffsiteTowDateTime,
	ReassignAdministrativeBranchDayID,
	ReassignAdministrativeBranchDateTime,
	ReturnTowDayID,
	ReturnTowDateTime,
	SalvageDroppedOffDayID,
	SalvageDroppedOffDateTime,
	SoldDayID,
	SoldDateTime,
	TowCompletedDayID,
	TowCompletedDateTime,
	TowInitiatedDayID,
	TowInitiatedDateTime,
	0 as BranchNumber,
	'' as TowTypeCode,
	DropOffDayID,
	DropOffDateTime 
FROM dbo.FactStockTowCycleTime FSTCT WITH (NOLOCK)
--INNER JOIN dbo.FactStockTow FST WITH (NOLOCK)
--ON FSTCT.StockID = FST.StockID and FSTCT.TowBillNumber = FST.TowBillNumber 
WHERE FSTCT.ETLLoadUpdateID = @ETLLoadID


---Changed the BranchNumber to AdministrativeBranchNumber

UPDATE #CycleTimeDates
SET TowTypeCode = Tow_Type_Code, BranchNumber = S.AdministrativeBranchNumber
FROM #CycleTimeDates CTD 
INNER JOIN dbo.DimStock S WITH (NOLOCK)
ON CTD.StockID = S.StockID  
INNER JOIN BI_ASAP_REP.dbo.Salvage_Tow ST WITH (NOLOCK)
ON S.ASAPSalvageID = ST.Salvage_ID and CTD.TowbillNumber = ST.Towbill_Number 


UPDATE #CycleTimeDates 
SET
AssignToPickupBusinessHours=0,
DispatchToLoginBusiness=CASE WHEN DispatchTowerDayID <> 29991231 AND DispatchTowerLoggedInDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours(DispatchTowerDatetime, DispatchTowerLoggedInDatetime, BranchNumber)/9.0
      ELSE 0
      END,
DispatchToLoginCalendar=CASE WHEN DispatchTowerDayID <> 29991231 AND DispatchTowerLoggedInDayId <> 29991231 AND DispatchTowerLoggedInDatetime>DispatchTowerDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerLoggedInDatetime- DispatchTowerDatetime)
      ELSE 0
      END,
DispatchToPickupBusiness=CASE WHEN DispatchTowerDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND SalvageDroppedOffDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours(DispatchTowerDatetime, SalvageDroppedOffDatetime, BranchNumber)/9.0
       WHEN DispatchTowerDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours(DispatchTowerDatetime, DispatchTowerLoggedInDatetime, BranchNumber)/9.0
      ELSE 0   END,
DispatchToPickupCalendar=CASE WHEN DispatchTowerDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND SalvageDroppedOffDayId <> 29991231 AND SalvageDroppedOffDatetime>DispatchTowerDatetime
        THEN CONVERT(Decimal(18,9),SalvageDroppedOffDatetime- DispatchTowerDatetime)
       WHEN DispatchTowerDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231 AND DispatchTowerLoggedInDatetime>DispatchTowerDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerLoggedInDatetime- DispatchTowerDatetime)
      ELSE 0
      END,
PickupEnrouteToPickupBusiness=CASE WHEN DispatchTowerLoggedOutDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND SalvageDroppedOffDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours(DispatchTowerLoggedOutDatetime, SalvageDroppedOffDatetime, BranchNumber)/9.0
       WHEN DispatchTowerLoggedOutDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours(DispatchTowerLoggedOutDatetime, DispatchTowerLoggedInDatetime, BranchNumber)/9.0
      ELSE 0
      END,
PickupEnrouteToPickupCalendar=CASE WHEN DispatchTowerLoggedOutDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND SalvageDroppedOffDayId <> 29991231 AND SalvageDroppedOffDatetime>DispatchTowerLoggedOutDatetime
        THEN CONVERT(Decimal(18,9),SalvageDroppedOffDatetime- DispatchTowerLoggedOutDatetime)
       WHEN DispatchTowerLoggedOutDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231 AND DispatchTowerLoggedInDatetime>DispatchTowerLoggedOutDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerLoggedInDatetime- DispatchTowerLoggedOutDatetime)
      ELSE 0
      END,
PickupToLoginBusiness=CASE WHEN SalvageDroppedOffDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours(SalvageDroppedOffDatetime, DispatchTowerLoggedInDatetime, BranchNumber)/9.0
       WHEN DispatchTowerLoggedInDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours(DispatchTowerLoggedInDatetime, DispatchTowerLoggedInDatetime, BranchNumber)/9.0
      ELSE 0
      END,
PickupToLoginCalendar=CASE WHEN SalvageDroppedOffDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231 AND DispatchTowerLoggedInDatetime>SalvageDroppedOffDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerLoggedInDatetime- SalvageDroppedOffDatetime)
       WHEN DispatchTowerLoggedInDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231 AND DispatchTowerLoggedInDatetime>DispatchTowerLoggedInDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerLoggedInDatetime- DispatchTowerLoggedInDatetime)
      ELSE 0
      END,
ReleaseToPickupBusiness=CASE WHEN ActualReleaseDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND SalvageDroppedOffDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours2(ActualReleaseDatetime, SalvageDroppedOffDatetime, BranchNumber)/9.0
       WHEN ActualReleaseDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours2(ActualReleaseDatetime, DispatchTowerLoggedInDatetime, BranchNumber)/9.0
      ELSE 0
      END,
ReleaseToPickupBusinessHours=CASE WHEN ActualReleaseDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND SalvageDroppedOffDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours2(ActualReleaseDatetime, SalvageDroppedOffDatetime, BranchNumber)--/9.0
       WHEN ActualReleaseDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231
        THEN dbo.fn_EDWBusinessHours2(ActualReleaseDatetime, DispatchTowerLoggedInDatetime, BranchNumber)--/9.0
      ELSE 0
      END,
ReleaseToPickupCalendar=CASE WHEN ActualReleaseDayID <> 29991231 AND TowTypeCode IN ('PK', 'OP') AND SalvageDroppedOffDayId <> 29991231 AND SalvageDroppedOffDatetime>ActualReleaseDatetime
        THEN CONVERT(Decimal(18,9),SalvageDroppedOffDatetime- ActualReleaseDatetime)
       WHEN ActualReleaseDayID <> 29991231 AND TowTypeCode NOT IN ('PK', 'OP') AND DispatchTowerLoggedInDayId <> 29991231 AND DispatchTowerLoggedInDatetime>ActualReleaseDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerLoggedInDatetime- ActualReleaseDatetime)
      ELSE 0
      END,
--ReleaseToPickupEffectiveDaysBusiness=0,
--ReleaseToPickupEffectiveDaysCalendar=0,
WaitDispatchToWaitDriverBusiness=CASE 
       WHEN TowTypeCode = 'BY' AND DispatchTowerDayID <> 29991231 AND BuyerTowRequestDayID <> 29991231 
        THEN dbo.fn_EDWBusinessHours(BuyerTowRequestDatetime, DispatchTowerDatetime, BranchNumber)/9.0
       WHEN TowTypeCode = 'IB' AND DispatchTowerDayID <> 29991231 AND InterbranchTransferDayID <> 29991231 
        THEN dbo.fn_EDWBusinessHours(InterbranchTransferDatetime, DispatchTowerDatetime, BranchNumber)/9.0
       WHEN TowTypeCode = 'OP' AND DispatchTowerDayID <> 29991231 AND OffsitePickupDayID <> 29991231 
        THEN dbo.fn_EDWBusinessHours(OffsitePickupDatetime, DispatchTowerDatetime, BranchNumber)/9.0
       WHEN TowTypeCode = 'OS' AND DispatchTowerDayID <> 29991231 AND OffsiteTowDayID <> 29991231 
        THEN dbo.fn_EDWBusinessHours(OffsiteTowDatetime, DispatchTowerDatetime, BranchNumber)/9.0
       WHEN TowTypeCode = 'PK' AND DispatchTowerDayID <> 29991231 AND ActualReleaseDayID <> 29991231 
        THEN dbo.fn_EDWBusinessHours(ActualReleaseDatetime, DispatchTowerDatetime, BranchNumber)/9.0
       WHEN TowTypeCode = 'RT' AND DispatchTowerDayID <> 29991231 AND ReturnTowDayID <> 29991231 
        THEN dbo.fn_EDWBusinessHours(ReturnTowDatetime, DispatchTowerDatetime, BranchNumber)/9.0
      ELSE 0
      END,
WaitDispatchToWaitDriverCalendar=CASE 
       WHEN TowTypeCode = 'BY' AND DispatchTowerDayID <> 29991231 AND BuyerTowRequestDayID <> 29991231 AND DispatchTowerDatetime>BuyerTowRequestDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerDatetime- BuyerTowRequestDatetime)
       WHEN TowTypeCode = 'IB' AND DispatchTowerDayID <> 29991231 AND InterbranchTransferDayID <> 29991231 AND DispatchTowerDatetime>InterbranchTransferDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerDatetime- InterbranchTransferDatetime)
       WHEN TowTypeCode = 'OP' AND DispatchTowerDayID <> 29991231 AND OffsitePickupDayID <> 29991231 AND DispatchTowerDatetime>OffsitePickupDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerDatetime- OffsitePickupDatetime)
       WHEN TowTypeCode = 'OS' AND DispatchTowerDayID <> 29991231 AND OffsiteTowDayID <> 29991231 AND DispatchTowerDatetime>OffsiteTowDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerDatetime- OffsiteTowDatetime)
       WHEN TowTypeCode = 'PK' AND DispatchTowerDayID <> 29991231 AND ActualReleaseDayID <> 29991231 AND DispatchTowerDatetime>ActualReleaseDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerDatetime- ActualReleaseDatetime)
       WHEN TowTypeCode = 'RT' AND DispatchTowerDayID <> 29991231 AND ReturnTowDayID <> 29991231 AND DispatchTowerDatetime>ReturnTowDatetime
        THEN CONVERT(Decimal(18,9),DispatchTowerDatetime- ReturnTowDatetime)
      ELSE 0
      END,
WaitDriverToPickupEnrouteBusiness=CASE WHEN AssignmentDayID <> 29991231 AND ActualReleaseDayId <> 29991231
       THEN dbo.fn_EDWBusinessHours(AssignmentDatetime, ActualReleaseDatetime, BranchNumber)/9.0
      ELSE 0
      END,
WaitDriverToPickupEnrouteCalendar=CASE WHEN AssignmentDayID <> 29991231 --AND TowTypeCode = 'PK' 
		AND ActualReleaseDayID <> 29991231 AND ActualReleaseDatetime>AssignmentDatetime
        THEN CONVERT(Decimal(18,9),ActualReleaseDatetime- AssignmentDatetime)
      ELSE 0
      END,      
--WaitDriverToPickupEnrouteCalendar=CASE WHEN DispatchTowerDayID <> 29991231 AND DispatchTowerLoggedOutDayId <> 29991231 AND DispatchTowerLoggedOutDatetime>DispatchTowerDatetime
--       THEN CONVERT(Decimal(18,9),DispatchTowerLoggedOutDatetime- DispatchTowerDatetime)
--      ELSE 0
--      END,
WaitReleaseToWaitDispatchBusiness=CASE WHEN ActualReleaseDayID <> 29991231 AND DispatchTowerDayID <> 29991231
        THEN dbo.fn_EDWBusinessHours(ActualReleaseDatetime, DispatchTowerDatetime, BranchNumber)/9.0
      ELSE 0
      END,
--WaitReleaseToWaitDispatchCalendar=CASE WHEN AssignmentDayID <> 29991231 AND TowTypeCode = 'PK' AND ActualReleaseDayID <> 29991231 AND ActualReleaseDatetime>AssignmentDatetime
--        THEN CONVERT(Decimal(18,9),ActualReleaseDatetime- AssignmentDatetime)
--      ELSE 0
--      END,
WaitReleaseToWaitDispatchCalendar=CASE WHEN DispatchTowerDayID <> 29991231 AND ActualReleaseDayId <> 29991231 AND DispatchTowerDatetime>ActualReleaseDatetime
       THEN CONVERT(Decimal(18,9),DispatchTowerDatetime- ActualReleaseDatetime)
      ELSE 0
      END,
ReturnTowBusinessHours=CASE WHEN TowInitiatedDayID <> 29991231 AND TowTypeCode = 'RT' AND TowCompletedDayID <> 29991231
        THEN dbo.fn_EDWBusinessHours(TowInitiatedDateTime, TowCompletedDateTime, BranchNumber)
      ELSE 0
      END,
ActualReleaseTimezonedDayId = CAST(CONVERT(CHAR(8),dbo.fn_TimeZoneConvert(ActualReleaseDatetime,BranchNumber),112) AS INT),
SalvageDroppedOffTimezonedDayId = CAST(CONVERT(CHAR(8),dbo.fn_TimeZoneConvert(SalvageDroppedOffDatetime,BranchNumber),112) AS INT),
ActualReleaseTimezonedDateTime = dbo.fn_TimeZoneConvert(ActualReleaseDatetime,BranchNumber), 
SalvageDroppedOffTimezonedDatetime =  dbo.fn_TimeZoneConvert(SalvageDroppedOffDatetime,BranchNumber),
DispatchTowerLoggedInTimezonedDayId = CAST(CONVERT(CHAR(8),dbo.fn_TimeZoneConvert(DispatchTowerLoggedInDatetime,BranchNumber),112) AS INT),
DispatchTowerLoggedInTimezonedDatetime =  dbo.fn_TimeZoneConvert(DispatchTowerLoggedInDatetime,BranchNumber)

UPDATE #CycleTimeDates 
SET ReleaseToPickupEffectiveDaysBusiness = 
	CASE
	 WHEN towtypecode IN ('PK', 'OP') THEN (Cast((CASE
			WHEN ActualReleaseTimezonedDayId<29991231
				 AND SalvageDroppedOffTimezonedDayId<29991231 THEN
			dbo.fn_EffectiveDaysBusiness2 (CASE WHEN Datepart(hour,ActualReleaseTimezonedDateTime)>=15 THEN Dateadd(day,1,(CONVERT(VARCHAR(11), ActualReleaseTimezonedDateTime, 110)))
												ELSE (CONVERT(VARCHAR(11), ActualReleaseTimezonedDateTime, 110)) END,
		  CONVERT(VARCHAR(11), SalvageDroppedOffTimezonedDatetime, 110))--,BranchNumber)
			ELSE 0
		  END)AS INT))
	 WHEN towtypecode NOT IN ('PK', 'OP') THEN (Cast((CASE
			WHEN ActualReleaseTimezonedDayId<29991231
				 AND DispatchTowerLoggedInTimezonedDayId<29991231 THEN
			dbo.fn_EffectiveDaysBusiness2 (CASE
									WHEN
			Datepart (hour,
			ActualReleaseTimezonedDateTime)>=15 THEN
		  Dateadd(day, 1, (
		  CONVERT(VARCHAR(11), ActualReleaseTimezonedDateTime, 110)))
		  ELSE (
		  CONVERT(VARCHAR(11), ActualReleaseTimezonedDateTime, 110))
		  END,
		  CONVERT(VARCHAR(11), DispatchTowerLoggedInTimezonedDateTime, 110))--,BranchNumber)
			ELSE 0
		  END)AS INT))
	 ELSE 0
	END--/9
	
	--Tower Kiosk Durations
INSERT INTO #TMP_DT
(StockID, Towbill_Number, DropOffTimezonedDateTime,DropOffTimezonedDayID)
SELECT StockID,TowBillNumber 
,dbo.Fn_timezoneconvert(CT.Dropoffdatetime, BranchNumber) AS DropOffTimezonedDateTime
,CONVERT(CHAR(8), dbo.Fn_timezoneconvert(CT.Dropoffdatetime, BranchNumber), 112) AS DropOffTimezonedDayID
FROM #CycleTimeDates CT WITH(NOLOCK)


UPDATE FSCT
SET CalendarDaysReleaseToDropOff = CASE
					WHEN CD.ActualReleaseDayID<29991231
						 AND CD.DropOffDayID<29991231 THEN
					  CASE
						WHEN CD.DropOffDateTime<CD.ActualReleaseDateTime THEN 0
						ELSE Cast(CD.DropOffDateTime AS FLOAT)-Cast(CD.ActualReleaseDateTime AS FLOAT)
					  END
				  END ,
	BusinessDaysReleaseToDropOff = (CASE
					WHEN CD.ActualReleaseDayID<29991231
						 AND FSCT.DropOffDayID<29991231 THEN
					  CASE
						WHEN CD.DropOffDateTime<CD.ActualReleaseDateTime THEN 0
						ELSE [dbo].[Fn_edwbusinesshours2](CD.ActualReleaseDateTime, CD.DropOffDateTime,BranchNumber)
					  END
				  END)/9 ,
	EffectiveDaysReleaseToDropOff = Cast(CASE
						WHEN CD.ActualReleaseTimezonedDayId<29991231
							 AND DT.DropOffTimezonedDayID<29991231 THEN
						  CASE
							WHEN DropOffTimezonedDateTime<(CASE
												   WHEN Datepart(HOUR, CD.ActualReleaseTimezonedDateTime)>=15 THEN Dateadd(DAY, 1, (CONVERT(VARCHAR(11),
																														   CD.ActualReleaseTimezonedDateTime,
																														   110)))
												   ELSE (CONVERT(VARCHAR(11), CD.ActualReleaseTimezonedDateTime, 110))
												 END) THEN 0
							ELSE [dbo].[Fn_effectivedaysbusiness2](CASE
																	WHEN Datepart(HOUR, CD.ActualReleaseTimezonedDateTime)>=15 THEN Dateadd(DAY, 1, (
																														   CONVERT(VARCHAR(11),
																														   CD.ActualReleaseTimezonedDateTime
																														   , 110))
																														   )
																	ELSE (CONVERT(VARCHAR(11), CD.ActualReleaseTimezonedDateTime, 110))
																  END, CONVERT(VARCHAR(11), DropOffTimezonedDateTime, 110))
						  END
					  END AS INT)			  
				  
FROM FactStockTowCycleTime FSCT WITH (NOLOCK)
INNER JOIN #CycleTimeDates CD ON FSCT.StockID = CD.StockID AND FSCT.TowBillNumber = CD.TowBillNumber
INNER JOIN #TMP_DT DT ON CD.StockID = DT.StockID AND CD.TowBillNumber = DT.Towbill_Number

	
	

--SELECT * FROM #CycleTimeDates

UPDATE STCT SET
	AssignToPickupBusinessHours = CTD.AssignToPickupBusinessHours,
	DispatchToLoginBusiness = CTD.DispatchToLoginBusiness,
	DispatchToLoginCalendar = CTD.DispatchToLoginCalendar,
	DispatchToPickupBusiness = CTD.DispatchToPickupBusiness,
	DispatchToPickupCalendar = CTD.DispatchToPickupCalendar,
	PickupEnrouteToPickupBusiness = CTD.PickupEnrouteToPickupBusiness,
	PickupEnrouteToPickupCalendar = CTD.PickupEnrouteToPickupCalendar,
	PickupToLoginBusiness = CTD.PickupToLoginBusiness,
	PickupToLoginCalendar = CTD.PickupToLoginCalendar,
	ReleaseToPickupBusiness = CTD.ReleaseToPickupBusiness,
	ReleaseToPickupBusinessHours = CTD.ReleaseToPickupBusinessHours,
	ReleaseToPickupCalendar = CTD.ReleaseToPickupCalendar,
	ReleaseToPickupEffectiveDaysBusiness = CTD.ReleaseToPickupEffectiveDaysBusiness,
	ReleaseToPickupEffectiveDaysCalendar = CTD.ReleaseToPickupEffectiveDaysCalendar,
	WaitDispatchToWaitDriverBusiness = CTD.WaitDispatchToWaitDriverBusiness,
	WaitDispatchToWaitDriverCalendar = CTD.WaitDispatchToWaitDriverCalendar,
	WaitDriverToPickupEnrouteBusiness = CTD.WaitDriverToPickupEnrouteBusiness,
	WaitDriverToPickupEnrouteCalendar = CTD.WaitDriverToPickupEnrouteCalendar,
	WaitReleaseToWaitDispatchBusiness = CTD.WaitReleaseToWaitDispatchBusiness,
	WaitReleaseToWaitDispatchCalendar = CTD.WaitReleaseToWaitDispatchCalendar,
	ReturnTowBusinessHours = CTD.ReturnTowBusinessHours,
ActualReleaseTimezonedDayId = CTD.ActualReleaseTimezonedDayId,
SalvageDroppedOffTimezonedDayId = CTD.SalvageDroppedOffTimezonedDayId,
ActualReleaseTimezonedDateTime = CTD.ActualReleaseTimezonedDateTime, 
SalvageDroppedOffTimezonedDatetime =  CTD.SalvageDroppedOffTimezonedDatetime,
DispatchTowerLoggedInTimezonedDayId = CTD.DispatchTowerLoggedInTimezonedDayId,
DispatchTowerLoggedInTimezonedDatetime =  CTD.DispatchTowerLoggedInTimezonedDatetime

FROM dbo.FactStockTowCycleTime STCT WITH (NOLOCK)
INNER JOIN #CycleTimeDates CTD WITH (NOLOCK)
ON STCT.StockID = CTD.StockID AND STCT.TowBillNumber = CTD.TowBillNumber

--Add the updated addresses to the updated count
SET @UpdateRowCount = @UpdateRowCount + ISNULL(@@ROWCOUNT,0)

--/*********************************************************************************************
--Return the change counts
--*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		
		IF OBJECT_ID('tempdb..#All_Changes') IS NOT NULL DROP TABLE #FSTCT_Changes
		IF OBJECT_ID('tempdb..#FSTCT_Changes') IS NOT NULL DROP TABLE #FSTCT_Changes
		IF OBJECT_ID('tempdb..#SEH_Changes') IS NOT NULL DROP TABLE #SEH_Changes
		IF OBJECT_ID('tempdb..#CycleTimeDates') IS NOT NULL DROP TABLE #CycleTimeDates
		IF OBJECT_ID('tempdb..#TowMapping') IS NOT NULL DROP TABLE #TowMapping
		IF OBJECT_ID('tempdb..#TowMappingBY') IS NOT NULL DROP TABLE #TowMappingBY
		IF OBJECT_ID('tempdb..#INIT') IS NOT NULL DROP TABLE #INIT
		IF OBJECT_ID('tempdb..#INITBY') IS NOT NULL DROP TABLE #INITBY
		IF OBJECT_ID('tempdb..#recordCount') IS NOT NULL DROP TABLE #recordCount
		IF OBJECT_ID('tempdb..#TMP_SEH') IS NOT NULL DROP TABLE #TMP_SEH
		IF OBJECT_ID('tempdb..#TMP_DT') IS NOT NULL DROP TABLE #TMP_DT
 
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN

		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		
		IF OBJECT_ID('tempdb..#All_Changes') IS NOT NULL DROP TABLE #FSTCT_Changes
		IF OBJECT_ID('tempdb..#FSTCT_Changes') IS NOT NULL DROP TABLE #FSTCT_Changes
		IF OBJECT_ID('tempdb..#SEH_Changes') IS NOT NULL DROP TABLE #SEH_Changes
		IF OBJECT_ID('tempdb..#CycleTimeDates') IS NOT NULL DROP TABLE #CycleTimeDates
		IF OBJECT_ID('tempdb..#TowMapping') IS NOT NULL DROP TABLE #TowMapping
		IF OBJECT_ID('tempdb..#TowMappingBY') IS NOT NULL DROP TABLE #TowMappingBY
		IF OBJECT_ID('tempdb..#INIT') IS NOT NULL DROP TABLE #INIT
		IF OBJECT_ID('tempdb..#INITBY') IS NOT NULL DROP TABLE #INITBY
		IF OBJECT_ID('tempdb..#recordCount') IS NOT NULL DROP TABLE #recordCount
		IF OBJECT_ID('tempdb..#TMP_SEH') IS NOT NULL DROP TABLE #TMP_SEH
        IF OBJECT_ID('tempdb..#TMP_DT') IS NOT NULL DROP TABLE #TMP_DT
		RETURN -1
	END CATCH
	END
GO


