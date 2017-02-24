
/* 
USE EDW
GO 
===============================================================================================================
Author		: Juan X. Jacome
Create date	: 09/05/2011
Description	: Populate the EDW FactStockTow table from CDC	
===============================================================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
10/24/2011	Juan X. Jacome		-- Added code to drop temp Tables (DBA Code Review)
11/3/2011   Juan X. Jacome		-- Added code to calucate TowZoneID
12/2/2011   Preetham Duvva      --Added TowZoneNumber and removed TowMileageBand, TowRadiusMiles and TowZoneID 
                                  as per story 4429
12/16/2011  Juan X. Jacome		-- Added code to drop temp Tables (DBA Code Review)
9/26/2012	Matt Stelter		Changed sorce for Tower Performance
11/15/2012  Preetham Duvva      Added IsTowerKiosk
6/7/2016   Yogitha Alwarsetty   Added CheckName   
===============================================================================================================
Usage Example:
Exec usp_ETLFactStockTow
===============================================================================================================
*/

Create PROCEDURE [dbo].[usp_ETLFactStockTow]
AS     
BEGIN

DECLARE @ETLLoadID        INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
	SET NOCOUNT ON;

BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactStockTow'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'


/*********************************************************************************************
Create the temp table to consolidate #Salvage_Tow
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_Salvage_Tow') IS NOT NULL DROP TABLE #TMP_Salvage_Tow
CREATE TABLE #TMP_Salvage_Tow
(	[Salvage_ID] [int] NOT NULL,
	[Towbill_Number] [int] NOT NULL,
	[Create_DateTime] [datetime] NULL,
	[Branch_Number] [int] NULL,
	[Towin_Storage_Location_ID] [int] NULL,
	[Towout_Storage_Location_ID] [int] NULL,
	[Tower_ID] [int] NULL,
	[Tow_Zone_Number] [smallint] NULL,
	[Tow_Type_Code] [char](2) NULL,
	[Tow_Miles] [smallint] NULL,
	[Tow_Days] [smallint] NULL,
	[Tow_Hours] [smallint] NULL,
	[Tow_Priority_Due_Date] [datetime] NULL,
	[Update_User_ID] [int] NULL,
	[Tow_Successful_Ind] [bit] NOT NULL,
	[Update_DateTime] [datetime] NULL,
	[ESPS_Branch_Number] [int] NULL,
	[ESPS_Towbill_Number] [int] NULL,
	[Pickup_Required_Date] [datetime] NULL,
	[Tow_Dry_Run_Ind] [bit] NOT NULL,
	[Tow_Priority_Ind] [bit] NOT NULL,
	[IAA_Employee_ID] [int] NULL,
	[Barcode_Image_ID] [int] NULL,
	[Internal_Tower_Branch_Number] [int] NULL,
	[EFax_Status] [smallint] NOT NULL,
	[EFax_Delivery_DateTime] [datetime] NULL,
	[EFax_Request_DateTime] [datetime] NULL,
	[Fax_Handle_ID] [int] NULL,
	[Direction_Code] [char](2) NULL,
	[Travel_Time_Minutes] [smallint] NULL,
	[Probability] [int] NULL,
	[Previous_Tow_Miles] [smallint] NULL

	)
	
/*********************************************************************************************
Create the temp table to consolidate #TMP_Stock_Tow
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_Stock_Tow') IS NOT NULL DROP TABLE #TMP_Stock_Tow
CREATE TABLE #TMP_Stock_Tow(
[Salvage_ID] [int] NOT NULL,
[Buyer_ID] [int] NOT NULL,
[Branch_Number] [int] NULL,
[Direction_Code] [char](2) NULL,
[EFax_Delivery_DateTime] [datetime] NULL,
[EFax_Request_DateTime] [datetime] NULL,
[EFax_Status_Code] [smallint] NULL,
[ESPS_Branch_Number] [int] NULL,
[ESPS_Towbill_Number] [int] NULL,
[Internal_Tower_Branch_ID] [int] NULL,
[Pickup_Required_DateTime] [datetime] NULL,
[Probability] [int] NULL,
[Provider_ID] [int] NULL,
--[Stock_Tow_Info_ID] [int] NULL,
[Tow_Days] [int] NULL,
[Tow_Miles] [int] NULL,
[Tow_Type_Code] [char](2) NULL,
[Tow_Zone_ID] [int] NULL,
[Tow_Zone_Number] [smallint] NULL,
[TowBill_Number] [int] NULL,
[Tower_ID] [int] NULL,
[Towin_Storage_Location_ID] [int] NULL,
[Towout_Storage_Location_ID] [int] NULL,
[Travel_Time_Minutes] [int] NULL,
[Update_DateTime] [datetime] NULL,
[Update_User_ID] [int] NULL,
[Tow_Priority_Ind] [bit] NULL,
[Tower_Payment_Method] [char](3) NULL
)	
/*********************************************************************************************
Create the temp table to consolidate #TMP_FactStockTow
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_FactStockTow') IS NOT NULL DROP TABLE #TMP_FactStockTow
CREATE TABLE #TMP_FactStockTow
(	[StockID] [int] NOT NULL,
	[SalvageID] [int] NOT NULL,
	[TowBillNumber] [int] NOT NULL,
	[BranchNumber] [int] NULL,
	[BuyerID] [int] NULL,
	[CustomerType] [nchar](2) NULL,
	[GlobalRate] [money] NULL,
	[FirstAssignedTowerID] [int] NULL,
	[PaidTowerID] [int] NULL,
	[IsReleaseProblem] [tinyint] NOT NULL,
	[ProviderID] [int] NULL,
	[IsTowBillOutlier] [tinyint] NOT NULL,
	[IsTowCancelled] [tinyint] NOT NULL,
	[TowDays] [int] NULL,
	[IsTowDispatchReversed] [tinyint] NOT NULL,
	[IsTowDryRun] [tinyint] NOT NULL,
	[TowerBranchZoneRateID] [int] NULL,
	[IsTowerChanged] [tinyint] NOT NULL,
	[TowerID] [int] NULL,
	[TowCategory] [nvarchar](10) NULL,
	--[TowMileageBand] [nvarchar](30) NULL,
	[TowTypeCode] [nchar](2) NULL,
	[TowerPaymentMethod] [nchar](3) NULL,
	[TowerPickupPerformance] [nvarchar](10) NULL,
	[TowerRate] [money] NULL,
	[TowinStorageLocationID] [int] NULL,
	[TowMiles] [int] NULL,
	[TowoutStorageLocationID] [int] NULL,
	[IsTowPriority] [tinyint] NOT NULL,
	[IsTowSuccessful] [tinyint] NOT NULL,
	--[TowZoneID] [int] NULL,
	[TravelTimeMinutes] [int] NULL,
	[ETLLoadID] [int] NOT NULL,
	[ETLLoadUpdateID] [int] NOT NULL,
	[AdditionalFeePerMileAtTow] [money] NULL,
	[IsBuyerOverrideAtTow] [tinyint] NOT NULL,
	[DryRunAmtAtTow] [float] NULL,
	[DryRunCalculationMethodAtTow] [nchar](3) NULL,
	[FuelSurchargeAmtAtTow] [float] NULL,
	[FuelSurchargeCalculationMethodAtTow] [nchar](3) NULL,
	[PickupTimeLimitHoursAtTow] [smallint] NULL,
	[RateAmountAtTow] [money] NULL,
	[SalvageDutyTypeCodeAtTow] [nchar](3) NULL,
	[StockTypeAtTow] [nchar](5) NULL,
	[TowCustomerTypeCodeAtTow] [nchar](2) NULL,
	[IsTowerOverrideAtTow] [tinyint] NOT NULL,
	--[TowRadiusMilesAtTow] [int] NULL,
	[IsZipCodeOverrideAtTow] [tinyint] NOT NULL,
	[TowZoneNumber] [smallint] NULL,
	[IsTowerKiosk] [tinyint] NULL,
	[CheckName] Varchar(60) NULL
	)

IF OBJECT_ID('tempdb..#tz1') IS NOT NULL DROP TABLE #tz1
CREATE TABLE #tz1(Tow_Zone_ID int, 
				  Branch_Number int,
				  Tow_Customer_Type_Code char(2) ,
				  Tow_Zone_Number int)

IF OBJECT_ID('tempdb..#SAS') IS NOT NULL DROP TABLE #SAS
CREATE TABLE #SAS(Salvage_ID int, 
				  Buyer_ID int)

IF OBJECT_ID('tempdb..#SAP') IS NOT NULL DROP TABLE #SAP
CREATE TABLE #SAP(Towbill_Number int, 
				  Tower_Payment_Method_Code char(3))
				  
				  
		  
IF OBJECT_ID('tempdb..#Branch_Tow_Rate') IS NOT NULL DROP TABLE #Branch_Tow_Rate
CREATE TABLE #Branch_Tow_Rate(charge_type_id int, 
							Tow_Customer_Type_Code char(3),
							Branch_Number int,
							Tow_Zone_Number int,
							Global_Rate Money)

IF OBJECT_ID('tempdb..#Branch_Tower_Tow_Rate3') IS NOT NULL DROP TABLE #Branch_Tower_Tow_Rate3
CREATE TABLE #Branch_Tower_Tow_Rate3(
	charge_type_id int, 
	Tow_Customer_Type_Code char(3),
	Branch_Number int,
	Tow_Zone_Number Int,
	Tower_id int,
	Tower_Rate Money)

							

IF OBJECT_ID('tempdb..#Branch_Tow_Rate3') IS NOT NULL DROP TABLE #Branch_Tow_Rate3
CREATE TABLE #Branch_Tow_Rate3(
	Branch_Number int,
	Tow_Zone_Number Int,
	Tow_Customer_Type_Code char(3),
	--Tow_Mileage_Band varchar(20),
	Tower_IAA_Branch_Zone_Rate_ID int
)


/*****Get Cancelled Towbills*****/
IF OBJECT_ID('tempdb..#Cancelled_Towbill') IS NOT NULL DROP TABLE #Cancelled_Towbill
CREATE TABLE #Cancelled_Towbill(Towbill_Number int)


/*****Get Reversed Dispatches*****/
IF OBJECT_ID('tempdb..#DispatchReversed_Towbill') IS NOT NULL DROP TABLE #DispatchReversed_Towbill
CREATE TABLE #DispatchReversed_Towbill(Towbill_Number int)


/*****Get Changed Towers*****/
IF OBJECT_ID('tempdb..#TowerChanged_Towbill') IS NOT NULL DROP TABLE #TowerChanged_Towbill
CREATE TABLE #TowerChanged_Towbill(Towbill_Number int, OldTowerASAPID varchar(100))
  
IF OBJECT_ID('tempdb..#TMP_ST') IS NOT NULL DROP TABLE #TMP_ST
CREATE TABLE #TMP_ST(StockID INT, TowbillNumber INT,Event_DateTime DATETIME)		

  
IF OBJECT_ID('tempdb..#TMP_checkname') IS NOT NULL DROP TABLE #TMP_checkname
CREATE TABLE #TMP_checkname([Salvage_ID] INT,[CheckName] Varchar(60) NULL)		
		  
--------------------------------------------------------------	

INSERT INTO #tz1(
Tow_Zone_ID
,Branch_Number
,Tow_Customer_Type_Code
,Tow_Zone_Number
)
select 
 tz1.Tow_Zone_ID
,tz1.Branch_Number
,tz1.Tow_Customer_Type_Code
,tz1.Tow_Zone_Number

from BI_ASAP_REP.dbo.tow_zone tz1 WITH (NOLOCK)
INNER JOIN  
	(
	select 	tow_zone_number
	,branch_number
	,tow_customer_type_code
	,update_datetime = max(update_datetime)
	from BI_ASAP_REP.dbo.tow_zone WITH (NOLOCK)
	group by 
		tow_zone_number
		,branch_number
		,tow_customer_type_code
	) tz2 
ON	tz1.tow_zone_number = tz2.tow_zone_number
and isnull(tz1.branch_Number, -1) = isnull(tz2.branch_Number, -1)
and tz1.tow_customer_type_code = tz2.tow_customer_type_code
and isnull(tz1.update_datetime, '12/31/2999')= isnull(tz2.update_datetime, '12/31/2999')

		  
INSERT INTO #SAS (
			Salvage_ID
			,Buyer_ID
			)
Select		SAS.Salvage_ID
			,SAS.Buyer_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_all_changes_dbo_Salvage_Sale](@StartLSN,@EndLSN,'all with merge') AS SAS
INNER JOIN
	(
	SELECT 	Salvage_ID 
			,MAX(Sold_DateTime) AS Sold_DateTime
	from	[BI_ASAP_REP].dbo.Salvage_Sale WITH (NOLOCK)
	group by Salvage_ID
	) AS ISS
ON 	SAS.Salvage_ID = ISS.Salvage_ID
AND SAS.Sold_DateTime = ISS.Sold_DateTime
WHERE [__$operation] = 5


INSERT INTO #SAP(
			Towbill_Number
			,Tower_Payment_Method_Code
			)
SELECT Towbill_Number, Tower_Payment_Method_Code
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_all_changes_dbo_Salvage_Advance_Payment](@StartLSN,@EndLSN,'all with merge') AS SAP
WHERE [__$operation] = 5
AND Check_Voided_Ind <> 1 
AND Salvage_Event_Category_Code = 'rls' 

--- Tower Kiosk

INSERT INTO #TMP_ST
SELECT  DS.StockID,S.Towbill_Number,MAX([Event_DateTime]) AS Event_DateTime
FROM [BI_ASAP_Rep].cdc.[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@StartLSN,@EndLSN,'all with merge')S
INNER JOIN [BI_ASAP_Rep].dbo.USER_INFO UI ON S.Update_user_ID = UI.User_ID AND UI.user_Name = 'Tower_Kiosk'
INNER JOIN DimStock DS ON S.Salvage_ID = DS.ASAPSalvageID
 WHERE [__$operation] = 5   
GROUP BY  StockID
      ,[Towbill_Number]

--select top 100 *
--from BI_ASAP_REP.dbo.Salvage_Advance_Payment
--where  Salvage_Event_Category_Code = 'rls' 
--AND Check_Voided_Ind <> 1 
	
/*********************************************************************************************
Create a temp table and Insert the values from the CDC Salvage Tow table.
*********************************************************************************************/
INSERT INTO #TMP_Salvage_Tow
   ([Salvage_ID],
	[Towbill_Number],
	[Create_DateTime],
	[Branch_Number],
	[Towin_Storage_Location_ID],
	[Towout_Storage_Location_ID],
	[Tower_ID],
	[Tow_Zone_Number] ,
	[Tow_Type_Code],
	[Tow_Miles],
	[Tow_Days],
	[Tow_Hours],
	[Tow_Priority_Due_Date],
	[Update_User_ID],
	[Tow_Successful_Ind],
	[Update_DateTime],
	[ESPS_Branch_Number],
	[ESPS_Towbill_Number],
	[Pickup_Required_Date],
	[Tow_Dry_Run_Ind],
	[Tow_Priority_Ind],
	[IAA_Employee_ID],
	[Barcode_Image_ID],
	[Internal_Tower_Branch_Number],
	[EFax_Status],
	[EFax_Delivery_DateTime],
	[EFax_Request_DateTime],
	[Fax_Handle_ID],
	[Direction_Code],
	[Travel_Time_Minutes],
	[Probability],
	[Previous_Tow_Miles]
	) 	
SELECT	
	[Salvage_ID],
	[Towbill_Number],
	[Create_DateTime],
	[Branch_Number],
	[Towin_Storage_Location_ID],
	[Towout_Storage_Location_ID],
	[Tower_ID],
	[Tow_Zone_Number] ,
	[Tow_Type_Code],
	[Tow_Miles],
	[Tow_Days],
	[Tow_Hours],
	[Tow_Priority_Due_Date],
	[Update_User_ID],
	[Tow_Successful_Ind],
	[Update_DateTime],
	[ESPS_Branch_Number],
	[ESPS_Towbill_Number],
	[Pickup_Required_Date],
	[Tow_Dry_Run_Ind],
	[Tow_Priority_Ind],
	[IAA_Employee_ID],
	[Barcode_Image_ID],
	[Internal_Tower_Branch_Number],
	[EFax_Status],
	[EFax_Delivery_DateTime],
	[EFax_Request_DateTime],
	[Fax_Handle_ID],
	[Direction_Code],
	[Travel_Time_Minutes],
	[Probability],
	[Previous_Tow_Miles]
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Tow](@StartLSN,@EndLSN,'all with merge') 
WHERE [__$operation] = 5
   

INSERT INTO #TMP_Stock_Tow(
Salvage_ID
, Buyer_ID
, Branch_Number
, Direction_Code
, EFax_Delivery_DateTime
, EFax_Request_DateTime
, EFax_Status_Code
, ESPS_Branch_Number
, ESPS_Towbill_Number
, Internal_Tower_Branch_ID
, Pickup_Required_DateTime
, Probability
, Provider_ID
--, Stock_Tow_Info_ID
, Tow_Days
, Tow_Miles
, Tow_Type_Code
, Tow_Zone_ID
, Tow_Zone_Number
, TowBill_Number
, Tower_ID
, Towin_Storage_Location_ID
, Towout_Storage_Location_ID
, Travel_Time_Minutes
, Update_DateTime
, Update_User_ID
, Tow_Priority_Ind
, Tower_Payment_Method
)
SELECT	ST.Salvage_Id
		,CASE 	WHEN st.tow_type_code = 'BY' THEN isnull(SS.Buyer_ID, 0) ELSE 0 END AS Buyer_ID
		,ST.Branch_Number
		,ST.Direction_Code
		,ST.EFax_Delivery_DateTime
		,ST.EFax_Request_DateTime
		,ST.EFax_Status
		,ST.ESPS_Branch_Number
		,ST.ESPS_Towbill_Number
		,ST.Internal_Tower_Branch_Number
		,ST.Pickup_Required_Date
		,ST.Probability
		,SI.Salvage_Provider_ID
		--,STI.Stock_Tow_Info_ID
		,ST.Tow_Days
		,ST.Tow_Miles
		,ST.Tow_Type_Code
		,Z.Tow_Zone_ID
		,ST.Tow_Zone_Number
		,ST.Towbill_Number
		,ST.Tower_ID
		,ST.Towin_Storage_Location_ID
		,ST.Towout_Storage_Location_ID
, 	Travel_Time_Minutes 			= ST.Travel_Time_Minutes
, 	Update_DateTime 			= ST.Update_DateTime
, 	Update_User_ID 				= ST.Update_User_ID
,	Tow_Priority_Ind		= CASE WHEN st.tow_type_code = 'PK' THEN S.Priority_Tow_Ind ELSE 0 END
,	Tower_Payment_Method	= SAP.Tower_Payment_Method_Code	
FROM 	#TMP_Salvage_Tow ST with (NOLOCK)
INNER JOIN [BI_ASAP_REP].dbo.Salvage AS	S  with (NOLOCK)
	ON 	ST.Salvage_ID = S.Salvage_ID
INNER JOIN  [BI_ASAP_REP].dbo.Salvage_Provider_Info AS SI with (NOLOCK)
	ON 	S.Salvage_ID = SI.Salvage_ID
--INNER JOIN  [PBI-DB1].dbo.Stock_Tow_Info AS	STI WITH (NOLOCK)
--	ON 	ST.Tow_Successful_Ind = STI.Tow_Successful_IND
--	AND ST.Tow_Dry_Run_Ind 	= STI.Tow_Dry_Run_IND
--	AND ST.Tow_Priority_Ind = STI.Tow_Priority_IND
LEFT OUTER JOIN	 #tz1 AS Z WITH (NOLOCK)
	ON	Z.Tow_Zone_Number = ST.Tow_Zone_Number
	AND	Z.Branch_Number = ST.Branch_Number
	AND	Z.Tow_Customer_Type_Code = CASE	WHEN st.tow_type_code = 'BY' THEN 'BY' ELSE 'SP' END
LEFT OUTER JOIN #SAS AS	SS WITH (NOLOCK)
	ON 	SS.Salvage_ID = ST.Salvage_ID
LEFT OUTER JOIN #SAP AS SAP WITH (NOLOCK)
ON 	ST.Towbill_Number = SAP.Towbill_Number


/*********************************************************************************************
Build ##TMP_FactStockTow.
*********************************************************************************************/
INSERT INTO #TMP_FactStockTow
   (		[StockID],
			[SalvageID],
			[TowBillNumber],
			[BranchNumber],
			[BuyerID],
			[CustomerType],
			[GlobalRate],
			[FirstAssignedTowerID],
			[PaidTowerID],
			[IsReleaseProblem],
			[ProviderID],
			[IsTowBillOutlier],
			[IsTowCancelled],
			[TowDays],
			[IsTowDispatchReversed],
			[IsTowDryRun],
			[TowerBranchZoneRateID],
			[IsTowerChanged],
			[TowerID],
			[TowCategory],
		--	[TowMileageBand],
			[TowTypeCode],
			[TowerPaymentMethod],
			[TowerPickupPerformance],
			[TowerRate],
			[TowinStorageLocationID],
			[TowMiles],
			[TowoutStorageLocationID],
			[IsTowPriority],
			[IsTowSuccessful],
		 --[TowZoneID],
			[TravelTimeMinutes],
			[ETLLoadID],
			[ETLLoadUpdateID],
			[AdditionalFeePerMileAtTow],
			[IsBuyerOverrideAtTow],
			[DryRunAmtAtTow],
			[DryRunCalculationMethodAtTow],
			[FuelSurchargeAmtAtTow],
			[FuelSurchargeCalculationMethodAtTow],
			[PickupTimeLimitHoursAtTow],
			[RateAmountAtTow],
			[SalvageDutyTypeCodeAtTow],
			[StockTypeAtTow],
			[TowCustomerTypeCodeAtTow],
			[IsTowerOverrideAtTow],
			--[TowRadiusMilesAtTow],
			[IsZipCodeOverrideAtTow],
			[TowZoneNumber],
			[IsTowerKiosk]
	) 	
SELECT	
	S.StockID
	,TST.Salvage_ID
	,TST.Towbill_Number
	,ISNULL(DB.BranchNumber,0)
	,TST.Buyer_ID
	,CASE WHEN TST.Tow_Type_Code = 'BY' THEN 'BY' 
		  ELSE 'SP' 
		  END 
	,'' AS GlobalRate
	,'' AS FirstAssignedTowerID
	,0  AS PaidTowerID
	,ISNULL(S.IsReleaseProblem,0)
	,ISNULL(TST.Provider_ID,0)
	,0 AS IsTowBillOutlier
	,0 AS IsTowCancelled  ---- Calculated
	,ISNULL(TST.Tow_Days, 0 )
	,0 AS IsTowDispatchReversed --- Calculated
	,ISNULL(Tmp_SalvageTow.Tow_Dry_Run_Ind, 0) AS IsTowDryRun
	,0 AS TowerBranchZoneRateID --- Calculated
	,0 AS IsTowerChanged --- Calculated
	,ISNULL(TST.Tower_ID, 0) AS TowerID
	,'' AS TowCategory
	--,'' AS TowMileageBand --- Calculated
	,ISNULL(TST.Tow_Type_Code, '') AS TowTypeCode
	,ISNULL(TST.Tower_Payment_Method, '') AS TowerPaymentMethod --- Calculated
	,'' AS TowerPickupPerformance --- Calculated
	,NULL AS TowerRate --- Calculated
	,ISNULL(TST.Towin_Storage_Location_ID, 0) AS TowinStorageLocationID
	,ISNULL(TST.Tow_Miles,0) AS TowMiles
	,ISNULL(Tmp_SalvageTow.Towout_Storage_Location_ID, 0) AS TowoutStorageLocationID
	,ISNULL(TST.Tow_Priority_Ind, 0) AS IsTowPriority
	,ISNULL(Tmp_SalvageTow.Tow_Successful_Ind, 0) AS IsTowSuccessful
	--,ISNULL(TST.Tow_Zone_ID,0) AS TowZoneID
	,ISNULL(TST.Travel_Time_Minutes,0) AS TravelTimeMinutes
	,@ETLLoadID
	,@ETLLoadID AS ETLLoadUpdateID
	,NULL AdditionalFeePerMileAtTow
	,0 AS IsBuyerOverrideAtTow
	,0 AS DryRunAmtAtTow
	,'' AS DryRunCalculationMethodAtTow
	,0 AS FuelSurchargeAmtAtTow
	,'' AS FuelSurchargeCalculationMethodAtTow
	,'' AS PickupTimeLimitHoursAtTow
	,NULL AS RateAmountAtTow
	,'' AS SalvageDutyTypeCodeAtTow
	,'' AS StockTypeAtTow
	,'' AS TowCustomerTypeCodeAtTow
	,0 AS IsTowerOverrideAtTow
	--,0 AS TowRadiusMilesAtTow
	,0 AS IsZipCodeOverrideAtTow
	,Tmp_SalvageTow.Tow_Zone_Number As TowZoneNumber
	,0
FROM #TMP_Stock_Tow TST
INNER JOIN #TMP_Salvage_Tow Tmp_SalvageTow
ON TST.Salvage_ID = Tmp_SalvageTow.Salvage_ID
AND TST.TowBill_Number = Tmp_SalvageTow.Towbill_Number
INNER JOIN dbo.DimStock S WITH (NOLOCK)
	ON TST.Salvage_ID = S.ASAPSalvageID
LEFT OUTER JOIN dbo.DimBranch DB WITH (NOLOCK)
	ON TST.Branch_Number = DB.BranchNumber

	
INSERT INTO #Branch_Tow_Rate(charge_type_id
			,Tow_Customer_Type_Code
			,Branch_Number
			,Tow_Zone_Number
			,Global_Rate)
SELECT ASAP_tbzr.charge_type_id
		,TowCustomerTypeCode
		,tbzr.BranchNumber
		,tbzr.TowZoneNumber
		,tbzr.RateAmount as Global_Rate 
FROM dbo.FactTowerBranchZoneRate AS tbzr WITH (NOLOCK)
INNER JOIN BI_ASAP_REP.dbo.Tower_IAA_Branch_Zone_Rate ASAP_tbzr WITH (NOLOCK)
ON tbzr.TowerBranchZoneRateID = ASAP_tbzr.Tower_IAA_Branch_Zone_Rate_ID
WHERE tbzr.towerid IS NULL OR tbzr.towerid = 0

INSERT INTO #Branch_Tower_Tow_Rate3(
	charge_type_id,
	Tow_Customer_Type_Code,
 	Branch_Number,
	Tow_Zone_Number,
	Tower_id,
	Tower_Rate
) 
SELECT 
	ASAP_tbzr.charge_type_id,
	TowCustomerTypeCode, 
	tbzr.BranchNumber, 
	TowZoneNumber,	
	tbzr.towerid,
	tbzr.RateAmount as Tower_Rate
FROM dbo.FactTowerBranchZoneRate tbzr  WITH (NOLOCK)
INNER JOIN BI_ASAP_REP.dbo.Tower_IAA_Branch_Zone_Rate ASAP_tbzr WITH (NOLOCK)
ON tbzr.TowerBranchZoneRateID = ASAP_tbzr.Tower_IAA_Branch_Zone_Rate_ID
WHERE tbzr.towerid <> 0	

INSERT INTO #Branch_Tow_Rate3(
	Branch_Number,
	Tow_Zone_Number,
	Tow_Customer_Type_Code,
--	Tow_Mileage_Band,
	Tower_IAA_Branch_Zone_Rate_ID
) 
SELECT 
	tbzr.BranchNumber, 
	TowZoneNumber,	
	TowCustomerTypeCode,
--	TowMileageBand,
	TowerBranchZoneRateID
FROM dbo.FactTowerBranchZoneRate tbzr  WITH (NOLOCK)

INSERT INTO #Cancelled_Towbill(Towbill_Number)
SELECT Towbill_Number 
FROM  [BI_ASAP_Rep].[cdc].[fn_cdc_get_all_changes_dbo_Salvage_Event_History](@StartLSN,@EndLSN,'all with merge')
WHERE [__$operation] = 5
AND Towbill_Number IS NOT NULL 
AND Salvage_Event_Code = 'dppc'

INSERT INTO #DispatchReversed_Towbill(Towbill_Number)
SELECT DISTINCT Towbill_Number 
from [BI_ASAP_Rep].[cdc].[fn_cdc_get_all_changes_dbo_Salvage_Note](@StartLSN,@EndLSN,'all with merge')
WHERE [__$operation] = 5
AND Towbill_Number IS NOT NULL 
AND Salvage_Event_Category_Code = 'dsp' 
AND Note_Text like '% performed dispatch reversal'


INSERT INTO #TowerChanged_Towbill(Towbill_Number, OldTowerASAPID)	
SELECT DISTINCT Towbill_Number,
CASE WHEN CHARINDEX( '~', Note_Text) > 0 THEN
	substring (Note_Text, CHARINDEX( 'of login from ', Note_Text)+14, (CHARINDEX( '~', Note_Text) - (CHARINDEX( 'of login from ', Note_Text)+14))) 
WHEN CHARINDEX( ' to ', Note_Text) > 0 THEN
	substring (Note_Text, CHARINDEX( 'of login from ', Note_Text)+14, (CHARINDEX( ' to ', Note_Text) - (CHARINDEX( 'of login from ', Note_Text)+14))) 
ELSE
	'99999'
END
from [BI_ASAP_Rep].[cdc].[fn_cdc_get_all_changes_dbo_Salvage_Note](@StartLSN,@EndLSN,'all with merge')
WHERE [__$operation] = 5
AND Towbill_Number IS NOT NULL 
AND Salvage_Event_Category_Code = 'dsp' 
AND Note_Text like 'Tower changed at time of login from %'		

INSERT INTO #TMP_checkname ([Salvage_ID] ,[CheckName] )

SELECT DISTINCT 
  S.[Salvage_ID] ,tt.Tower_Name
 
  FROM 
   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage] (@StartLSN,@EndLSN,'all with merge') s 
  Left outer join  [BI_ASAP_Rep].[dbo].[Salvage_Expense] se
  on se.Salvage_ID=s.Salvage_ID
  Join (Select Salvage_ID,Max(Create_Datetime) Create_Datetime  FROM [BI_ASAP_Rep].[dbo].[Salvage_Expense] see
    join bi_asap_rep.dbo.charge_type CT WITH(nolock) 
      ON SEe.charge_type_id = CT.charge_type_id 
    JOIN bi_asap_rep.dbo.charge C WITH(nolock) 
              ON C.charge_id = CT.charge_id 
  where  C.Charge_Description In ('Tow') AND [Payment_Status_Code] <>'vod' 
  Group by Salvage_ID) a
  on a.Salvage_ID=se.Salvage_ID
  and a.Create_Datetime=se.Create_Datetime
 JOIN #TMP_Salvage_Tow ST WITH(nolock) 
	  on ST.Salvage_ID=se.Salvage_ID
	  and st.Tower_ID=se.ASAP_Object_ID
 JOIN [BI_ASAP_Rep].[dbo].[Tower] TT WITH(nolock) 
	  on TT.Tower_ID=ST.Tower_ID    
	 and TT.Status_Code='ACT' 
 WHERE s.[__$operation] = 5

	
------------------------------------------------	
/****Global Rate****/
--SP
UPDATE #TMP_FactStockTow                    
SET  GlobalRate = ISNULL(Rate_Zone.Global_Rate, 0)          
FROM #TMP_FactStockTow FST WITH (NOLOCK)
INNER JOIN #TMP_Salvage_Tow ST
ON FST.SalvageID = ST.Salvage_ID
AND FST.TowBillNumber = ST.Towbill_Number
INNER JOIN #Branch_Tow_Rate Rate_Zone WITH (NOLOCK)
	ON FST.branchNumber = Rate_Zone.Branch_Number
	AND ST.Tow_Zone_Number = Rate_Zone.Tow_Zone_Number
	AND Rate_Zone.charge_type_id = 47


--Buyer
UPDATE #TMP_FactStockTow                    
SET GlobalRate = ISNULL(Rate_Zone.Global_Rate, 0)          
FROM #TMP_FactStockTow FST WITH (NOLOCK)
INNER JOIN #TMP_Salvage_Tow ST
ON FST.SalvageID = ST.Salvage_ID
AND FST.TowBillNumber = ST.Towbill_Number
INNER JOIN dbo.DimBuyer Buyer WITH (NOLOCK)
	ON FST.BuyerID = Buyer.BuyerID
INNER JOIN #Branch_Tow_Rate Rate_Zone WITH (NOLOCK)
	ON FST.branchNumber = Rate_Zone.Branch_Number
	AND ST.Tow_Zone_Number = Rate_Zone.Tow_Zone_Number
	AND FST.TowTypeCode = Rate_Zone.Tow_Customer_Type_Code
	AND CASE WHEN Buyer.IsPreferredBuyer = 1 THEN 367 ELSE 158 END = Rate_Zone.charge_type_id

/****Tower Rate****/
--SP
UPDATE #TMP_FactStockTow                  
SET TowerRate 	= ISNULL(Tower_Rate_Zone.Tower_Rate,0)
FROM #TMP_FactStockTow FST WITH (NOLOCK)
INNER JOIN #TMP_Salvage_Tow ST
ON FST.SalvageID = ST.Salvage_ID
AND FST.TowBillNumber = ST.Towbill_Number
INNER JOIN #Branch_Tower_Tow_Rate3 Tower_Rate_Zone WITH (NOLOCK)
	ON FST.BranchNumber = Tower_Rate_Zone.Branch_Number
	AND ST.Tow_Zone_Number = Tower_Rate_Zone.Tow_Zone_Number
	AND FST.towerid = Tower_Rate_Zone.tower_id
	AND Tower_Rate_Zone.charge_type_id = 47

--Buyer
UPDATE #TMP_FactStockTow                     
SET TowerRate 	= ISNULL(Tower_Rate_Zone.Tower_Rate,0)
FROM #TMP_FactStockTow FST WITH (NOLOCK)
INNER JOIN #TMP_Salvage_Tow ST
ON FST.SalvageID = ST.Salvage_ID
AND FST.TowBillNumber = ST.Towbill_Number
INNER JOIN dbo.DimBuyer Buyer WITH (NOLOCK)
	ON FST.BuyerID = Buyer.BuyerID
INNER JOIN #Branch_Tower_Tow_Rate3 Tower_Rate_Zone WITH (NOLOCK)
	ON FST.BranchNumber = Tower_Rate_Zone.Branch_Number
	AND ST.Tow_Zone_Number = Tower_Rate_Zone.Tow_Zone_Number
	AND FST.towerid = Tower_Rate_Zone.tower_id
	AND FST.TowTypeCode = Tower_Rate_Zone.Tow_Customer_Type_Code
	AND CASE WHEN Buyer.IsPreferredBuyer = 1 THEN 367 ELSE 158 END = Tower_Rate_Zone.charge_type_id

/****Tow Mileage Band, Tower_IAA_Branch_Zone_Rate_DM_ID****/
UPDATE #TMP_FactStockTow                      
SET --TowMileageBand = Rate_Zone3.Tow_Mileage_Band
	TowerBranchZoneRateID = Rate_Zone3.Tower_IAA_Branch_Zone_Rate_ID
FROM #TMP_FactStockTow FST WITH (NOLOCK)
INNER JOIN #TMP_Salvage_Tow ST
ON FST.SalvageID = ST.Salvage_ID
AND FST.TowBillNumber = ST.Towbill_Number
INNER JOIN #Branch_Tow_Rate3 Rate_Zone3 WITH (NOLOCK)
	ON FST.BranchNumber = Rate_Zone3.Branch_Number
	AND ST.Tow_Zone_Number = Rate_Zone3.Tow_Zone_Number
	AND FST.CustomerType = Rate_Zone3.Tow_Customer_Type_Code

/****Tow_Cancelled_IND****/
UPDATE #TMP_FactStockTow                     
SET IsTowCancelled = CASE WHEN tct.Towbill_Number IS NULL THEN 0 ELSE 1 END
FROM #TMP_FactStockTow SRC WITH (NOLOCK)
INNER JOIN #Cancelled_Towbill tct WITH (NOLOCK)
	ON SRC.TowbillNumber = tct.Towbill_Number

/****Tow Dispatch Reversed IND****/
UPDATE #TMP_FactStockTow                     
SET IsTowDispatchReversed= CASE WHEN tdrt.Towbill_Number IS NULL THEN 0 ELSE 1 END
FROM #TMP_FactStockTow SRC WITH (NOLOCK)
INNER JOIN #DispatchReversed_Towbill tdrt WITH (NOLOCK)
	ON SRC.TowbillNumber = tdrt.Towbill_Number

/****Tower Changed IND****/
UPDATE #TMP_FactStockTow                     
SET IsTowerChanged = CASE WHEN ttcht.Towbill_Number IS NULL THEN 0 ELSE 1 END
FROM #TMP_FactStockTow SRC WITH (NOLOCK)
INNER JOIN #TowerChanged_Towbill ttcht WITH (NOLOCK)
	ON SRC.TowbillNumber = ttcht.Towbill_Number
	
UPDATE #TMP_FactStockTow
SET		[AdditionalFeePerMileAtTow] = FTBZR.[AdditionalFeePerMile],
		[IsBuyerOverrideAtTow] = FTBZR.[IsBuyerOverride],
		[DryRunAmtAtTow] = FTBZR.[DryRunAmt],
		[DryRunCalculationMethodAtTow] = FTBZR.[DryRunCalculationMethod],
		[FuelSurchargeAmtAtTow] = FTBZR.[FuelSurchargeAmt],
		[FuelSurchargeCalculationMethodAtTow] = FTBZR.[FuelSurchargeCalculationMethod],
		[PickupTimeLimitHoursAtTow] = FTBZR.[PickupTimeLimitHours],
		[RateAmountAtTow] = FTBZR.[RateAmount],
		[SalvageDutyTypeCodeAtTow] = FTBZR.[SalvageDutyTypeCode],
		[StockTypeAtTow] = FTBZR.[StockType],
		[TowCustomerTypeCodeAtTow] = FTBZR.[TowCustomerTypeCode],
		[IsTowerOverrideAtTow] = FTBZR.[IsTowerOverride],
		--[TowRadiusMilesAtTow] = FTBZR.[TowRadiusMiles],
		[IsZipCodeOverrideAtTow] = FTBZR.[IsZipCodeOverride]
		 
FROM #TMP_FactStockTow FST
INNER JOIN dbo.FactTowerBranchZoneRate FTBZR WITH (NOLOCK)
ON FST.TowerBranchZoneRateID = FTBZR.TowerBranchZoneRateID
WHERE FST.TowerBranchZoneRateID <> 0

UPDATE #TMP_FactStockTow
SET TowerPickupPerformance = 
		CASE 
		WHEN (FST.TowMiles <= 100 AND FSTCT.ReleaseToPickupEffectiveDaysBusiness < 1) OR (FST.TowMiles > 100 AND FSTCT.ReleaseToPickupEffectiveDaysBusiness < 2) THEN 'Early'
		WHEN (FST.TowMiles <= 100 AND FSTCT.ReleaseToPickupEffectiveDaysBusiness >= 1 AND FSTCT.ReleaseToPickupEffectiveDaysBusiness < 2) OR (FST.TowMiles > 100 AND FSTCT.ReleaseToPickupEffectiveDaysBusiness >= 2 AND FSTCT.ReleaseToPickupEffectiveDaysBusiness < 3) THEN 'On Time'
		ELSE 'Late' END
FROM #TMP_FactStockTow FST
INNER JOIN dbo.FactStockTowCycleTime FSTCT WITH (NOLOCK)
ON FST.StockID = FSTCT.StockID
AND FST.TowBillNumber = FSTCT.TowBillNumber


--Tower Kiosk

UPDATE FST
SET  IsTowerKiosk = 1
FROM #TMP_FactStockTow FST WITH(NOLOCK)
INNER JOIN #TMP_ST TS ON FST.StockID = TS.StockID AND FST.TowBillNumber = TS.TowbillNumber


UPDATE FST
SET  CheckName = CN.CheckName
FROM #TMP_FactStockTow FST WITH(NOLOCK)
INNER JOIN #TMP_checkname CN 
ON FST.SalvageID = CN.Salvage_ID 
INNER JOIN #TMP_Salvage_Tow ST
ON FST.SalvageID = ST.Salvage_ID
AND FST.TowBillNumber = ST.Towbill_Number  

--/*********************************************************************************************
--MERGE the data from the temp table into the EDW FactStockTow table
--This does Update and Insert only.  No Deletes
--*********************************************************************************************/
IF OBJECT_ID('tempdb..#FactStockTowRecordCount') IS NOT NULL DROP TABLE #FactStockTowRecordCount
CREATE TABLE #FactStockTowRecordCount (ChangeType VARCHAR(10)) 


BEGIN TRANSACTION 
  
    MERGE [dbo].FactStockTow AS FST
    USING #TMP_FactStockTow AS TST
          ON TST.[StockID] = FST.[StockID]
          AND TST.[TowBillNumber] = FST.[TowBillNumber]
	WHEN MATCHED THEN 
     UPDATE SET 
		[BranchNumber]= TST.[BranchNumber],
		[BuyerID]= TST.[BuyerID],
		[CustomerType]= TST.[CustomerType],
		[GlobalRate]= TST.[GlobalRate],
		[FirstAssignedTowerID]= TST.[FirstAssignedTowerID],
		[PaidTowerID]= TST.[PaidTowerID],
		[IsReleaseProblem]= TST.[IsReleaseProblem],
		[ProviderID]= TST.[ProviderID],
		[IsTowBillOutlier]= TST.[IsTowBillOutlier],
		[IsTowCancelled]= TST.[IsTowCancelled],
		[TowDays]= TST.[TowDays],
		[IsTowDispatchReversed]= TST.[IsTowDispatchReversed],
		[IsTowDryRun]= TST.[IsTowDryRun],
		[TowerBranchZoneRateID]= TST.[TowerBranchZoneRateID],
		[IsTowerChanged]= TST.[IsTowerChanged],
		[TowerID]= TST.[TowerID],
		[TowCategory]= TST.[TowCategory],
		--[TowMileageBand]= TST.[TowMileageBand],
		[TowTypeCode]= TST.[TowTypeCode],
		[TowerPaymentMethod]= TST.[TowerPaymentMethod],
		[TowerPickupPerformance]= TST.[TowerPickupPerformance],
		[TowerRate]= TST.[TowerRate],
		[TowinStorageLocationID]= TST.[TowinStorageLocationID],
		[TowMiles]= TST.[TowMiles],
		[TowoutStorageLocationID]= TST.[TowoutStorageLocationID],
		[IsTowPriority]= TST.[IsTowPriority],
		[IsTowSuccessful]= TST.[IsTowSuccessful],
		--[TowZoneID]= TST.[TowZoneID],
		[TravelTimeMinutes]= TST.[TravelTimeMinutes],
		[ETLLoadUpdateID]= TST.[ETLLoadUpdateID],
		[AdditionalFeePerMileAtTow]= TST.[AdditionalFeePerMileAtTow],
		[IsBuyerOverrideAtTow]= TST.[IsBuyerOverrideAtTow],
		[DryRunAmtAtTow]= TST.[DryRunAmtAtTow],
		[DryRunCalculationMethodAtTow]= TST.[DryRunCalculationMethodAtTow],
		[FuelSurchargeAmtAtTow]= TST.[FuelSurchargeAmtAtTow],
		[FuelSurchargeCalculationMethodAtTow]= TST.[FuelSurchargeCalculationMethodAtTow],
		[PickupTimeLimitHoursAtTow]= TST.[PickupTimeLimitHoursAtTow],
		[RateAmountAtTow]= TST.[RateAmountAtTow],
		[SalvageDutyTypeCodeAtTow]= TST.[SalvageDutyTypeCodeAtTow],
		[StockTypeAtTow]= TST.[StockTypeAtTow],
		[TowCustomerTypeCodeAtTow]= TST.[TowCustomerTypeCodeAtTow],
		[IsTowerOverrideAtTow]= TST.[IsTowerOverrideAtTow],
		--[TowRadiusMilesAtTow]= TST.[TowRadiusMilesAtTow],
		[IsZipCodeOverrideAtTow]= TST.[IsZipCodeOverrideAtTow],
		[TowZoneNumber]= TST.[TowZoneNumber],
		[IsTowerKiosk]= TST.[IsTowerKiosk],
		[CheckName]=TST.[CheckName]
	WHEN NOT MATCHED THEN
    INSERT( [StockID],
			[TowBillNumber],
			[BranchNumber],
			[BuyerID],
			[CustomerType],
			[GlobalRate],
			[FirstAssignedTowerID],
			[PaidTowerID],
			[IsReleaseProblem],
			[ProviderID],
			[IsTowBillOutlier],
			[IsTowCancelled],
			[TowDays],
			[IsTowDispatchReversed],
			[IsTowDryRun],
			[TowerBranchZoneRateID],
			[IsTowerChanged],
			[TowerID],
			[TowCategory],
		--	[TowMileageBand],
			[TowTypeCode],
			[TowerPaymentMethod],
			[TowerPickupPerformance],
			[TowerRate],
			[TowinStorageLocationID],
			[TowMiles],
			[TowoutStorageLocationID],
			[IsTowPriority],
			[IsTowSuccessful],
		--	[TowZoneID],
			[TravelTimeMinutes],
			[ETLLoadID],
			[ETLLoadUpdateID],
			[AdditionalFeePerMileAtTow],
			[IsBuyerOverrideAtTow],
			[DryRunAmtAtTow],
			[DryRunCalculationMethodAtTow],
			[FuelSurchargeAmtAtTow],
			[FuelSurchargeCalculationMethodAtTow],
			[PickupTimeLimitHoursAtTow],
			[RateAmountAtTow],
			[SalvageDutyTypeCodeAtTow],
			[StockTypeAtTow],
			[TowCustomerTypeCodeAtTow],
			[IsTowerOverrideAtTow],
		--	[TowRadiusMilesAtTow],
			[IsZipCodeOverrideAtTow],
			[TowZoneNumber],
			[IsTowerKiosk],
			[CheckName]
	) 	
    VALUES ([StockID],
			[TowBillNumber],
			[BranchNumber],
			[BuyerID],
			[CustomerType],
			[GlobalRate],
			[FirstAssignedTowerID],
			[PaidTowerID],
			[IsReleaseProblem],
			[ProviderID],
			[IsTowBillOutlier],
			[IsTowCancelled],
			[TowDays],
			[IsTowDispatchReversed],
			[IsTowDryRun],
			[TowerBranchZoneRateID],
			[IsTowerChanged],
			[TowerID],
			[TowCategory],
		--	[TowMileageBand],
			[TowTypeCode],
			[TowerPaymentMethod],
			[TowerPickupPerformance],
			[TowerRate],
			[TowinStorageLocationID],
			[TowMiles],
			[TowoutStorageLocationID],
			[IsTowPriority],
			[IsTowSuccessful],
		--	[TowZoneID],
			[TravelTimeMinutes],
			[ETLLoadID],
			[ETLLoadUpdateID],
			[AdditionalFeePerMileAtTow],
			[IsBuyerOverrideAtTow],
			[DryRunAmtAtTow],
			[DryRunCalculationMethodAtTow],
			[FuelSurchargeAmtAtTow],
			[FuelSurchargeCalculationMethodAtTow],
			[PickupTimeLimitHoursAtTow],
			[RateAmountAtTow],
			[SalvageDutyTypeCodeAtTow],
			[StockTypeAtTow],
			[TowCustomerTypeCodeAtTow],
			[IsTowerOverrideAtTow],
		--	[TowRadiusMilesAtTow],
			[IsZipCodeOverrideAtTow],
			[TowZoneNumber],
			[IsTowerKiosk],
			[CheckName]
    ) 	
OUTPUT $action INTO #FactStockTowRecordCount;
	
	  

/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #FactStockTowRecordCount

--/*********************************************************************************************
--Return the change counts
--*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		
		IF OBJECT_ID('tempdb..#TMP_Salvage_Tow') IS NOT NULL DROP TABLE #TMP_Salvage_Tow
		IF OBJECT_ID('tempdb..#TMP_Stock_Tow') IS NOT NULL DROP TABLE #TMP_Stock_Tow
		IF OBJECT_ID('tempdb..#TMP_FactStockTow') IS NOT NULL DROP TABLE #TMP_FactStockTow
		IF OBJECT_ID('tempdb..#tz1') IS NOT NULL DROP TABLE #tz1
		IF OBJECT_ID('tempdb..#SAS') IS NOT NULL DROP TABLE #SAS
		IF OBJECT_ID('tempdb..#SAP') IS NOT NULL DROP TABLE #SAP
		IF OBJECT_ID('tempdb..#Branch_Tow_Rate') IS NOT NULL DROP TABLE #Branch_Tow_Rate
		IF OBJECT_ID('tempdb..#Branch_Tower_Tow_Rate3') IS NOT NULL DROP TABLE #Branch_Tower_Tow_Rate3
		IF OBJECT_ID('tempdb..#Branch_Tow_Rate3') IS NOT NULL DROP TABLE #Branch_Tow_Rate3
		IF OBJECT_ID('tempdb..#Cancelled_Towbill') IS NOT NULL DROP TABLE #Cancelled_Towbill
		IF OBJECT_ID('tempdb..#DispatchReversed_Towbill') IS NOT NULL DROP TABLE #DispatchReversed_Towbill
		IF OBJECT_ID('tempdb..#TowerChanged_Towbill') IS NOT NULL DROP TABLE #TowerChanged_Towbill
		IF OBJECT_ID('tempdb..#TMP_ST') IS NOT NULL DROP TABLE #TMP_ST


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
		
		IF OBJECT_ID('tempdb..#TMP_Salvage_Tow') IS NOT NULL DROP TABLE #TMP_Salvage_Tow
		IF OBJECT_ID('tempdb..#TMP_Stock_Tow') IS NOT NULL DROP TABLE #TMP_Stock_Tow
		IF OBJECT_ID('tempdb..#TMP_FactStockTow') IS NOT NULL DROP TABLE #TMP_FactStockTow
		IF OBJECT_ID('tempdb..#tz1') IS NOT NULL DROP TABLE #tz1
		IF OBJECT_ID('tempdb..#SAS') IS NOT NULL DROP TABLE #SAS
		IF OBJECT_ID('tempdb..#SAP') IS NOT NULL DROP TABLE #SAP
		IF OBJECT_ID('tempdb..#Branch_Tow_Rate') IS NOT NULL DROP TABLE #Branch_Tow_Rate
		IF OBJECT_ID('tempdb..#Branch_Tower_Tow_Rate3') IS NOT NULL DROP TABLE #Branch_Tower_Tow_Rate3
		IF OBJECT_ID('tempdb..#Branch_Tow_Rate3') IS NOT NULL DROP TABLE #Branch_Tow_Rate3
		IF OBJECT_ID('tempdb..#Cancelled_Towbill') IS NOT NULL DROP TABLE #Cancelled_Towbill
		IF OBJECT_ID('tempdb..#DispatchReversed_Towbill') IS NOT NULL DROP TABLE #DispatchReversed_Towbill
		IF OBJECT_ID('tempdb..#TowerChanged_Towbill') IS NOT NULL DROP TABLE #TowerChanged_Towbill
		IF OBJECT_ID('tempdb..#TMP_ST') IS NOT NULL DROP TABLE #TMP_ST


		RETURN -1
	END CATCH
	END


