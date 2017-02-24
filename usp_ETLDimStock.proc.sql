/*
USE EDW
GO 
==============================================================================
Author		: Venkata Vempali
Create date	: 09/14/2011
Description	: Populate the EDW DimStock table from CDC tables.
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
10/11/2011  Venkata Vempali               Added all the TO-BE Columns & Default values.
10/18/2011	Matt Stelter		Changed field type for Express_Pickup_Ind
10/28/2011	Venkata Vempali            	Added code to drop temp Tables (DBA Code Review)
                                Used columns names instead of *
12/05/2011  Venkata Vempali               Added object & written etl code for CurrentStockStatusCode  
12/05/2011 Somesh Velupalli     Added object IsStandardSale & written etl code for IsAffiliateStock ,IsStandardSale  
12/09/2011	 Murao Sidd         Written ETL for CurrentBuyerID and CurrentProviderID
12/09/2011  Venkata Vempali               Written ETL for IsCancel, ProcessStatusCode ,ProcessStatusDescription ,CurrentStockStatusDescription
									and modified logic for IsStockSold
1/26/2012	Matt Stelter		Added EDW Phase 2-Sprint 7 Objects 
1/30/2012   Venkata Vempali               Written ETL for IsBuyerReneged ,IsBuyerRenegedAfterSettlement ,BuyerRenegedCount ,IsBuyerCancel					
										,IsBuyerCancelAfterSettlement ,BuyerCancelCount ,BuyerRenegedCancelCount ,BuyerRenegedCancelCountBeforeSettlement 
										,BuyerRenegedType ,PreviousAgreementTermTypeCode,PreviousAgreementTermTypeDescription,IsTitleProblem ,IsHadTitleProblem
1/30/2012   Venkata Vempali               Commented IsBuyerCancelAfterSettlement & Renamed IsBuyerRenegedAfterSettlement to IsBuyerRenegedCancelAfterSettlement
2/16/2012	Matt Stelter		Removed commented code.  Added NOLOCK removed Temp table Deletes 
02/29/2012  Somesh velupalli    Changed updated statement for Title processing branch number  -- Bug 8415
3/7/2012	Matt Stelter		Added IsOnHold and IsHadHold --R5.2 S10
3/7/2012    Venkata Vempali              Added ActualCashValue ,BlackbookACV & IsACVOutlier --R5.2 S10
3/19/2012   Somesh velupalli    Sprint 11 - Stock Objects Misc 
3/20/2012   Preetham DuVenkata Vempalia      Added AssignmentMethod
3/20/2012   Venkata Vempali              Added IsIBF,SalePrice,SaleTaxAmount,PAPrice --R5.2 S11 
3/21/2012   Preetham DuVenkata Vempalia      Added 'FARM' to EBIZ AssignmentMethod
3/26/2012   Preetham DuVenkata Vempalia      Updated the salvage_event_codes for AssignmentMethod
3/28/2012   Venkata Vempali              Updated code for Sprint 12 - Stock Buyer Renege/Cancel
4/4/2012    Somesh Velupalli	Added code for HoldCounts and CourtOrderHoldCounts --R5.2 S12 
4/4/2012    Venkata Vempali              Added code for IsFlippedtoPA and IsAgreementChanged -R5.2 S12
								Updated code for Agreement & Previous Agreement Term Type code
4/6/2012    Venkata Vempali              Updated logic for Previous Agreement Term Type code IsFlippedtoPA and IsAgreementChanged --R5.2 S12 
4/13/2012   Somesh Velupalli    IsCreditSale
4/16/2012   Somesh Velupalli	Updated BulkSale logic and moved Temp Tables Prior to merge statement
4/17/2012   Somesh Velupalli    Added PickupLocation ID
4/26/2012   Venkata Vempali              Error Fix - #TMP_BCC_Data (Added Salvage_ID IS NOT NULL in where clause)
4/25/2012   Venkata Vempali              GrossReturn
4/26/2012   Somesh Velupalli    Advance Storage Info Objects
4/27/2012   Venkata Vempali              Added SaleRunCountExcludingCancel ,SaleRunCountExCancelExNoBid
4/30/2012   Sidd Murao          Added Original Branch Name, Original Provider Name, Original ProviderGroup Name , Branch Transfer Ind, Provider Transfer Ind
4/30/2012   Venkata Vempali              Updated logic SaleRunCountExcludingCancel ,SaleRunCountExCancelExNoBid
4/30/2012   Venkata Vempali              Updated the update statement for IsIBFSold
5/3/2012   Sidd Murao           Changed Column Names for Transfer
5/8/2012    Venkata Vempali              Commented out SaleRunCountExcludingCancel ,SaleRunCountExCancelExNoBid
5/11/2012	Juan X Jacome       Added WITH(NOLOCK) for #TMP_AdvanceStorage Insert
5/15/2012   Venkata Vempali              Added BuyerFinanceCompanyCode ,BuyerFinanceAmount
5/15/2012  Matt Stelter         Added ECR
5/17/2012   Venkata Vempali              Added SaleDocumentTypeCategory
5/21/2012   Venkata Vempali              Updated the Logic for GrossReturn -- Updated #TMP_Gross_Return_Salvage_ID
5/21/2012  Somesh Velupalli     Changed logic for Title Problem IND
5/30/2012  Somesh Velupalli     Added SoldModelYearAge  And SoldModelYearBand 
6/12/2012  Preetham DuVenkata Vempalia       Commented out the IsHadReleaseProblem
6/12/2012   Venkata Vempali              Updated the Logic for Sale Doc Number.
                                Included 'Salvage' table to cdc for #TMP_Gross_Return_Salvage_ID
06/14/2012 Somesh Velupalli     Added RunandDrive Indicators        
06/19/2012 Sidd Murao           Changed logic for Original Branch Number     
06/20/2012 Somesh Velupalli     Updated logic for Run&Drive Enhancement Indicator
06/21/2012 Venkata Vempali              Added IsECI,IsMissingParts S18, R5.5  
06/22/2012 Somesh Velupalli	    Added Enhancement Indicators S18  
06/25/2012 Matt Stelter		    Added IS Negotiated Sale S18   
06/25/2012 Venkata Vempali              Updated join for StockCancelReasonDescription S18     
06/29/2012 Somesh velupalli		Added ISRun and Drive Enhancement
06/29/2012 Venkata Vempali				Updated IsECI to use Salvage_Event_history table    
07/02/2012 Sidd Murao           Updated Logic for Branch Transfer Ind 
07/05/2012 Somesh Velupalli     Added code to look up status codes for Enhancement Ind 
07/06/2012 Sidd Murao	        Changed order of insert statement 
07/09/2012 Sidd Murao           Changed logic for OriginalBranchNumber
07/10/2012 Somesh Velupalli     Added IsIBFDisplay and HadIBFDisplay
07/10/2012 Sidd Murao           Changed logic for ProviderTransfer
07/11/2012 Sidd Murao           Changed logic for Branch Transfer
07/20/2012 Somesh Velupalli     Added code for IBF renege Ind 
07/22/2012 Matt Stelter			Changed var from Date to Datetime for Min bid source
08/03/2012 Somesh Velupalli     Moved IBF Indicator's update to run after IsIBFSold update. (#TMP_SPI_Data)
                                And updated case stmt for IsIBFQualified
08/06/2012 Venkata Vempali              Updated logic for MinimumBidSource S21 R5.6.1
08/06/2012 Venkata Vempali              Added code to Fix ETL for DamageCode's Primary & Secondary  S21 R5.6.1	
08/07/2012 Venkata Vempali              Updated to use [EDW].[dbo].[CSATAutoMinimumBidUpdateHistoryNew]
									instead of 	[BI_CSA_Rep].[dbo].[csat_Auto_MinimumBid_Update_History_New]
8/31/2012  Matt Stelter				Changed IsNoRowSlot logic														    
9/10/2012  Matt Stelter				Changed IsEnhancement logic	
9/12/2012  Venkata Vempali	            Updated cdc Grossreturn Salvage_ID's and kept ISNULL condition.  
10/03/2012 Venkata Vempali              Added coalesce for CountryOfManufacture ,FullBodyStyle ,StockSegment (inorder not to update it to blank when it a has value before.)
10/04/2012	Matt Stelter				Fix for Provider Tow Miles (InboundTowMiles)
10/05/2012 Venkata Vempali              Fix for IsRunAndDriveAuction  --Updating from #TMP_SC_Data
10/05/2012 Venkata Vempali              Fix for IsBuyerCancelAfterSettlement ,IsBuyerRenegedAfterSettlement. CDC for #TMP_SEH_Data_SalvageIDs Should also include FSEN & STLV Sprint-25
10/10/2012  Matt Stelter				Fix for ACV update after cancel
10/22/2012 Somesh Velupalli     Added logic for IsKeyEnhancement Ind
10/25/2012 Venkata Vempali              Added logic for StorageLocationID and Updated logic for SellFromStorageLocationID S27 R5.9
10/30/2012 Somesh Velupalli     Added logic for Enhancement Indicators to look at charges too
10/31/2012	Matt Stelter		Fix for bad Aisle characters
11/09/2012  Somesh Velupalli    Added logic for enhancement indicators to look at Expenses too
01/21/2013	Somesh Velupalli    Fix for AirbagMissing and AirbagDeployed Ind
1/23/2013	Matt Stelter		Fix for IsResidence
1/24/2013	Matt Stelter		Fix for Polk
1/30/2013	Matt Stelter		Fix for Indicators
2/6/2013    Somesh Velupalli    ETL for SpecialtycategoryDescription(Specailty Vehicle Mix)
2/8/2013    Somesh Velupalli    ADD Scooter and ATV in SpecialtycategoryDescription
2/18/2013   Venkata Vempali     Added ETL code for Zero_Deficit_Adjustment_Amount ,Zero_Deficit_Settlement_Ind ,Min_Remit_Discount ,Min_Remit_Ind UserStory: 21360 ,S35
4/12/2013   Venkata Vempali     Added logic for IsMinimumBidOutlier and commented it in usp_ETLDimStock_Update
4/17/2013	Matt Stelter		Added AdvanceStorageInfoID to table/AssignmentSource
4/29/2013	Matt Stelter		Added Assignment Method back
5/16/2013	Matt Stelter		Added AlternatePayeeID, CertState, EngineInformation,TransmissionType,FuelType,DriveLineType for CSA Today, IsFastTrack
5/21/2013	Matt Stelter		Added ProviderRefund amount
6/10/2013	Matt Stelter		Changed IBF SP Approved to show 0 if PA Stock
09/3/13		John Stires			Added Updates from Polk to Make and Model columns
9/4/2013	Matt Stelter		Changed VIN Decode process
09/3/13		John Stires			Added Updates from Polk columns:	FullBodyStyle, EVMBaseShippingWeight, SeriesName,
																	 EVMCylindersDescription, ASAPDriveLineType, ASAPFuelType,
																	 EVMVehicle_Class
10/07/13	John Stires			Added Updates for name changes to Adjustors.
10/22/13	John Stires			Added Updates for name changes to Assignors and ClaimHandlers.
10/22/2013	Matt Stelter		Added update for IsEVMUpdated
10/25/2013	John Stires			Added AssignmentOrigin
11/11/2013	Matt Stelter		Moved Min Bid User to usp_DimStock_Update , Changed Is Reneged/Cancelled After Settle to include the code "SAL1" so it reruns the code
11/19/2013	Juan X. Jacome		Added IsOdoProbed
12/05/2013	Matt Stelter		Added AFCFlooringSource
12/10/2013	Matt Stelter		Added BuyerFinanceAmount
02/11/2014  Bruce Kutnick		Added RecommendedSaleDocState, RecommendedSaleDocType, RecommendedSaleDocBrand
02/21/2014  Bruce Kutnick		Set RecommendedSaleDocState, RecommendedSaleDocType, RecommendedSaleDocBrand to BLANK (non NULL and not 'NONE') when no value exists
02/21/2014  Bruce Kutnick		Modified the subquery that obtains RecommendedSaleDocState, etc so that The RecommendedSaleDoc State will default to the state of the Admin
                                Branch if there is no value.
03/20/2014	Bruce Kutnick		Modified #TMP_SExpense_Data toinsure we get the PA_Price on the LATEST Salvage Expense For Charge Type 315.
03/25/2014  Juan X. Jacome		--Update logic to not load stock from ASAP to EDW that are in the LKExcludeASAPSalvageID table.
6/2/2014	Matt Stelter		Added ClaimKey
6/6/2014	Matt Stelter		Added AdjusterPhone	
10/03/2014	Bruce Kutnick		Modified code for LossTypeDescription so as to duplicate the CSA Today logic as part of DW Merge. 
10/03/2014	Bruce Kutnick		Modified code for Sale_Document_Type_Description so as to duplicate the CSA Today logic as part of DW Merge. 
10/27/2014	Bruce Kutnick		Modified section of code for Salvage Settlement so that VOD (void) settlements are not included.
								This will mirror CSA Today logic for DW Merger (see section  '#TMP_Stock_Settlement_Unit_Data')
11/25/2014	Matt Stelter		Fix for Secondary Damage Code
12/3/2014	Matt Stelter		Fixed Title State
12/16/2014	Matt Stelter		Adding IOS App to AssignmentMethod
01/16/2015	Bruce Kutnick		Added EnhancementLevelID for Vehicle Inspection Services tracking purposes.
01/19/2015  Bruce Kutnick		Modified EnhancementLevelID so that a NULL value is replaced with '' instead of 0.
01/22/2015	Bruce Kutnick		Removed join to CDC table (Salvage Enhancement Level) and replaced with a join to #TMP_Stock_Data_Stage
01/28/2015  Bruce Kutnick		(Temporary) Remove reference to new objects, EnhancementLevelId and InspectionServiceLevel (appr on rows 2709 and 3783) until ASAP changes are 
								fully implemented
01/30/2015	Bruce Kutnick		Added back new objects removed on 1-28-15.
02/05/2015	Bruce Kutnick		For Inspection Service Level code modification: for those records that have no default value(which we set to default to 'None'), exclude 
								Tow Only and Title Work Only Assignment Type Codes 
03/12/2015	Bruce Kutnick		Added logic for trapping Assignment Method of ANDROID.
03/19/2015  Bruce Kutnick		Modified CASE Statement order for AssignmentMethod and changed description of IOS from IOS APP to IOS.
03/20/2015  Yogitha Alwarsetty  Added NetSplitAdjustment and NetSplitAdjustmentInd
4/27/2015	Juan X. Jacome		Update logic for Inspection Services fields
05/04/2015	Bruce Kutnick		Added IsASAPCAT attribute
5/18/2015	Matt Stelter		Fixed Loss Type Description
6/1/2015	Juan X. Jacome		Removed IsASAPCAT attribute, moved to etlDimStockUpdate 
6/5/2015   Yogitha Alwarsetty   Added Sale_Document_Note
6/15/2015	Matt Stelter		Changed to use EVM Reporting Values
7/1/2015	Matt Stelter		RTRIM for Make/Model and changed ASAP_ModelSource
7/8/2015	Matt Stelter		Added MFG Origin
7/15/2015	Matt Stelter		Fix for Stock Segment
9/10/2015	Matt Stelter		Fix for Assignor Last Name
10/20/2015	Juan X.Jacome		Added table alias for Run_And_Drive_Ind for INSERT INTO #TMP_RunDriveInd
11/3/2015	Juan X. Jacome		Update logic for Inspection Service Level
1/12/2016	Matt Stelter		Change for EnhancementLevelID = 3
1/12/2016	Matt Stelter		SoldModelYearBand2
2/2/2016	Matt Stelter		Change for Run and Drive at Auction
4/12/2016   Pratyusha Koduru    Added PreviousProcessStatusDesc
4/18/2016	Matt Stelter		Change EVM to not set IsEVMUpdated if not true
4/28/2016	Matt Stelter		LEFT 25 for EVM Model
5/12/2016  Yogitha Alwarsetty   Modified logic for IsInspectionServiceCancelled,IsInspectionServiceReportsCancelled
5/12/2016  Yogitha Alwarsetty    Added IsInspectionServiceAppraisalCancelled
7/28/2016  Yogitha Alwarsetty    Modified ModelYear Logic
8/04/2016  Yogitha Alwarsetty   Modified SoldModelYearAge,SoldModelYearBand,SoldModelYearBand2
11/12/2016	MAtt Stelter		Changes to EVM process to pick up Country of Origin
11/4/2016  Yogitha Alwarsetty   Added VehicleGrade
11/14/2016 Pratyusha Koduru     Updated IsHadTitleProblem Logic
===================================================================================================================================================================
Usage Example:
Exec usp_ETLDimStock
===================================================================================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimStock]

AS     

DECLARE @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		,@ASAPSalvageID	Int
	,	@Vin			nchar(17)
	,	@Make			nvarchar(35)
	,	@Model			nvarchar(25)
	,	@RowCtr			int
			
SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLDimStock'
		
--Get the Min and Max LSN's
SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'


/***************************************************************************************
Create all temp tables and Insert the values from the CDC Current into the temp tables.
***************************************************************************************/

--DECLARE @MinLSN Binary(10)
--		,@MaxLSN Binary (10)

--SELECT @MinLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Salvage')
--SELECT @MaxLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()

 --Gets the data from Salvage table using the CDC function and inserts it into a Temp table. 

CREATE TABLE #TMP_Stock_Data_Stage(
	[StockNumber] [int] NULL,
	[ASAPSalvageID] [int] NULL,
	[StorageLocationID] [int] NULL,
	[AffiliateId] [int] NULL,
	[AssignorBranchNumber] [int] NULL,
	[AdministrativeBranchNumber] [int] NULL,
	[AssignmentTypeCode] [char](3) NULL,
	[AssignmentTypeDescription] [varchar](40) NULL,
	[BranchNumber] [int] NULL,
	[BodyStyleName] [varchar](30) NULL,
	[ColorDescription] [varchar](20) NULL,
	[DamageSeverityCode] [char](2) NULL,
	[DamageTypeCode] [char](2) NULL,
	[ExteriorColorCode] [char](2) NULL,
	[FullStockNumber] [varchar](16) NULL,
	[KeysRetainedCode] [smallint] NULL,
	[KeysRetainedCodeatSale] [smallint] NULL,
	[ModelName] [varchar](25) NULL,
	[ASAP_ModelName] [varchar](25) NULL,
	[ModelYear] [smallint] NULL,
	[Mileage] [int] NULL,
	[OdometerReadingTypeCode] [int] NULL,
	[OdometerReadingTypeDescription] [varchar](40) NULL,
	[VIN] [char](17) NULL,
	[VINSerialNumber] [char](6) NULL,
	[SeriesName] [varchar](25) NULL,
	[SaleRunCount] [int] NULL,
	[IsCallForSaleAuthorization] [bit] NULL,
	[IsHybrid] [bit] NULL,
	[IsSpecialty] [bit] NULL,
	[IsStockSPE] [bit] NULL,
	[IsTowable] [bit] NULL,
	[IsVIC] [bit] NULL,
	[IsDeliveredToBranch] [bit] NULL,
	[StockCancelReasonCode] [char](3) NULL,
	[StockCancelReasonDescription] [varchar](40) NULL,
	[StockType] [char](5) NULL,
	[StockTypeDescription] [varchar](40) NULL,
	[SpecialtyCategoryDescription] [varchar] (40) NULL,
	[StockSubType] [char](5) NULL,
	[StockSubTypeDescription] [varchar](40) NULL,
	[AssignorUserID] [int] NULL,
	[AssignorFirstName] [varchar](30) NULL,
	[AssignorLastName] [varchar](30) NULL,
	[IsPriorityTow] [bit] NULL,
	[StockHoldReasonCode] [char](3) NULL,
	[StockHoldReasonDescription] [varchar](40) NULL,
	[IsReportedToNICB] [int] NULL,
	[IsCancel] [tinyint] NULL,
	[ASAP_MakeName] [varchar](35) NULL,
	[MakeName] [varchar](35) NULL,
	[FullBodyStyle] [varchar](40) NULL,
	[CountryOfManufacture] [varchar](20) NULL,
	[StockSegment] [varchar](20) NULL,
	[MotorCycleSegment] [varchar](20) NULL,
	[SoldKeysApplicableInd] [bit] NULL,
	[IsValidMileage] [bit] NULL,
	[AssignmentMethod] [varchar](10) NULL,
	[OriginalBranchNumber] int Null ,
	[OriginalProviderID] int Null,
	EngineInformation varchar(30) NULL,
	TransmissionType VARCHAR(45) NULL, 
	FuelType VARCHAR(200) NULL, 
	DriveLineType VARCHAR(3) NULL,
	ISFastTrack TINYINT NULL,
	AssignmentOrigin nvarchar(50) NULL,
	IsOdoProbed TINYINT NULL,
	RecommendedSaleDocState	VARCHAR(4) NULL,
	RecommendedSaleDocType	VARCHAR(40)NULL,
	RecommendedSaleDocBrand	VARCHAR(40) NULL,

	[EVMBaseShippingWeight] [nvarchar](4),
	[EVMCylindersDescription] [varchar](6) NULL,
	[EVMVehicleClass] [varchar](200) NULL,
	[IsEVMUpdated] [tinyint] NULL DEFAULT ((0)),
	ManufacturerOrigin VARCHAR (50) NULL  ,
	[VehicleGrade] Varchar(200) NULL
	)

	CREATE TABLE #TMP_CT_Salavage_Provider_Original
([asapsalvageid]                             [INT] NOT NULL,
	[OriginalProviderID]                   [INT] NULL,
	ran                                      [INT] NOT NULL
)

CREATE TABLE #TMP_CT_Salavage_Event_REAB_BT
([STOCKID]                             [INT] NOT NULL
)

CREATE TABLE #TMP_CT_Salavage_Event_IBTR_IBND_BT
([STOCKID]                             [INT] NOT NULL,
	[Event_DateTime]                        [DATETIME] NULL
)


CREATE TABLE #TMP_CT_Salavage_Event_IBCN_BT
([STOCKID]                             [INT] NOT NULL,
	[Event_DateTime]                        [DATETIME] NULL
)




CREATE TABLE #TMP_CT_Salavage_Original_Update
([asapsalvageid]                             [INT] NOT NULL,
	[OriginalBranchNumber]                   [INT] NULL,
	ran                                      [INT] NOT NULL
)

CREATE TABLE #TMP_VINDecodeReturn	(
	AirConditioning				VARCHAR(500),
	AntilockBrakes				VARCHAR(500),
	Base_Shipping_Weight		VARCHAR(500),
	Body_Style_Name				VARCHAR(500),
	Body_Type_Code				VARCHAR(500),
	BrakesType					VARCHAR(500),
	CabConfiguration			VARCHAR(500),
	Carburetion					VARCHAR(500),
	Check_Digit					VARCHAR(500),
	CompletePrefixFile			VARCHAR(500),
	CountryOfOrigin				VARCHAR(500),
	CubicInchDisplacement		VARCHAR(500),
	Cylinders					VARCHAR(500),
	Cylinders_Description		VARCHAR(500),
	DaytimeRunningLights		VARCHAR(500),
	Drive_Line_Type				VARCHAR(500),
	Drive_Line_Type_Code		VARCHAR(500),
	ElectricBatteryInfoKW		VARCHAR(500),
	ElectricBatteryInfoType		VARCHAR(500),
	ElectricBatteryInfoVolts	VARCHAR(500),
	EngineInfoHighOutput		VARCHAR(500),
	EngineInfoProprietary		VARCHAR(500),
	EngineInfoSupercharged		VARCHAR(500),
	EngineInfoTurbocharged		VARCHAR(500),
	EngineInfoVVTL				VARCHAR(500),
	EngineInformation			VARCHAR(500),
	EngineManufacturer			VARCHAR(500),
	EngineModel					VARCHAR(500),
	EngineType					VARCHAR(500),
	FrontAxleType				VARCHAR(500),
	Fuel_Type					VARCHAR(500),
	Fuel_Type_Code				VARCHAR(500),
	GVWCycles					VARCHAR(500),
	HighPerformanceType			VARCHAR(500),
	Hybrid_Ind					VARCHAR(500),
	IsFullVinDetails			VARCHAR(500),
	LocationIndicator			VARCHAR(500),
	Make						VARCHAR(500),
	Model						VARCHAR(500),
	Model_Year					VARCHAR(500),
	NCIC						VARCHAR(500),
	NVPP						VARCHAR(500),
	OptionalRadio1				VARCHAR(500),
	OptionalRadio2				VARCHAR(500),
	OptionalRoof1				VARCHAR(500),
	OptionalRoof2				VARCHAR(500),
	OptionalTransmission1		VARCHAR(500),
	OptionalTransmission2		VARCHAR(500),
	PowerBrakes					VARCHAR(500),
	PowerSteering				VARCHAR(500),
	PowerWindows				VARCHAR(500),
	Radio						VARCHAR(500),
	RearAxleType				VARCHAR(500),
	Restraint_Type				VARCHAR(500),
	Roof						VARCHAR(500),
	Salvage_Type				VARCHAR(500),
	SecuritySystem				VARCHAR(500),
	Segment						VARCHAR(500),
	Segmentation_Code			VARCHAR(500),
	Segmentation_Description	VARCHAR(500),
	Series_Name					VARCHAR(500),
	TiltWheel					VARCHAR(500),
	TireSize					VARCHAR(500),
	TonRating					VARCHAR(500),
	TrailerBodyStyleType		VARCHAR(500),
	TrailerLength				VARCHAR(500),
	TrailerNumberofAxles		VARCHAR(500),
	TransmissionCode			VARCHAR(500),
	TransmissionSpeed			VARCHAR(500),
	TransmissionSpeedCode		VARCHAR(500),
	TransmissionType			VARCHAR(500),
	Transmission_Type_Code		VARCHAR(500),
	Transmission_Type_Description VARCHAR(500),
	Truck_Axles					VARCHAR(500),
	Vehicle_Class				VARCHAR(500),
	Vehicle_Type				VARCHAR(500),
	WeightVariance				VARCHAR(500),
	WheelBase					VARCHAR(500)	)

CREATE TABLE #VINDecodeSourceExtract	(
--StockID					int NOT NULL,
		  ASAPSalvageID				int NULL
		, Vin						nchar (30) NULL
		, Make						NVARCHAR(500)
		, Model						NVARCHAR(500)
		, Body_Style_Name			NVARCHAR(500)
		, Base_Shipping_Weight		NVARCHAR(500)
		, Series_Name				NVARCHAR(500)
		, Cylinders_Description		NVARCHAR(500)
		, Drive_Line_Type			NVARCHAR(500)
		, Fuel_Type					NVARCHAR(500)
		, VehicleClass				NVARCHAR(500)
		, Segment					VARCHAR(500)
		, CountryOfOrigin			VARCHAR(500)
													)

CREATE TABLE #TMP_User	(
        User_ID			INT NOT NULL
      , User_Name		varchar (40) NULL
	  , EffectiveDayID	int NULL
      , Effective_Date	datetime NULL
      , ExpirationDayID	int NULL
      , Expiration_Date	datetime NULL
      , First_Name		varchar (30) NULL
      , Middle_Initial	char (1) NULL
      , Last_Name		varchar (30) NULL
	  , AdjustorPhone	Varchar(11)	)


CREATE TABLE #TMP_Enhancement_Salvage_IDs_InspectionServices(
		Salvage_ID int,
		Salvage_Enhancement_Level_ID int,
		Enhancement_Level_ID int
		)

CREATE TABLE #TMP_InspectionserviceIndicators
(	
Salvage_ID int,
[IsInspectionServiceAppraisalCancelled]  [tinyint] NOT NULL DEFAULT ((0)),
[IsInspectionServiceCancelled]  [tinyint] NOT NULL DEFAULT ((0)),
[IsInspectionServiceReportsCancelled]  [tinyint] NOT NULL DEFAULT ((0))
)
--==========================================================================================
INSERT INTO #TMP_InspectionserviceIndicators (Salvage_ID ,[IsInspectionServiceAppraisalCancelled] ,
[IsInspectionServiceCancelled] ,[IsInspectionServiceReportsCancelled])
select Distinct SN.Salvage_ID,

Max(Case when Note_Text IN(

'Inspection Services Appraisal enhancement marked cancelled.'

) then 1 else 0 end ) as [IsInspectionServiceAppraisalCancelled],

Max(Case when Note_Text IN(
'Inspection Services Photos enhancement marked cancelled.'

) then 1 else 0 end) as [IsInspectionServiceCancelled],

Max(Case when Note_Text IN(
'Inspection Services Report enhancement marked cancelled.'

) then 1 else 0 end ) as [IsInspectionServiceReportsCancelled]

from BI_ASAP_REP.[dbo].[Salvage_Note] SN with (NOLOCK)
INNER JOIN (select Distinct Salvage_ID  from [BI_ASAP_Rep].dbo.Salvage_Enhancement_Level
where Update_DateTime between (select MinDatetime from  dbo.ETLRun WHERE ETLJobName = 'DailyETLJob') and (select MaxDatetime from  dbo.ETLRun WHERE ETLJobName = 'DailyETLJob')) AS cdc_SNN
On SN.Salvage_ID=cdc_SNN.Salvage_ID

Group by SN.Salvage_ID

UPDATE ISI
SET [IsInspectionServiceAppraisalCancelled] = (CASE WHEN SN.Note_Text IN(
'Service level updated from Empty to Images & Report & Appraisal',
'Service level updated from Images & Report to Images & Report & Appraisal',
'Service level updated from Images to Images & Report & Appraisal',
'Service level updated from None to Images & Report & Appraisal'
)THEN 0 ELSE ISI.[IsInspectionServiceAppraisalCancelled] END ),

 [IsInspectionServiceCancelled] = (CASE WHEN  SN.Note_Text IN(
'Service level updated from Empty to Images',
'Service level updated from Empty to Images & Report',
'Service level updated from Empty to Images & Report & Appraisal',
'Service level updated from Images & Report & Appraisal to Images',
'Service level updated from Images & Report & Appraisal to Images & Report',
'Service level updated from Images & Report to Images',
'Service level updated from Images & Report to Images & Report & Appraisal',
'Service level updated from Images to Images & Report',
'Service level updated from Images to Images & Report & Appraisal',
'Service level updated from None to Images',
'Service level updated from None to Images & Report',
'Service level updated from None to Images & Report & Appraisal'
)THEN 0 ELSE ISI.[IsInspectionServiceCancelled] end ),

  [IsInspectionServiceReportsCancelled]= (CASE WHEN SN.Note_Text IN(

'Service level updated from Empty to Images & Report',
'Service level updated from Empty to Images & Report & Appraisal',
'Service level updated from Images & Report & Appraisal to Images & Report',
'Service level updated from Images & Report to Images & Report & Appraisal',
'Service level updated from Images to Images & Report',
'Service level updated from Images to Images & Report & Appraisal',
'Service level updated from None to Images & Report',
'Service level updated from None to Images & Report & Appraisal'
) THEN 0 ELSE ISI.[IsInspectionServiceReportsCancelled] END)

from #TMP_InspectionserviceIndicators ISI with (NOLOCK)
Join BI_ASAP_REP.[dbo].[Salvage_Note] SN with (NOLOCK)
ON ISI.Salvage_ID=SN.Salvage_ID
Join (select salvage_ID,Max(update_DateTime)update_DateTime
From BI_ASAP_REP.[dbo].[Salvage_Note]
where  Note_Text IN(
'Service level updated from Empty to Images',
'Service level updated from Empty to Images & Report',
'Service level updated from Empty to Images & Report & Appraisal',
'Service level updated from Images & Report & Appraisal to Images',
'Service level updated from Images & Report & Appraisal to Images & Report',
'Service level updated from Images & Report & Appraisal to None',
'Service level updated from Images & Report to Images',
'Service level updated from Images & Report to Images & Report & Appraisal',
'Service level updated from Images & Report to None',
'Service level updated from Images to Images & Report',
'Service level updated from Images to Images & Report & Appraisal',
'Service level updated from Images to None',
'Service level updated from None to Images',
'Service level updated from None to Images & Report',
'Service level updated from None to Images & Report & Appraisal'
)Group by salvage_ID)a
on a.Salvage_ID=sn.Salvage_ID
and a.update_DateTime=sn.Update_DateTime

CREATE UNIQUE CLUSTERED INDEX IX_SL_Salvage_Note_ASAPSalvageID
    ON #TMP_InspectionserviceIndicators (Salvage_ID) 
	with fillfactor = 100; 


UPDATE DS
SET 
[IsInspectionServiceAppraisalCancelled] =ISNULL(ISI.IsInspectionServiceAppraisalCancelled,0),
[IsInspectionServiceCancelled] =ISNULL(ISI.[IsInspectionServiceCancelled],0),
[IsInspectionServiceReportsCancelled]=ISNULL(ISI.[IsInspectionServiceReportsCancelled],0)
FROM Edw.dbo.dimstock DS WITH (NOLOCK)
Inner JOIN #TMP_InspectionserviceIndicators ISI WITH (NOLOCK)
ON DS.[ASAPSalvageID] = ISI.[salvage_id]


INSERT INTO #TMP_CT_Salavage_Original_Update (asapsalvageid ,OriginalBranchNumber, ran )
SELECT	SC1.salvage_id  AS salvage_id ,
		SC1.Administrative_Branch_Number,
		RANK () over (partition by  SC1.salvage_id order by SC1.update_datetime) ran 
FROM [BI_ASAP_Rep].cdc.dbo_salvage_ct AS SC1  WITH (NOLOCK)
INNER JOIN [BI_ASAP_Rep].cdc.dbo_salvage_ct  As SC2  WITH (NOLOCK)
	ON ( SC1.__$seqval = SC2.__$seqval
		AND SC1.__$operation = 3
		AND SC2.__$operation = 4
		AND SC1.Administrative_Branch_Number <> SC2.Administrative_Branch_Number)
INNER JOIN [BI_ASAP_Rep].cdc.dbo_salvage_event_history_ct SHE  WITH (NOLOCK)
	on (SHE.salvage_id = SC1.salvage_id)
INNER JOIN dimstock ds with (nolock) 
	on (ds.asapsalvageid = she.salvage_id and salvage_event_code in ('REAB','IBND','IBTR') and IsBranchTransfer =0 )
GROUP BY SC1.Salvage_id,SC1.Administrative_Branch_Number,SC1.update_datetime

INSERT INTO #TMP_Stock_Data_Stage([StockNumber],[ASAPSalvageID],[StorageLocationID],[AffiliateId]
,[AssignorBranchNumber],[AdministrativeBranchNumber],[AssignmentTypeCode],[AssignmentTypeDescription]
,[BranchNumber],[BodyStyleName],[ColorDescription],[DamageSeverityCode],[DamageTypeCode],[ExteriorColorCode]
,[FullStockNumber],[KeysRetainedCode],[KeysRetainedCodeatSale],[ModelName],[ASAP_ModelName],[ModelYear],[Mileage],[OdometerReadingTypeCode]
,[OdometerReadingTypeDescription],[VIN],[VINSerialNumber]
,[SeriesName],[SaleRunCount],[IsCallForSaleAuthorization],[IsHybrid],[IsSpecialty],[IsStockSPE],[IsTowable],[IsVIC]
,[IsDeliveredToBranch],[StockCancelReasonCode],[StockCancelReasonDescription],[StockType],[StockTypeDescription]
,[SpecialtyCategoryDescription]
,[StockSubType],[StockSubTypeDescription],[AssignorUserID],[AssignorFirstName],[AssignorLastName],[IsPriorityTow],[StockHoldReasonCode]
,[StockHoldReasonDescription],[IsReportedToNICB],[IsCancel]
,MakeName ,[ASAP_MakeName],[SoldKeysApplicableInd],[IsValidMileage],[AssignmentMethod],[OriginalBranchNumber]
,[OriginalProviderid],EngineInformation,TransmissionType,FuelType,DriveLineType,IsFastTrack,AssignmentOrigin,IsOdoProbed,
RecommendedSaleDocState,
RecommendedSaleDocType,
RecommendedSaleDocBrand,
[VehicleGrade] 
)

SELECT   S.[Stock_Number]
		,S.[Salvage_ID]
		,ISNULL(S.[Sell_From_Storage_Location_ID],0) AS Storage_Location_ID
		,ISNULL(S.[Affiliate_ID],0) AS Affiliate_ID
		,ISNULL(S.[Assignor_Branch_Number],0) AS Assignor_Branch_Number
		,ISNULL(S.[Administrative_Branch_Number],0) AS Administrative_Branch_Number
		,ISNULL(S.[Assignment_Type_Code],'') AS Assignment_Type_Code
		,ISNULL(AT.[Assignment_Type_Description],'') AS Assignment_Type_Description
		,ISNULL(S.[Branch_Number],0) AS Branch_Number
		,ISNULL(S.[Body_Style_Name],'') AS Body_Style_Name
		,ISNULL(C.[Color_Description],'') AS Color_Description
		,ISNULL(S.[Damage_Severity_Code],'') AS Damage_Severity_Code
		,ISNULL(S.[Damage_Type_Code],'') AS Damage_Type_Code
		,ISNULL(S.[Exterior_Color_Code],'') AS Exterior_Color_Code
		,CAST((CASE WHEN LEN(S.Branch_Number) = 3 
						THEN CAST(S.Branch_Number AS CHAR(3))
					 ELSE CAST('000' AS CHAR(3))END) AS CHAR(3)) + '-' 
				+ RIGHT('00000000' + CAST(S.Stock_Number AS VARCHAR(20)),8) AS Full_Stock_Number	
		,[Keys_Retained_Ind]
		,[Keys_Retained_Ind] AS Keys_Retained_Ind_Sale
		,ISNULL(S.[Model_Name],'') AS Model_Name
		,ISNULL(S.[Model_Name],'') AS ASAP_Model_Name
		,CASE WHEN S.[Model_Year] < 1900 
				THEN YEAR(GETDATE())-9
			  WHEN S.[Model_Year] > YEAR(GETDATE()) + 2
				THEN YEAR(GETDATE())
			  ELSE ISNULL(S.[Model_Year],0)
			END AS Model_Year
		,ISNULL(S.[Mileage],0) AS Mileage
		,ISNULL(S.Odometer_Reading_Type_Code,8) AS Odometer_Reading_Type_Code
		,ISNULL(ORT.[Odometer_Reading_Type_Description],'Unknown') AS Odometer_Reading_Type_Description
		,ISNULL(S.[VIN],'') AS VIN
		,ISNULL(S.[VIN_Serial_Number],'') AS VIN_Serial_Number
		,ISNULL(S.[Series_Name],'') AS Series_Name
		,ISNULL(S.[Sale_Run_Count],0) AS Sale_Run_Count
		,S.[Call_for_Sale_Authorization_Ind]
		,ISNULL(SVi.Hybrid_Ind,0) AS Hybrid_Ind
		,S.[Specialty_Ind]
		,S.[Settlement_Packet_Express_Ind]
		,S.[Towable_Ind]
		,S.[VIC_Ind]
		,S.[Delivered_To_Branch_Ind]
		,ISNULL(S.[Salvage_Cancel_Reason_Code],'') AS Salvage_Cancel_Reason_Code
		,ISNULL(SR.[Salvage_Cancel_Reason_Description],'') AS Salvage_Cancel_Reason_Description
		,ISNULL(S.[Salvage_Type],'') AS Salvage_Type
		,ISNULL(ST.[Salvage_Description],'') AS Salvage_Description
		,ISNULL(CASE WHEN ST.[Salvage_Description] IN ('Boat','Automobile','Other','Snowmobile','Personal Watercraft','Motorcycle','Scooter','ATV') THEN 'Other'
		             WHEN ST.[Salvage_Description] IN ('Heavy Equipment','Crane') THEN 'Crane/Heavy Equipment'
		             WHEN ST.[Salvage_Description] IN ('Farm Equipment','Forestry Equipment') THEN 'Farm/Forestry'
		             WHEN ST.[Salvage_Description] IN ('Motor Home','Travel Trailer') THEN 'Motor Home/Travel Trailer'
		             WHEN ST.[Salvage_Description] IN ('Emergency Equipment','Bus') THEN 'Municipal'
		             WHEN ST.[Salvage_Description] IN ('Trailer') THEN 'Trailer'
                   WHEN ST.[Salvage_Description] IN ('Truck') THEN 'Truck' END,'') AS SpecialtyCategoryDescription
		,ISNULL(S.Salvage_Sub_Type,'') AS Salvage_Sub_Type	
		,ISNULL(SST.Salvage_Sub_Type_Description,'') AS Salvage_Sub_Type_Description	
		,ISNULL(S.Assignor_User_ID,0) AS Assignor_User_ID
		,ISNULL (DU1.[FirstName],'') AS Assignor_First_Name
		,ISNULL (DU1.[LastName],'') AS Assignor_Last_Name
		,S.[Priority_Tow_Ind] 
		,ISNULL(S.Salvage_Hold_Reason_Code,'') AS Salvage_Hold_Reason_Code
		,ISNULL(Salvage_Hold_Reason_Description,'') AS Salvage_Hold_Reason_Description
		,S.Reported_To_NICB_Ind
		,S.Cancel_Salvage_Ind
		,S.[Make_Name] AS MakeName
		,S.[Make_Name] AS ASAP_MakeName
		,CASE WHEN Keys_Retained_IND IN (1, 0) 
         THEN 1  ELSE 0 END AS SoldKeysApplicableInd
		,CASE WHEN (S.[Mileage] BETWEEN 1 and 299999) AND  S.Odometer_Reading_Type_Code = 0
		  THEN 1 ELSE 0 END AS IsValidMileage
		,'' As AssignmentMethod ,
		0, 0,S.Engine_Information,
		svi.Transmission_Description,
		svi.Fuel_Type_Description,
		svi.Drive_Line_Type_Description,
		S.FastTrack_Ind,
		ct.AlphaID	AS AssignmentOrigin,
		ISNULL(S.Odo_Probed_Ind,0) AS IsOdoProbed,
		[docsale].RecommendedSaleDocState,
		[docsale].RecommendedSaleDocType,
		[docsale].RecommendedSaleDocBrand,
		Xref.Vehicle_Grade_Description
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](
   @MinLSN,@MaxLSN,'all with merge') AS S 
   	INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Type AS ST WITH (NOLOCK)
		ON S.Salvage_Type = ST.Salvage_Type
	LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Odometer_Reading_Type	AS ORT WITH (NOLOCK) 
		ON S.Odometer_Reading_Type_Code =  ORT.Odometer_Reading_Type_Code
	LEFT OUTER JOIN [BI_ASAP_Rep].dbo.[Assignment_Type] AS AT WITH (NOLOCK)
		ON S.Assignment_Type_Code = AT.Assignment_Type_Code
	LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Color AS C WITH (NOLOCK)
		ON S.Exterior_Color_Code =  C.Color_Code
	LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Salvage_Cancel_Reason SR WITH(NOLOCK)
		ON S.Salvage_Cancel_Reason_Code = SR.Salvage_Cancel_Reason_Code	
	LEFT OUTER JOIN [BI_ASAP_REP].dbo.Salvage_Vehicle_Info AS SVI WITH (NOLOCK)
			ON S.Salvage_Id = SVI.Salvage_Id
	LEFT OUTER JOIN dbo.DimUser AS DU1 with (NOLOCK)
			ON S.Assignor_User_ID = DU1.[UserID]
	LEFT OUTER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Hold_Reason] AS SHR WITH (NOLOCK)
			ON SHR.Salvage_Hold_Reason_Code = S.Salvage_Hold_Reason_Code
	LEFT OUTER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Sub_Type] AS SST WITH (NOLOCK)
			ON S.Salvage_Type = SST.Salvage_Type AND S.Salvage_Sub_Type = SST.Salvage_Sub_Type
    Left Outer Join BI_ASAP_REP.[dbo].[Vehicle_Grade_Xref] Xref
            on s.Salvage_VehicleGrade=xref.Vehicle_Grade_value
	LEFT OUTER Join	CodesTable AS ct WITH (NOLOCK)
			ON ISNULL(S.Origin_ID, 0) = ct.NumericID and ct.CodeType = 'AssignmentOrigin'
	LEFT OUTER JOIN
			(
				SELECT	ci.Salvage_ID,
					CASE
						WHEN  sdt.State_Abbreviation IS NULL
							THEN db.StateAbbreviation
						ELSE sdt.State_Abbreviation
					END AS RecommendedSaleDocState,
					CASE
						WHEN sdt.Sale_Document_Type IS NULL
							THEN ''
						ELSE sdt.Sale_Document_Type
					END AS  RecommendedSaleDocType,
					CASE
						WHEN ci.[Recommended_Sale_Document_Brand] IS NULL
							 THEN ''
						ELSE ci.[Recommended_Sale_Document_Brand]
					END AS RecommendedSaleDocBrand,
					db.BranchName,
					db.BranchNumber,
					s.Branch_Number
			FROM	[BI_ASAP_Rep].[dbo].[Salvage_Checkin] ci WITH (NOLOCK)
					INNER JOIN [BI_ASAP_Rep].[dbo].[Salvage] s WITH (NOLOCK)
						ON s.Salvage_ID = ci.Salvage_ID
					INNER JOIN dbo.DimBranch db WITH (NOLOCK)
						ON db.BranchNumber = s.Administrative_Branch_Number
					LEFT OUTER JOIN	[BI_ASAP_Rep].[dbo].sale_document_Type sdt WITH (NOLOCK)
						ON sdt.Sale_Document_Type_ID = ci.Recommended_Sale_Document_Type_ID
			) AS [docsale]
		ON [docsale].Salvage_ID = S.[Salvage_ID]
WHERE [__$operation] = 5
AND S.[Salvage_ID] NOT IN (SELECT ASAPSalvageID FROM EDW.dbo.LKExcludeASAPSalvageID) --logic to not included stocks that we dont want it EDW

CREATE UNIQUE CLUSTERED INDEX IX_SL_Stock_Data_Stage_ASAPSalvageID
    ON #TMP_Stock_Data_Stage (ASAPSalvageID) 
	with fillfactor = 100; 

-- Updating Polk data to temp table #TMP_Stock_Data_Stage
CREATE TABLE #TMP_FullBodyStyle_Data(
	FullBodyStyleName [varchar] (50) NULL
	,makename [varchar] (50) NULL
	,StockSegment [varchar] (14) NULL
	,BodyTypeCode [varchar] (2) NULL
	,VehicleType [char] (1) NULL
	,MakeCode [varchar] (10) NULL)
INSERT INTO #TMP_FullBodyStyle_Data(FullBodyStyleName ,makename ,StockSegment ,BodyTypeCode ,VehicleType ,MakeCode)
SELECT 
    FullBodyStyleName
    ,ltrim(rtrim(MakeName)) as makename
    ,CASE 	WHEN SEGMENT = 'Sedan' and MakeCode IS NOT NULL THEN 'Foreign Sedan'
		    WHEN SEGMENT = 'Sedan'                           THEN 'Domestic Sedan'
		    ELSE ISNULL(segment, 'Other')
	 END AS StockSegment
    ,BodyTypeCode
    ,VehicleType
    ,MakeCode
FROM dbo.SegmentBodyStyle WITH (NOLOCK)
CROSS JOIN 
	dbo.SegmentOrigin WITH (NOLOCK)
	
CREATE UNIQUE CLUSTERED INDEX IX_TMP_FullBodyStyle
    ON #TMP_FullBodyStyle_Data (BodyTypeCode,VehicleType,MakeCode ) 
	with fillfactor = 100;

--SELECT vin,* FROM #TMP_Stock_Data_Stage
/* DECLARE @ASAPSalvageID	Int
	,	@Vin			nchar(17)
	,	@Make			nvarchar(35)
	,	@Model			nvarchar(25)
	,	@RowCtr			int
--*/

UPDATE SDS SET IsEVMUpdated = DS.IsEVMUpdated
	FROM #TMP_Stock_Data_Stage SDS
	INNER JOIN dbo.dimStock DS WITH (NOLOCK)
		ON sds.ASAPSalvageID = DS.ASAPSalvageID

DECLARE VINSTockCursor CURSOR FOR
	SELECT
		  SDS.ASAPSalvageID		as int
		, ISNULL(SDS.Vin,'0')	as VIN
	FROM #TMP_Stock_Data_Stage SDS WITH (NOLOCK)
	LEFT OUTER JOIN dbo.dimStock DS WITH (NOLOCK)
		ON sds.ASAPSalvageID = DS.ASAPSalvageID
	WHERE RTRIM(LTRIM(SDS.VIN)) <> ''
		AND	ISNULL(RTRIM(LTRIM(SDS.VIN)),'0') <> '0' 		
		And LEN(ISNULL(RTRIM(LTRIM(SDS.VIN)),'')) = 17
		And SDS.IsEVMUpdated = 0

OPEN VINSTockCursor

Set @RowCtr = 0

-- Perform the first fetch.
FETCH NEXT FROM VINSTockCursor
		INTO
			  @ASAPSalvageID
			, @Vin

--=========================================================================
--	Loop through the Cursor to process each new row
-- Check @@FETCH_STATUS to see if there are any more rows to fetch.

WHILE @@FETCH_STATUS = 0
  BEGIN

   -- This is executed as long as the previous fetch succeeds.
	
	IF RTRIM(LTRIM(@VIN)) <> ''
		BEGIN
		   BEGIN TRY
				Insert	INTO #TMP_VINDecodeReturn
  
--				EXEC DecodeVinCLR  @Vin,1,0,1,1
				EXEC DecodeVinCLR  @Vin,1,0,NULL,1,1,2


			END TRY
			BEGIN CATCH
			END CATCH

			IF (SELECT COUNT(*) FROM #TMP_VINDecodeReturn WITH (NOLOCK)) >= 1
			  BEGIN


				INSERT INTO #VINDecodeSourceExtract	(
					  ASAPSalvageID
					, Vin
					, Make
					, Model
					, Body_Style_Name
					, Base_Shipping_Weight
					, Series_Name
					, Cylinders_Description
					, Drive_Line_Type
					, Fuel_Type
					, VehicleClass
					, Segment
					, CountryOfOrigin							)
				SELECT
					  @ASAPSalvageID
					, @Vin
					, Make
					, Model
					, Body_Style_Name
					, Base_Shipping_Weight
					, Series_Name
					, Cylinders_Description
					, Drive_Line_Type
					, Fuel_Type
					, Vehicle_Class
					, Segment
					, CountryOfOrigin
				FROM #TMP_VINDecodeReturn WITH (NOLOCK)

			  END
			  DELETE FROM #TMP_VINDecodeReturn
		END

	    FETCH NEXT FROM VINSTockCursor
			INTO
			  @ASAPSalvageID
			, @Vin


		Set @RowCtr = @RowCtr + 1

  END

CLOSE VINSTockCursor
DEALLOCATE VINSTockCursor

--SELECT * FROM #TMP_VINDecodeReturn
--SELECT * FROM #VINDecodeSourceExtract

	Update ds
		Set   ds.MakeName				= COALESCE(vdse.Make,S.MakeName,ISNULL(ds.MakeName,''))
			, ds.ModelName				= LEFT(COALESCE(vdse.Model,S.ModelName,ISNULL(ds.ModelName,'')),25)
			, ds.FullBodyStyle			= COALESCE(vdse.Body_Style_Name,S.FullBodyStyle,ISNULL(ds.FullBodyStyle,''))
			, ds.EVMBaseShippingWeight	= COALESCE(vdse.Base_Shipping_Weight,S.EVMBaseShippingWeight,ISNULL(ds.EVMBaseShippingWeight,''))
			, ds.SeriesName				= LEFT(COALESCE(vdse.Series_Name,S.SeriesName,ISNULL(ds.SeriesName,'')),25)
			, ds.EVMCylindersDescription= COALESCE(vdse.Cylinders_Description,S.EVMCylindersDescription,ISNULL(ds.EVMCylindersDescription,''))
			, ds.DriveLineType			= COALESCE(vdse.Drive_Line_Type,S.DriveLineType,ISNULL(ds.DriveLineType,''))
			, ds.FuelType				= COALESCE(vdse.Fuel_Type,S.FuelType,ISNULL(ds.FuelType,''))
			, ds.EVMVehicleClass		= COALESCE(vdse.VehicleClass,S.EVMVehicleClass,ISNULL(ds.EVMVehicleClass,''))
			, ds.StockSegment			= COALESCE(CASE 	WHEN ISNULL(vdse.SEGMENT,'Sedan') = 'Sedan' and SO.MakeCode IS NOT NULL THEN 'Foreign Sedan'
												WHEN ISNULL(vdse.SEGMENT,'Sedan') = 'Sedan'                          THEN 'Domestic Sedan'
												ELSE ISNULL(vdse.segment, 'Other') END, S.StockSegment)
			, ds.IsEVMUpdated = IIF(vdse.ASAPSalvageID IS NULL,s.IsEVMUpdated,1)
			, ds.CountryOfManufacture	= vdse.CountryOfOrigin
	From #TMP_Stock_Data_Stage AS ds
		LEFT OUTER Join	#VINDecodeSourceExtract AS vdse WITH (NOLOCK)
				on ds.ASAPSalvageID	= vdse.ASAPSalvageID
		LEFT OUTER JOIN [EDW].[dbo].[SegmentOrigin] SO WITH (NOLOCK)
				on ds.MakeName = LTRIM(RTRIM(SO.MakeName))
		LEFT OUTER JOIN dbo.dimStock S WITH (NOLOCK)
				on ds.ASAPSalvageID = S.ASAPSalvageID

--UPDATE DSS SET
----SELECT DSS.ASAPSalvageID ,
	
--	 StockTypeDescription = CASE WHEN DSS.StockTypeDescription IN ('Motorcycle','Other') 
--									 THEN CASE WHEN (DSS.ModelName LIKE '%Scooter%' OR DSS.MakeName LIKE '%Scooter%')
--												  THEN 'Scooter' 
--											    ELSE CASE WHEN (DSS.ModelName LIKE '%ATV%' OR DSS.MakeName LIKE '%ATV%')
--														  THEN 'ATV'  
--												     ELSE ISNULL(DSS.StockTypeDescription,'') END END
--									ELSE  ISNULL(DSS.StockTypeDescription,'') END 
--	, StockSegment = CASE WHEN DSS.StockTypeDescription IN ('Motorcycle','Other') 
--							THEN CASE WHEN (DSS.ModelName LIKE '%Scooter%' OR DSS.MakeName LIKE '%Scooter%'
--										OR DSS.ModelName LIKE '%ATV%' OR DSS.MakeName LIKE '%ATV%')
--										THEN 'Other' 
--										ELSE CASE 
--												WHEN ISNULL(DSS.FullBodyStyle,'Other') = 'Other'         THEN 'Other'
--												WHEN ISNULL(DSS.StockSegment,'Domestic')  = 'Foreign Sedan' THEN 'Foreign Sedan'
--												WHEN ISNULL(DSS.StockSegment,'Sedan' ) = 'Sedan'         THEN 'Domestic Sedan'
--													ELSE COALESCE(DSS.StockSegment,DimStock.StockSegment,'')
--												END END
--							ELSE CASE 
--									WHEN ISNULL(DSS.FullBodyStyle,'Other') = 'Other'         THEN 'Other'
--									WHEN ISNULL(DSS.stocksegment,'Domestic')  = 'Foreign Sedan' THEN 'Foreign Sedan'
--									WHEN ISNULL(DSS.stocksegment,'Sedan' ) = 'Sedan'         THEN 'Domestic Sedan'
--										ELSE COALESCE(DSS.stocksegment,DimStock.StockSegment,'')
--									END 
--							END 
--	 , MotorCycleSegment = CASE WHEN DSS.StockTypeDescription = 'Motorcycle'
--								  THEN	CASE WHEN DSS.MakeName IN ('HONDA','HYOSUNG','KAWASAKI','SUZUKI','YAMAHA','ZONGSHEN') THEN 'Asian'
--										 WHEN DSS.MakeName IN ('AMERICAN IRON HORSE','ATK','BIG BEAR CHOPPERS','BIG DOG','BUELL','CPI MOTOR COMPANY','ELECTRIC VEHICLE TECH','INDIAN','POLARIS','VICTORY') THEN 'Domestic'
--										 WHEN DSS.MakeName IN ('HARLEY-DAVIDSON','HARLEY DAVIDSON') THEN 'Harley' 
--										 WHEN DSS.MakeName IN ('APRILIA','BMW','BMW','CAGIVA','DERBI','DUCATI','HUSABERG','HUSQVARNA','KTM','MOTO GUZZI','MOTORRAD ZWEIRADWERK','PIAGGIO','TRIUMPH','VESPA' ) THEN  'European'
--										 ELSE 'Other' END
--								 ELSE 'Other' END 
--FROM #TMP_Stock_Data_Stage AS DSS
--	LEFT OUTER JOIN dbo.DimStock WITH(NOLOCK)
--		ON DSS.ASAPSalvageID = DimStock.ASAPSalvageID

UPDATE DSS SET
--SELECT DSS.ASAPSalvageID ,DSS.StockSegment,
	 StockTypeDescription = CASE WHEN DSS.StockTypeDescription IN ('Motorcycle','Other') 
									 THEN CASE WHEN (DSS.ModelName LIKE '%Scooter%' OR DSS.MakeName LIKE '%Scooter%')
												  THEN 'Scooter' 
											    ELSE CASE WHEN (DSS.ModelName LIKE '%ATV%' OR DSS.MakeName LIKE '%ATV%')
														  THEN 'ATV'  
												     ELSE ISNULL(DSS.StockTypeDescription,'') END END
									ELSE  ISNULL(DSS.StockTypeDescription,'') END 
	,StockSegment = CASE WHEN DSS.StockTypeDescription IN ('Motorcycle','Other') 
							THEN CASE WHEN (DSS.ModelName LIKE '%Scooter%' OR DSS.MakeName LIKE '%Scooter%'
										OR DSS.ModelName LIKE '%ATV%' OR DSS.MakeName LIKE '%ATV%')
										THEN 'Other' 
										ELSE CASE 
												WHEN ISNULL(sbs.FullBodyStyleName,'Other') = 'Other'         THEN 'Other'
												WHEN ISNULL(tbs.stocksegment,'Domestic')  = 'Foreign Sedan' THEN 'Foreign Sedan'
												WHEN ISNULL(tbs.stocksegment,sbs.segment) = 'Sedan'         THEN 'Domestic Sedan'
													ELSE COALESCE(tbs.stocksegment, sbs.segment,DimStock.StockSegment,'')
												END END
							ELSE CASE 
									WHEN ISNULL(sbs.FullBodyStyleName,'Other') = 'Other'         THEN 'Other'
									WHEN ISNULL(tbs.stocksegment,'Domestic')  = 'Foreign Sedan' THEN 'Foreign Sedan'
									WHEN ISNULL(tbs.stocksegment,sbs.segment) = 'Sedan'         THEN 'Domestic Sedan'
										ELSE COALESCE(tbs.stocksegment, sbs.segment,DimStock.StockSegment,'')
									END 
							END 
	 , MotorCycleSegment = CASE WHEN DSS.StockTypeDescription = 'Motorcycle'
								  THEN	CASE WHEN DSS.MakeName IN ('HONDA','HYOSUNG','KAWASAKI','SUZUKI','YAMAHA','ZONGSHEN') THEN 'Asian'
										 WHEN DSS.MakeName IN ('AMERICAN IRON HORSE','ATK','BIG BEAR CHOPPERS','BIG DOG','BUELL','CPI MOTOR COMPANY','ELECTRIC VEHICLE TECH','INDIAN','POLARIS','VICTORY') THEN 'Domestic'
										 WHEN DSS.MakeName IN ('HARLEY-DAVIDSON','HARLEY DAVIDSON') THEN 'Harley' 
										 WHEN DSS.MakeName IN ('APRILIA','BMW','BMW','CAGIVA','DERBI','DUCATI','HUSABERG','HUSQVARNA','KTM','MOTO GUZZI','MOTORRAD ZWEIRADWERK','PIAGGIO','TRIUMPH','VESPA' ) THEN  'European'
										 ELSE 'Other' END
								 ELSE 'Other' END
FROM #TMP_Stock_Data_Stage AS DSS
	LEFT OUTER JOIN dbo.PolkPrefix AS TPP  WITH (NOLOCK)
		ON SUBSTRING(DSS.vin,1,8) = matchkey_1_8
			AND SUBSTRING(DSS.vin,10,1) = matchkey_10
	LEFT OUTER JOIN #TMP_FullBodyStyle_Data AS TBS with (NOLOCK)
		ON TBS.BodyTypeCode = TPP.BodyType	
			AND	TBS.VehicleType = TPP.VehicleType
			AND	TBS.makecode = ltrim(rtrim(TPP.MakeAbbreviation))
	LEFT OUTER JOIN dbo.SegmentBodyStyle AS SBS with (NOLOCK)
		ON SBS.BodyTypeCode = TPP.BodyType	
			AND	SBS.VehicleType = TPP.VehicleType
	LEFT OUTER JOIN dbo.DimStock WITH(NOLOCK)
		ON DSS.ASAPSalvageID = DimStock.ASAPSalvageID

---AssignmentMethod

CREATE TABLE #Tmp_Assignment_Source
           (Salvage_ID      INT,
           Code char(4))
    

-- Added Salvage Event Code 'AAND' (Android) 03-12-15
INSERT INTO #Tmp_Assignment_Source
          (Salvage_ID ,
            Code)
              SELECT    s.salvage_id, Code =( SELECT TOP 1 seh.Salvage_Event_Code 
                                                FROM [BI_ASAP_Rep].dbo.[Salvage_Event_History] seh WITH (NOLOCK)
                                                WHERE seh.salvage_id = s.salvage_id
                                                AND seh.salvage_event_code IN 
                                                ('FARM','EVAA','CSAT','EAIA','AIOS','AAND')
                                                ORDER BY seh.[Update_DateTime] DESC)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](
   @MinLSN,@MaxLSN,'all with merge') AS S 
   WHERE [__$operation] = 5
	
	
UPDATE DS SET
	AssignmentSource= (case when TAS.code in ('AVIS','BRMU','CHRY','FARM','GEIC','JUCR','USAA') then 'Direct' when TAS.code in ('EVAA','CSAT') then 'CSA' when TAS.code = 'AASC' then 'AASC' when TAS.code = 'ADP' then 'Audatex' when TAS.code = 'AIMS' then 'AIMS' when TAS.code = 'CCCS' then 'CCC'when TAS.code = 'EAUT' then 'EAUT' when TAS.code = 'ESUR' then 'Process Claims' when TAS.code = 'PROC' then 'Process Claims' when TAS.code = 'MITC' then 'MITC' else 'Manual' END)
    ,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock DS
	INNER JOIN #Tmp_Assignment_Source TAS
		ON DS.ASAPSalvageID = TAS.Salvage_ID 
	
--UPDATE TS
--SET AssignmentMethod = (CASE 
--						WHEN sic.code in ('EVAA','CSAT') THEN 'CSA' 
--						WHEN sic.code IN ('EAIA','FARM') THEN 'EBIZ' 
--						WHEN sic.code IN ('AIOS') THEN 'IOS APP' 
--						WHEN sic.code IN ('AAND') THEN 'ANDROID'
--						ELSE 'ASAP' END)
--FROM #TMP_Stock_Data_Stage TS WITH (NOLOCK)
--LEFT JOIN #Tmp_Assignment_Source sic
--ON TS.[ASAPSalvageID] = sic.[salvage_id]



UPDATE TS
SET AssignmentMethod = 
			(
             CASE 
                      WHEN sic.code IN ('AIOS') THEN 'IOS'
                     WHEN sic.code IN ('AAND') THEN 'ANDROID'
              WHEN sic.code in ('EVAA','CSAT') THEN 'CSA' 
                     WHEN sic.code IN ('EAIA','FARM') THEN 'EBIZ' 
                     ELSE 'ASAP' 
                    END
			)
FROM #TMP_Stock_Data_Stage TS WITH (NOLOCK)
LEFT JOIN #Tmp_Assignment_Source sic
ON TS.[ASAPSalvageID] = sic.[salvage_id]

----      
	
	
--UPDATE TS SET
----SELECT DSS.ASAPSalvageID ,
--	  MakeName = COALESCE(TPP.MakeName,TS.ASAP_MakeName,'')
--	, FullBodyStyle = COALESCE(TPP.FullBodyStyleName,DimStock.FullBodyStyle,'')
--	, CountryOfManufacture = COALESCE(TPP.CountryofOrigin,DimStock.CountryOfManufacture,'')
--FROM #TMP_Stock_Data_Stage AS TS
--	--INNER JOIN #TMP_Stock_Data_Stage AS TS
--	--	ON DSS.ASAPSalvageID = TS.ASAPSalvageID 
--	LEFT OUTER JOIN #TMP_Unique_Stock_Item_Polk_Xref AS TSP WITH (NOLOCK)
--		ON TS.ASAPSalvageID = TSP.Salvage_ID 
--	--LEFT OUTER JOIN [Polk].dbo.Polk_Prefix AS TPP  WITH (NOLOCK)-- Alias DB1 - Polk_Prefix P
--	LEFT OUTER JOIN dbo.PolkPrefix AS	TPP with (NOLOCK)
--		ON TSP.MatchKey = TPP.Matchkey
--	LEFT OUTER JOIN dbo.DimStock WITH(NOLOCK)
--		ON TS.ASAPSalvageID = DimStock.ASAPSalvageID
	

--UPDATE DSS SET
----SELECT DSS.ASAPSalvageID ,
	
--	 StockTypeDescription = CASE WHEN DSS.StockTypeDescription IN ('Motorcycle','Other') 
--									 THEN CASE WHEN (DSS.ModelName LIKE '%Scooter%' OR DSS.MakeName LIKE '%Scooter%')
--												  THEN 'Scooter' 
--											    ELSE CASE WHEN (DSS.ModelName LIKE '%ATV%' OR DSS.MakeName LIKE '%ATV%')
--														  THEN 'ATV'  
--												     ELSE ISNULL(DSS.StockTypeDescription,'') END END
--									ELSE  ISNULL(DSS.StockTypeDescription,'') END 
--	, StockSegment = CASE WHEN DSS.StockTypeDescription IN ('Motorcycle','Other') 
--							THEN CASE WHEN (DSS.ModelName LIKE '%Scooter%' OR DSS.MakeName LIKE '%Scooter%'
--										OR DSS.ModelName LIKE '%ATV%' OR DSS.MakeName LIKE '%ATV%')
--										THEN 'Other' 
--										ELSE CASE 
--												WHEN ISNULL(sbs.FullBodyStyleName,'Other') = 'Other'         THEN 'Other'
--												WHEN ISNULL(tbs.stocksegment,'Domestic')  = 'Foreign Sedan' THEN 'Foreign Sedan'
--												WHEN ISNULL(tbs.stocksegment,sbs.segment) = 'Sedan'         THEN 'Domestic Sedan'
--													ELSE COALESCE(tbs.stocksegment, sbs.segment,DimStock.StockSegment,'')
--												END END
--							ELSE CASE 
--									WHEN ISNULL(sbs.FullBodyStyleName,'Other') = 'Other'         THEN 'Other'
--									WHEN ISNULL(tbs.stocksegment,'Domestic')  = 'Foreign Sedan' THEN 'Foreign Sedan'
--									WHEN ISNULL(tbs.stocksegment,sbs.segment) = 'Sedan'         THEN 'Domestic Sedan'
--										ELSE COALESCE(tbs.stocksegment, sbs.segment,DimStock.StockSegment,'')
--									END 
--							END 
--	 , MotorCycleSegment = CASE WHEN DSS.StockTypeDescription = 'Motorcycle'
--								  THEN	CASE WHEN DSS.MakeName IN ('HONDA','HYOSUNG','KAWASAKI','SUZUKI','YAMAHA','ZONGSHEN') THEN 'Asian'
--										 WHEN DSS.MakeName IN ('AMERICAN IRON HORSE','ATK','BIG BEAR CHOPPERS','BIG DOG','BUELL','CPI MOTOR COMPANY','ELECTRIC VEHICLE TECH','INDIAN','POLARIS','VICTORY') THEN 'Domestic'
--										 WHEN DSS.MakeName IN ('HARLEY-DAVIDSON','HARLEY DAVIDSON') THEN 'Harley' 
--										 WHEN DSS.MakeName IN ('APRILIA','BMW','BMW','CAGIVA','DERBI','DUCATI','HUSABERG','HUSQVARNA','KTM','MOTO GUZZI','MOTORRAD ZWEIRADWERK','PIAGGIO','TRIUMPH','VESPA' ) THEN  'European'
--										 ELSE 'Other' END
--								 ELSE 'Other' END 
--FROM #TMP_Stock_Data_Stage AS DSS
--	--INNER JOIN #TMP_Stock_Data_Stage AS TS
--	--	ON DSS.ASAPSalvageID = TS.ASAPSalvageID 
--	LEFT OUTER JOIN #TMP_Unique_Stock_Item_Polk_Xref AS TSP WITH (NOLOCK)
--		ON DSS.ASAPSalvageID = TSP.Salvage_ID 
----	LEFT OUTER JOIN [Polk].dbo.Polk_Prefix AS TPP  WITH (NOLOCK)-- Alias DB1 - Polk_Prefix P
--	LEFT OUTER JOIN dbo.PolkPrefix AS TPP  WITH (NOLOCK)
--		ON TSP.MatchKey = TPP.Matchkey
--	LEFT OUTER JOIN #TMP_FullBodyStyle_Data AS TBS with (NOLOCK)
--		ON TBS.BodyTypeCode = TPP.BodyType	
--			AND	TBS.VehicleType = TPP.VehicleType
--			AND	TBS.makecode = ltrim(rtrim(TPP.MakeAbbreviation))
--	LEFT OUTER JOIN dbo.SegmentBodyStyle AS SBS with (NOLOCK)
--		ON SBS.BodyTypeCode = TPP.BodyType	
--			AND	SBS.VehicleType = TPP.VehicleType
--	LEFT OUTER JOIN dbo.DimStock WITH(NOLOCK)
--		ON DSS.ASAPSalvageID = DimStock.ASAPSalvageID

--SellFromStorageLocationID
--Update the SellFromStorageLocationID in DimStock table, When the Stock was not sold.

CREATE TABLE #Tmp_Salvage_ID_2_Update(Salvage_ID INT, Sell_From_Storage_Location_ID INT)

INSERT INTO #Tmp_Salvage_ID_2_Update(Salvage_ID ,Sell_From_Storage_Location_ID)
SELECT S.Salvage_ID ,Sell_From_Storage_Location_ID
FROM BI_ASAP_Rep.dbo.Salvage S WITH(NOLOCK)
WHERE S.Salvage_ID IN (
					SELECT S.Salvage_ID
					FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](
						@MinLSN,@MaxLSN,'all with merge') AS S 
					WHERE [__$operation] = 5
					UNION 
					SELECT SPI.Salvage_ID
					FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Provider_Info](
						@MinLSN,@MaxLSN,'all with merge') AS SPI 
					WHERE [__$operation] = 5)





CREATE TABLE #Tmp_Storage_Location_Update(Salvage_ID INT, StorageLocationDatetime Datetime, StorageLocation_ID INT)

--Get the latest Storage_Location_ID Status_Change_DateTime for the stocks
INSERT INTO #Tmp_Storage_Location_Update(Salvage_ID, StorageLocationDatetime)
SELECT SLH.Salvage_ID, MAX(SLH.Status_Change_DateTime) AS Status_Change_DateTime
FROM [BI_ASAP_REP].dbo.Salvage_Location_History SLH WITH (NOLOCK)
INNER JOIN #Tmp_Salvage_ID_2_Update TS WITH (NOLOCK)
	ON 	SLH.Salvage_ID = TS.Salvage_ID
GROUP BY SLH.salvage_ID 

--Latest Storage_Location_ID for the stocks
UPDATE TS
SET 
--SELECT SLH.Salvage_ID,
Sell_From_Storage_Location_ID  = ISNULL(SLH.Storage_Location_ID,0)
from [BI_ASAP_REP].dbo.Salvage_Location_History SLH with (NOLOCK)
INNER JOIN #Tmp_Salvage_ID_2_Update TS 
	ON 	SLH.Salvage_ID = TS.Salvage_ID 
INNER JOIN #Tmp_Storage_Location_Update TSLU  with (NOLOCK)
	ON 	SLH.Salvage_ID = TSLU.Salvage_ID 
	AND	SLH.Update_DateTime = TSLU.StorageLocationDatetime 



-- Gets the data from Salavage_Location_History table using the CDC function and inserts it into a Temp table. 
CREATE TABLE #TMP_SLH_Data(
	  Salvage_ID [int] NOT NULL
	, Aisle_Number [varchar](3) NULL
	, Stall_Number [smallint] NULL)
INSERT INTO #TMP_SLH_Data (Salvage_ID ,Aisle_Number ,Stall_Number)
SELECT SLHCT.[Salvage_ID] ,
CASE WHEN ISNULL(LTRIM(RTRIM(SLHCT.[Aisle_Number])),'') = 'TH' THEN '' ELSE ISNULL(LTRIM(RTRIM(SLHCT.[Aisle_Number])),'') END AS Aisle_Number  ,
ISNULL(SLHCT.[Stall_Number],0) AS Stall_Number
	FROM [BI_ASAP_Rep].[cdc].[dbo_Salvage_Location_History_CT] AS SLHCT WITH (NOLOCK)
		INNER JOIN 
			(SELECT Salvage_ID ,MAX(__$start_lsn) AS Start_lsn
				FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Location_History](
					@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
				GROUP BY Salvage_ID) AS SLHID
				ON SLHID.Salvage_ID = SLHCT.Salvage_ID AND SLHID.Start_lsn = SLHCT.__$start_lsn 
			WHERE [__$operation] IN (2,4)
	
Update #TMP_SLH_Data SET Aisle_Number='' WHERE Aisle_Number IN ('$',',','À','Ç','Ñ','Ò','Ù','ZÑ','Ð','Þ','β')


CREATE INDEX IX_SLH_Data_Salvage_ID
    ON #TMP_SLH_Data ( Salvage_ID ) 
	WITH FILLFACTOR = 100;

-- Gets the data from Salvage_Provider_Info table using the CDC function and inserts it into a Temp table. 
CREATE TABLE #TMP_SPI_Data(
	  Salvage_ID [int] NOT NULL
	, Claim_Number [varchar](30) NULL
	, Handler_User_ID [int] NULL
	, Adjustor_First_Name [varchar] (60) NULL
	, Adjustor_Last_Name [varchar]	(60) NULL
	, Caller_User_ID [int] NULL
	, Claim_Handler	[varchar] (150) NULL
	, Policy_Number [varchar](20) NULL
	, Owner_First_Name [varchar](50) NULL
	, Owner_Last_Name [varchar](50) NULL
	, Minimum_Bid_Amount [decimal](11, 2) NULL
	, Insured_First_Name [varchar](50) NULL
	, Insured_Last_Name [varchar](50) NULL
	, IBF_AC_Qualified_Ind [bit] NULL
	, IBF_Qualified_Ind [bit] NULL
	, IBF_Run_Count [tinyint] NULL
	, IBF_Run_Threshold_Reached_Ind [bit] NULL
	, IBF_SP_Approved_Ind [bit] NULL 
	, Offsite_Sale_Ind [tinyint] NOT NULL
	, Minimum_Bid_Source [varchar](20) NULL
	, Provider_ID [int] NULL
	,[OriginalProviderid] int NULL
	, AlternatePayeeID INT NULL
	,ClaimKey [varchar](32)
	,AdjustorPhone varchar(11))
INSERT INTO #TMP_SPI_Data (Salvage_ID ,Claim_Number ,Handler_User_ID ,Adjustor_First_Name 
	,Adjustor_Last_Name ,Caller_User_ID ,Claim_Handler ,Policy_Number ,Owner_First_Name ,Owner_Last_Name
	,Minimum_Bid_Amount ,Insured_First_Name ,Insured_Last_Name ,IBF_AC_Qualified_Ind 
	,IBF_Qualified_Ind ,IBF_Run_Count ,IBF_Run_Threshold_Reached_Ind ,IBF_SP_Approved_Ind
	,Offsite_Sale_Ind ,Provider_ID,[OriginalProviderid],AlternatePayeeID,ClaimKey,AdjustorPhone  )
SELECT SPI.Salvage_ID
	 ,ISNULL(SPI.Claim_Number,'') AS Claim_Number
	 ,ISNULL(SPI.Handler_User_ID,0) AS Handler_User_ID
	 ,ISNULL(DU1.FirstName,'') AS Adjustor_First_Name  
	 ,ISNULL(DU1.LastName,'') AS Adjustor_Last_Name
	 ,ISNULL(SPI.Caller_User_ID,0) AS Caller_User_ID  
     ,CASE WHEN DU2.FirstName IS NULL 
			THEN ISNULL(DU2.LastName,'')
		   ELSE ISNULL(DU2.FirstName + ISNULL(' '+DU2.LastName,''),'')
	  END AS Claim_Handler
     ,ISNULL(SPI.Policy_Number,'') AS Policy_Number
     ,ISNULL(SPI.Owner_First_Name,'') AS Owner_First_Name
     ,ISNULL(SPI.Owner_Last_Name,'') AS Owner_Last_Name
	 ,SPI.Minimum_Bid_Amount
	 ,ISNULL(SPI.Insured_First_Name,'') Insured_First_Name
	 ,ISNULL(SPI.Insured_Last_Name,'') Insured_Last_Name
	 ,SPI.IBN_AC_Qualified_Ind ,SPI.IBN_Qualified_Ind ,SPI.IBN_Run_Count ,SPI.IBN_Run_Threshold_Reached_Ind
	 ,SPI.IBN_SP_Approved_Ind
	 ,Offsite_Sale_Ind 
	 ,ISNULL(SPI.Salvage_Provider_ID,0) AS Salvage_Provider_ID 
	 , 0 AS [OriginalProviderid]
	 ,SPI.Alternate_Payee_ID
	 ,ISNULL(SPI.[Unit_Number],'') AS ClaimKey,''
	 --,ISNULL(UI.Phone_Number_1,'') AS AdjustorPhone
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Provider_Info](
   @MinLSN,@MaxLSN,'all with merge') AS SPI
   LEFT OUTER JOIN dbo.DimUser AS DU1 with (NOLOCK)
		ON SPI.Handler_User_ID = DU1.[UserID]
   LEFT OUTER JOIN dbo.DimUser AS DU2 with (NOLOCK) 
	    ON SPI.Caller_User_ID = DU2.[UserID]
   LEFT OUTER JOIN [BI_ASAP_REP].[dbo].[Salvage_Provider] AS SP with (NOLOCK)
        ON (SP.Salvage_Provider_ID = SPI.Salvage_Provider_ID)
   LEFT OUTER JOIN [BI_ASAP_REP].[dbo].[Salvage_Provider_Group] AS SPG with (NOLOCK)
        ON (SPG.Salvage_Provider_Group_ID = SP.Salvage_Provider_Group_ID )
  -- LEFT OUTER JOIN [BI_ASAP_REP].[dbo].User_Info AS UI with (NOLOCK)
		--ON SPI.Handler_User_ID = UI.[User_ID]
   WHERE [__$operation] = 5

UPDATE SPI SET  AdjustorPhone=ISNULL(UI.Phone_Number_1,'')
FROM #TMP_SPI_Data AS SPI
   Inner JOIN [BI_ASAP_REP].[dbo].User_Info AS UI with (NOLOCK)
		ON SPI.Handler_User_ID = UI.[User_ID]

   Update #TMP_Stock_Data_Stage
   set 	
	[OriginalProviderid] =#TMP_SPI_Data.[OriginalProviderid] 
	from #TMP_Stock_Data_Stage INNER JOIN  #TMP_SPI_Data ON
	(#TMP_Stock_Data_Stage.asapsalvageid = #TMP_SPI_Data.salvage_id)

 CREATE UNIQUE CLUSTERED INDEX IX_TMP_SPI_Data
    ON #TMP_SPI_Data (Salvage_ID) 
	WITH FILLFACTOR = 100


-----------------------------------IBFRenege Ind-----------------------------------------------------

CREATE TABLE #TMP_IBF_Renege
(Salvage_ID INT NOT NULL,
 RenegeCancel_Datetime Datetime)
 
CREATE TABLE #TMP_IBN_Sold
(Salvage_ID INT NOT NULL ,
 Sold_Datetime Datetime,
 IBN_IND INT)


INSERT INTO #TMP_IBF_Renege
(Salvage_ID,RenegeCancel_Datetime)
SELECT 
Salvage_ID,
Max(CASE  WHEN Salvage_Event_Code IN ('BYRE','BYCN') THEN SEH.Event_DateTime  END) RenegeCancel_DateTime
FROM 
[BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge') AS SEH 
where 
SEH.Salvage_Event_Code IN ('BYRE','BYCN')
GROUP BY 
Salvage_ID,
Salvage_Event_Code


INSERT INTO #TMP_IBN_Sold
(Salvage_ID,Sold_Datetime,IBN_IND)
SELECT Salvage_ID, MAX(Sold_Datetime) AS SOLD_Datetime, IBN_Ind
FROM 
[BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Sale_History](@MinLSN,@MaxLSN,'all with merge')
WHERE IBN_Ind = 1 
GROUP BY Salvage_ID,IBN_Ind


---------------------------Calculating MinimumBidSource -----------------------------------
CREATE TABLE #TMP_MinBS_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Call_for_Sale_Authorization_Ind] [bit] NULL
	, [ASAP_Minimum_Bid_Amount] [decimal](18, 0) NULL
	, [CSAT_Updated_Minimim_Bid] [decimal](18, 0) NULL)	
INSERT INTO #TMP_MinBS_Data (Salvage_ID ,Call_for_Sale_Authorization_Ind ,ASAP_Minimum_Bid_Amount)
SELECT SPI.Salvage_ID , S.Call_for_Sale_Authorization_Ind ,SPI.Minimum_Bid_Amount
FROM #TMP_SPI_Data SPI WITH (NOLOCK)
	INNER JOIN BI_ASAP_Rep.dbo.Salvage AS S WITH(NOLOCK)
		ON SPI.Salvage_ID = S.Salvage_ID

CREATE TABLE #TMP_CSADATE_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Max_Cddate] [datetime] NULL)
INSERT INTO #TMP_CSADATE_Data (Salvage_ID ,Max_Cddate)
SELECT TMD.Salvage_ID , MAX(CSAT.cddate) AS Max_Cddate
FROM dbo.CSATAutoMinimumBidUpdateHistoryNew AS CSAT WITH (NOLOCK)
	INNER JOIN #TMP_MinBS_Data AS TMD  WITH (NOLOCK)
		ON TMD.Salvage_ID = CSAT.ASAPSalvageID
GROUP BY TMD.Salvage_ID

--SELECT TMDD.Salvage_ID ,CSAT.ASAP_Minimum_Bid_Amount ,CSAT.Updated_Minimim_Bid
UPDATE TMD 
	SET ASAP_Minimum_Bid_Amount = TMD.ASAP_Minimum_Bid_Amount 
	, CSAT_Updated_Minimim_Bid = CSAT.CSATUpdatedMinimimBid
FROM #TMP_CSADATE_Data AS TCSA WITH (NOLOCK)
	INNER JOIN dbo.CSATAutoMinimumBidUpdateHistoryNew AS CSAT WITH (NOLOCK)
		ON TCSA.Salvage_ID = CSAT.ASAPSalvageID AND TCSA.Max_Cddate = CSAT.Cddate
	INNER JOIN #TMP_MinBS_Data AS TMD 
		ON TCSA.Salvage_ID = TMD.Salvage_ID 
 
UPDATE SPI SET
--SELECT
Minimum_Bid_Source = 
	CASE WHEN TMD.Call_For_Sale_Authorization_IND = 1 THEN 'Call'
	     WHEN ISNULL(TMD.ASAP_Minimum_Bid_Amount,0) = 0 THEN 'None'
		 WHEN ISNULL(TMD.ASAP_Minimum_Bid_Amount,0) > 0 AND ISNULL(TMD.csat_Updated_Minimim_Bid,0) > 0 
				AND ISNULL(TMD.ASAP_Minimum_Bid_Amount,0) = csat_Updated_Minimim_Bid THEN 'Automatic'
		 WHEN ISNULL(TMD.ASAP_Minimum_Bid_Amount,0) > 0 AND ISNULL(TMD.csat_Updated_Minimim_Bid,0) > 0 
				AND ISNULL(TMD.ASAP_Minimum_Bid_Amount,0) <> ISNULL(TMD.csat_Updated_Minimim_Bid,0) THEN 'User Override'
			
		ELSE 'User'
	END	
FROM #TMP_MinBS_Data TMD  WITH (NOLOCK)
	INNER JOIN #TMP_SPI_Data SPI
		ON TMD.Salvage_ID = SPI.Salvage_ID
		


-- Gets the data from Salavage_Title table using the CDC function and inserts it into a Temp table. 
-- To get the Name fields from Salavage_Title 
CREATE TABLE #TMP_ST_Data(
	 [Salvage_ID] [int] NOT NULL
	, [Sale_Document_Type_ID] [int] NULL
	, [Sale_Document_Type_Description] [varchar] (40) NULL
	, [Sale_Document_Brand] [varchar](40) NULL
	, [Sale_Document_Number] [varchar](20) NULL
	, [Title_Received_Brand] [varchar](40) NULL
	, [Title_Received_Number] [varchar](20) NULL
	, [Title_State] [char] (3) NULL
	, [Title_type] [varchar] (40) NULL
    , [Auction_Type_Code] [char](2) NULL
    , [Auction_Type_Description] [varchar](40) NULL
    , [Charge_Title_Process_Fee_Ind] [bit] NULL
    , [Salvage_Provider_Process_Title_Ind] [bit] NULL
    , [Title_Processing_Branch_Number] [int] NULL
    , [Title_Service_Used_Ind] [bit] NULL
    , [Sale_Document_Type_Category] [varchar](20) NULL
	, [Cert_State] [char] (3) NULL
	, [Sale_Document_Note] [Varchar](50) NULL
    )
INSERT INTO #TMP_ST_Data (Salvage_ID ,Sale_Document_Type_ID ,
	Sale_Document_Type_Description
	, Sale_Document_Brand ,Sale_Document_Number ,Title_Received_Brand 
	, Title_Received_Number ,Title_State ,Title_type ,Auction_Type_Code ,Auction_Type_Description
	, Charge_Title_Process_Fee_Ind ,Salvage_Provider_Process_Title_Ind ,Title_Processing_Branch_Number
	, Title_Service_Used_Ind ,Sale_Document_Type_Category,Cert_State,Sale_Document_Note)
SELECT ST.Salvage_ID
  , ISNULL(ST.Sale_Document_Type_ID,0) AS Sale_Document_Type_ID

  --, ISNULL(SDT.Sale_Document_Type,'') AS Sale_Document_Type_Description 

  , ISNULL(
			CASE 
			   WHEN  
					(CONVERT
							(VARCHAR(80),LEN (RTRIM(LTRIM(
															(
															SELECT  top 1 Sale_Document_Type    
															FROM	BI_ASAP_Rep.dbo.sale_document_type sdt  WITH (NOLOCK)
																inner join BI_ASAP_Rep.dbo.salvage_title st  WITH (NOLOCK)
																	on st.sale_document_type_id = sdt.sale_document_type_id
																	and s.Salvage_ID = st.Salvage_ID
																order by Sale_Document_Type 
															)
														) 
													 )
											   )
							)
					) < 5

				THEN	(
						SELECT	TOP 1 cd.rslt_char
						FROM	[Provider_DM].dbo.Code_Detail cd  WITH (NOLOCK)
						WHERE	cd.arg_char =
										(
										SELECT  top 1 Sale_Document_Type    
										FROM	BI_ASAP_Rep.dbo.sale_document_type sdt  WITH (NOLOCK)
											inner join BI_ASAP_Rep.dbo.salvage_title st  WITH (NOLOCK)
												on st.sale_document_type_id = sdt.sale_document_type_id
												and s.Salvage_ID = st.Salvage_ID
											order by Sale_Document_Type 
										 )	
						AND cd.code_id = 11
						ORDER BY cd.rslt_char 
						)
			
				ELSE  RTRIM(LTRIM(
								 (
										SELECT  top 1 Sale_Document_Type    
										FROM	BI_ASAP_Rep.dbo.sale_document_type sdt  WITH (NOLOCK)
											inner join BI_ASAP_Rep.dbo.salvage_title st  WITH (NOLOCK)
												on st.sale_document_type_id = sdt.sale_document_type_id
												and s.Salvage_ID = st.Salvage_ID
											order by Sale_Document_Type 
										 )	
								 )
						   )
		 
				END, 'unknown'
		) AS Sale_Document_Type_Description 



  , ISNULL(ST.Sale_Document_Brand,'') AS Sale_Document_Brand 
  , CASE WHEN S.Assignment_Type_Code = 'TTL' 
			THEN ISNULL(ST.Certificate_Number,'') 
		 ELSE ISNULL(ST.Sale_Document_Number,'') END AS Sale_Document_Number	
  , ISNULL(ST.Title_Received_Brand,'') AS Title_Received_Brand 
  , ISNULL(ST.Title_Received_Number,'') AS Title_Received_Number
  , ISNULL(TRT.State_Abbreviation,'') AS Title_State 
  , ISNULL(TRT.Title_Received_Type,'') AS TitleType
  , ISNULL(ST.Auction_Type_Code,'') AS Auction_Type_Code
  , ISNULL(AT.Auction_Type_Description,'') AS Auction_Type_Description
  , ST.Charge_Title_Process_Fee_Ind
  , ST.Salvage_Provider_Process_Title_Ind
  , ISNULL(ST.Title_Processing_Branch_Number,0) AS Title_Processing_Branch_Number
  , ST.Title_Service_Used_Ind
  , ISNULL(XR.TitleTypeCategory,'Other') AS Sale_Document_Type_Category
  , SDT.state_abbreviation AS Cert_State
  ,ISNULL(ST.Sale_Document_Note,'')  AS Sale_Document_Note

FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Title](
		@MinLSN,@MaxLSN,'all with merge') ST
   INNER JOIN [BI_ASAP_Rep].dbo.Salvage AS S WITH(NOLOCK)
		ON ST.Salvage_ID = S.Salvage_ID		
   LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Sale_Document_Type AS SDT WITH (NOLOCK)
		ON ST.Sale_Document_Type_ID = SDT.Sale_Document_Type_ID
   LEFT OUTER JOIN [BI_ASAP_REP].dbo.[Title_Received_Type] TRT WITH (NOLOCK)
		ON ISNUll(ST.Title_Received_Type_ID,1) = ISNULL(TRT.Title_Received_Type_ID,1)
   LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Auction_Type AS AT WITH(NOLOCK)
		ON ST.Auction_Type_Code = AT.Auction_Type_Code
   LEFT OUTER JOIN dbo.SaleDocumentTypeXRef AS XR WITH(NOLOCK)
		ON SDT.Sale_Document_Type = XR.SaleDocumentTypeDescription			
   WHERE [__$operation] = 5 
 
-- Gets the data from Salvage_Damage table using the CDC function and inserts it into a Temp table. 
-- To get the Damage codes and Damage Decsription 
CREATE TABLE #TMP_SD_Data(
	[Salvage_ID] [int] NOT NULL,
	[Damage_Code_Primary] [char](2) NULL,
	[Damage_Code_Secondary] [char](2) NULL)

INSERT INTO #TMP_SD_Data (Salvage_ID)
SELECT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Damage](
   @MinLSN,@MaxLSN,'all with merge') 
   WHERE [__$operation] = 5 AND Sequence_Number = 1 
UNION
SELECT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Damage](
   @MinLSN,@MaxLSN,'all with merge') 
   WHERE [__$operation] = 5 AND Sequence_Number = 2 

UPDATE TSD SET
	Damage_Code_Primary = SD.Damage_Code
FROM #TMP_SD_Data TSD 
	INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Damage SD WITH(NOLOCK)
		ON TSD.Salvage_ID = SD.Salvage_ID 
WHERE --Damage_Code_Primary IS NULL AND 
	SD.Sequence_Number = 1

UPDATE TSD SET
	[Damage_Code_Secondary] = SD.Damage_Code
FROM #TMP_SD_Data TSD 
	INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Damage SD WITH(NOLOCK)
		ON TSD.Salvage_ID = SD.Salvage_ID 
WHERE --Damage_Code_Primary IS NULL AND 
	SD.Sequence_Number = 2

-- Gets the data from Salvage_Special_Purchase table using the CDC function and inserts it into a Temp table. 	
-- To get the Special Purchase code and Description 
CREATE TABLE #TMP_SSP_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Salvage_Special_Purchase_ID] [int] NOT NULL
	, [Special_Purchase_Type_Code] [char](3) NULL
	, [Special_Purchase_Type_Description] [varchar] (30) NULL)
INSERT INTO #TMP_SSP_Data(Salvage_ID ,Salvage_Special_Purchase_ID ,Special_Purchase_Type_Code ,Special_Purchase_Type_Description)
SELECT  SSPID.Salvage_ID ,SSPID.Salvage_Special_Purchase_ID 
,ISNULL(SSPC.Special_Purchase_Type_Code,'') AS Special_Purchase_Type_Code
,ISNULL(SPT.Special_Purchase_Type_Description,'') AS Special_Purchase_Type_Description
FROM BI_ASAP_Rep.cdc.dbo_Salvage_Special_Purchase_CT AS SSPC WITH (NOLOCK)
	INNER JOIN 
		(SELECT Salvage_ID, Max(Salvage_Special_Purchase_ID) AS Salvage_Special_Purchase_ID
			FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Special_Purchase](
			   @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
			GROUP BY Salvage_ID) SSPID
		ON SSPID.Salvage_ID = SSPC.Salvage_ID AND SSPID.Salvage_Special_Purchase_ID = SSPC.Salvage_Special_Purchase_ID 
	LEFT OUTER JOIN [BI_ASAP_Rep].[dbo].[Special_Purchase_Type] AS SPT WITH(NOLOCK)
		ON SSPC.Special_Purchase_Type_Code = SPT.Special_Purchase_Type_Description
	WHERE SSPC.[__$operation] IN (2,4)
	

-- Gets the data from Salvage_Alternate_Info table using the CDC function and inserts it into a Temp table. 	
-- To get the Alternate Stock Number 
CREATE TABLE #TMP_SAI_Data(
	  [Salvage_ID] [int] NOT NULL
	 ,[Alternate_Stock_Number_1] [varchar](15) NOT NULL)
INSERT INTO #TMP_SAI_Data (Salvage_ID, Alternate_Stock_Number_1)
SELECT Salvage_ID ,Alternate_Stock_Number_1  
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Alternate_Info](
	  @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5


-- Gets the data from Salvage_Checkin table using the CDC function and inserts it into a Temp table. 	
-- To get the Engine Status Code 
CREATE TABLE #TMP_SC_Data(
	  [Salvage_ID] [int] NOT NULL
	 ,[Engine_Status_Code] [char](3) NULL
	 ,[Run_and_Drive_Ind] INT NULL)
INSERT INTO #TMP_SC_Data(Salvage_ID, Engine_Status_Code,Run_and_Drive_Ind)
SELECT Salvage_ID, ISNULL(Engine_Status_Code,'') AS Engine_Status_Code,
        Runnable_and_Driveable_Ind
		FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Checkin](
			@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5



-- Gets the data from Salvage_Checkin table using the CDC function and inserts it into a Temp table. 	 
-- To get the Loss type Code & Description 
CREATE TABLE #TMP_SLCD_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Loss_Type_Code] [char](3) NOT NULL
	, [Loss_Type_Description] [varchar](40) NOT NULL)
----INSERT INTO #TMP_SLCD_Data(Salvage_ID ,Loss_Type_Code ,Loss_Type_Description) 
----SELECT SLID.Salvage_ID , SLC.Loss_Type_Code, LT.Loss_Type_Description 
----FROM [BI_ASAP_Rep].[cdc].[dbo_Salvage_Loss_CT] AS SLC WITH(NOLOCK)
----	INNER JOIN	(SELECT Salvage_ID ,MAX(__$start_lsn) AS Start_lsn
----					FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Loss](
----						@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
----					GROUP BY Salvage_ID) AS SLID
----		ON SLC.__$start_lsn = SLID.Start_lsn AND SLC.Salvage_ID = SLID.Salvage_ID
----	INNER JOIN [BI_ASAP_Rep].dbo.Loss_Type AS LT with (NOLOCK)
----		ON SLC.Loss_Type_Code = LT.Loss_Type_Code
----	WHERE SLC.[__$operation] IN (2,4)

INSERT INTO #TMP_SLCD_Data(Salvage_ID ,Loss_Type_Code ,Loss_Type_Description) 
SELECT	SLID.Salvage_ID , 
		SLC.Loss_Type_Code, 
		--LT.Loss_Type_Description 
		ISNULL(
				CASE 
					WHEN (
						SELECT TOP 1 loss_type_code 
						FROM BI_asap_rep.dbo.Salvage_loss sl WITH (NOLOCK)
						WHERE sl.salvage_id = slid.salvage_id
						ORDER BY sl.Update_DateTime DESC ) = 'CO' 
					THEN 
							(
							SELECT rslt_char
							FROM [Provider_DM].[dbo].[Code_Detail] cd WITH (NOLOCK) 
							WHERE cd.Arg_Nbr = 1
							AND cd.code_ID = 6
							)
					WHEN (
						SELECT TOP 1 loss_type_code 
						FROM BI_asap_rep.dbo.Salvage_loss sl WITH (NOLOCK)
						WHERE sl.salvage_id = slid.salvage_id
						ORDER BY sl.Update_DateTime DESC ) = 'TH' 
					THEN 
						(
							SELECT rslt_char
							FROM [Provider_DM].[dbo].[Code_Detail] cd WITH (NOLOCK) 
							WHERE cd.Arg_Nbr =2
							AND cd.code_ID = 6
							)
					WHEN (
						SELECT TOP 1 loss_type_code 
						FROM BI_asap_rep.dbo.Salvage_loss sl WITH (NOLOCK)
						WHERE sl.salvage_id = slid.salvage_id
						ORDER BY sl.Update_DateTime DESC ) = 'FI' 
					THEN 
							(
							SELECT rslt_char
							FROM [Provider_DM].[dbo].[Code_Detail] cd WITH (NOLOCK) 
							WHERE cd.Arg_Nbr = 3
							AND cd.code_ID = 6
							)
					WHEN (
						SELECT TOP 1 loss_type_code 
						FROM BI_asap_rep.dbo.Salvage_loss sl WITH (NOLOCK)
						WHERE sl.salvage_id = slid.salvage_id
						ORDER BY sl.Update_DateTime DESC ) = 'WA' 
					THEN  
							(
							SELECT rslt_char
							FROM [Provider_DM].[dbo].[Code_Detail] cd WITH (NOLOCK) 
							WHERE cd.Arg_Nbr = 4
							AND cd.code_ID = 6
							)
					ELSE  
							(
							SELECT rslt_char
							FROM [Provider_DM].[dbo].[Code_Detail] cd WITH (NOLOCK) 
							WHERE cd.Arg_Nbr = 6
							AND cd.code_ID = 6
							)
				END , 'Unknown' 
			)  AS Loss_Type_Description 
FROM	[BI_ASAP_Rep].[cdc].[dbo_Salvage_Loss_CT] AS SLC WITH(NOLOCK)
		INNER JOIN	(SELECT Salvage_ID ,MAX(__$start_lsn) AS Start_lsn
					FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Loss](
						@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
					GROUP BY Salvage_ID) AS SLID
			ON SLC.__$start_lsn = SLID.Start_lsn AND SLC.Salvage_ID = SLID.Salvage_ID
		INNER JOIN [BI_ASAP_Rep].dbo.Loss_Type AS LT with (NOLOCK)
			ON SLC.Loss_Type_Code = LT.Loss_Type_Code
		    WHERE SLC.[__$operation] IN (2,4)




-- Tow Fields EDW Phase 2-Sprint 7
CREATE TABLE #TMP_STow_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Buyer_Tow_Ind] [tinyint] NOT NULL
	, [Return_Tow_Ind] [tinyint] NOT NULL
	, [Dry_Run_Ind] [tinyint] NOT NULL
	,Buyer_Tow_Miles int NULL
	,Provider_Tow_Miles int NULL)
INSERT INTO #TMP_STow_Data(Salvage_ID ,Buyer_Tow_Ind ,Return_Tow_Ind ,Dry_Run_Ind,Buyer_Tow_Miles, Provider_Tow_Miles)
SELECT  ST.[Salvage_ID]  
		,MAX(CASE WHEN Tow_Type_Code = 'by' THEN 1 ELSE 0 END) AS Buyer_Tow_Ind
		,MAX(CASE WHEN Tow_Type_Code = 'rt' THEN 1 ELSE 0 END) AS Return_Tow_Ind
        ,MAX(CASE WHEN Tow_Dry_Run_Ind = 1 THEN 1 ELSE 0 END) AS Dry_Run_Ind
		,MAX(CASE WHEN Tow_Type_Code = 'by' AND Tow_Successful_Ind = 1 THEN Tow_Miles ELSE NULL END) AS Buyer_Tow_Miles
		,MAX(CASE WHEN Tow_Type_Code = 'pk' AND Tow_Successful_Ind = 1 THEN Tow_Miles ELSE NULL END) AS Provider_Tow_Miles
FROM [BI_ASAP_Rep].[dbo].[Salvage_Tow] AS ST WITH(NOLOCK)
	INNER JOIN (SELECT DISTINCT Salvage_ID 
			    FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Tow](
			    @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5) AS cdc_ST
    ON ST.Salvage_ID = cdc_ST.Salvage_ID
GROUP BY ST.Salvage_ID

-- Express Pickup Indicator
CREATE TABLE #TMP_SP_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Express_Pickup_Ind] [int] NOT NULL
	, [Pickup_Location_ID] int )
INSERT INTO #TMP_SP_Data(Salvage_ID,Express_Pickup_Ind,Pickup_Location_ID)
SELECT [Salvage_ID],[Express_Pickup_Code],[Storage_Location_ID] 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Pickup](
			    @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
		
/* Populating from from Salvage_Sale table EDW Phase 2-Sprint 7*/	 
---- 
--IBFSoldIND,SalePrice,SaleTax
CREATE TABLE #TMP_SSale_Data(
	  [Salvage_ID] [int] NOT NULL
	, IBN_Ind tinyint null default((0))
	, Sale_Amount money null
	, Tax_Amount money null)
INSERT INTO #TMP_SSale_Data(Salvage_ID)
SELECT DISTINCT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Sale](@MinLSN,@MaxLSN,'all with merge')
WHERE Buyer_ID IS NOT NULL 


UPDATE T_SS SET
	  IBN_Ind = SS.IBN_Ind
	 ,Sale_Amount = SS.Sale_Amount
	 ,Tax_Amount = SS.Tax_Amount
--SELECT T_SS.* ,SS.Sale_Amount ,SS.Tax_Amount ,SS.IBN_Ind
FROM #TMP_SSale_Data T_SS
	INNER JOIN BI_ASAP_Rep.dbo.Salvage_Sale SS
		ON T_SS.Salvage_ID = SS.Salvage_ID
WHERE Buyer_ID IS NOT NULL


/* Populating from from Salvage_Expense table - PAPrice EDW Sprint 11*/	 
CREATE TABLE #TMP_SExpense_Data([Salvage_ID] [int] NOT NULL, PA_Price money null)
INSERT INTO #TMP_SExpense_Data(Salvage_ID, PA_Price)
-- Select statement prior to 3-202014
---SELECT Salvage_ID --,Amount ,Payment_Status_Code ,[__$operation]
---	,CASE WHEN Payment_Status_Code <> 'VOD'	THEN Amount
---		  WHEN Payment_Status_Code = 'VOD'	THEN 0 END AS PA_Price 
---FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Expense](
---			    @MinLSN,@MaxLSN,'all with merge') 
---WHERE Charge_Type_ID = 315 AND [__$operation] = 5
-------
/* Modified 03-20-2014 to insure we get the PA_Price on the LATEST Salvage Expense For Charge Type 315*/
SELECT	a.Salvage_ID,
		--b.Salvage_Expense_ID,
		CASE 
			WHEN a.Payment_Status_Code <> 'VOD'	THEN Amount
			WHEN a.Payment_Status_Code = 'VOD'	THEN 0 
		END AS PA_Price 
FROM	[BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Expense](
			    @MinLSN,@MaxLSN,'all with merge') AS a
		INNER JOIN 
				(
				SELECT	MAX(c.Salvage_Expense_ID) Salvage_Expense_ID,
						c.Salvage_ID
				FROM	[BI_ASAP_Rep].[dbo].[Salvage_Expense]  c WITH (NOLOCK)
				WHERE	c.Charge_Type_ID = 315
				GROUP BY
						c.Salvage_ID
				)   b
			ON b.Salvage_ID = a.Salvage_ID
			AND b.Salvage_Expense_ID = a.Salvage_Expense_ID
WHERE a.Charge_Type_ID = 315 AND [__$operation] = 5







-------

/* Stock which are assignment Cancelled should not have PA Price. */
CREATE TABLE #TMP_Stock_Cancelled_Date(Salvage_ID INT NOT NULL, Cancel_DateTime DATETIME NULL)
INSERT INTO #TMP_Stock_Cancelled_Date (Salvage_ID ,Cancel_DateTime)
SELECT  Salvage_ID ,Event_DateTime AS Cancel_DateTime
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge')
WHERE  Salvage_Event_Code = 'ASCN' AND [__$operation] = 5


/***** Populating from from Salvage_Event table - EDW Phase 2-Sprint 7*****/	 
--DECLARE @MinLSN Binary(10)
--		,@MaxLSN Binary (10)

--SELECT @MinLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Salvage_Event_History')
--SELECT @MaxLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()


CREATE TABLE #TMP_Salvage_Event_History_Data
([Salvage_ID] [int] NOT NULL, 
 Bulk_Sale_IND tinyint null, 
 Had_Hold_IND tinyint null, 
-- Had_Release_Problem_IND tinyint null,
 Had_Title_Problem_IND tinyint null)

INSERT INTO #TMP_Salvage_Event_History_Data
(Salvage_ID , Bulk_Sale_IND ,Had_Hold_IND 
--,Had_Release_Problem_IND 
,Had_Title_Problem_IND)
SELECT DISTINCT Salvage_ID 
	,CASE WHEN Salvage_Event_Code = 'SAL9' THEN 1 ELSE NULL END AS Bulk_Sale_IND
	,CASE WHEN Salvage_Event_Code IN ('HDCF','HLCO') THEN 1 ELSE NULL END AS Had_Hold_IND
--	,CASE WHEN Salvage_Event_Code = 'RLPR' THEN 1 ELSE NULL END AS Had_Release_Problem_IND
	,CASE WHEN Salvage_Event_Code = 'TPRG' THEN 1 ELSE NULL END as Had_Title_Problem_IND
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5 AND Salvage_Event_Code IN ('SAL9','HDCF','HLCO','RLPR','TPRG') 
ORDER BY Salvage_ID


/*****Populating from from Salvage_Buyer_Charge_Credit_Card_Premium table - Credit_Card_Sale_Ind EDW Phase 2-Sprint 7*****/	 
CREATE TABLE #TMP_BCC_Data([Salvage_ID] [int] NOT NULL, Credit_Card_Sale_Ind tinyint null)
INSERT INTO #TMP_BCC_Data(Salvage_ID, Credit_Card_Sale_Ind)
SELECT DISTINCT Salvage_ID 
	,Credit_Card_Sale_Ind = CASE WHEN status_code='PD' THEN 1 ELSE 0 END
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Buyer_Charge_Credit_Card_Premium](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5 AND Salvage_ID IS NOT NULL

/*****Populating from Salvage_Estimate table - SP_ACV AS ActualCashValue , IAA_ACV AS BlackbookACV  *****/	 
--IF OBJECT_ID('tempdb..#TMP_Open_Stock_ACV') IS NOT NULL DROP TABLE #TMP_Open_Stock_ACV
CREATE TABLE #TMP_Open_Stock_ACV(Salvage_ID INT, SP_ACV MONEY, IAA_ACV MONEY, ECR MONEY)
INSERT INTO #TMP_Open_Stock_ACV(Salvage_ID)
SELECT DISTINCT SE.Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Estimate](@MinLSN,@MaxLSN,'all with merge') SE
	INNER JOIN BI_ASAP_Rep.dbo.Salvage S WITH(NOLOCK)
		ON S.Salvage_ID = SE.Salvage_ID
WHERE [__$operation] = 5 --AND S.Status_Code <> 'CLO'
	
CREATE UNIQUE CLUSTERED INDEX IX_Open_Stock_IDS ON #TMP_Open_Stock_ACV(Salvage_ID)

UPDATE #TMP_Open_Stock_ACV SET 
	SP_ACV = SE1.Estimate_Amount
--SELECT SE1.Salvage_ID ,SE1.Estimate_Amount AS [SP_ACV] 
FROM BI_ASAP_Rep.dbo.Salvage_Estimate SE1 WITH(NOLOCK)
INNER JOIN #TMP_Open_Stock_ACV OS 
	ON SE1.Salvage_ID = OS.Salvage_ID
WHERE SE1.Estimate_Type_Code = 'ACV' AND SE1.Estimation_Source_Code = 'SP' AND 
	SE1.Estimate_DateTime = (SELECT MAX(SE2.Estimate_DateTime)
								FROM BI_ASAP_Rep.dbo.Salvage_Estimate SE2 WITH(NOLOCK)
							 WHERE SE2.Estimate_Type_Code = 'ACV' AND SE2.Estimation_Source_Code = 'SP'
								   AND SE2.Salvage_ID = OS.Salvage_ID)
						   
UPDATE #TMP_Open_Stock_ACV SET 
	IAA_ACV = SE3.Estimate_Amount
--SELECT SE3.Salvage_ID ,SE3.Estimate_Amount  AS [IAA_ACV]
FROM BI_ASAP_Rep.dbo.Salvage_Estimate SE3 WITH(NOLOCK)
INNER JOIN #TMP_Open_Stock_ACV OS 
	ON SE3.Salvage_ID = OS.Salvage_ID
WHERE SE3.Estimate_Type_Code = 'ACV' AND SE3.Estimation_Source_Code = 'IAA' AND
		SE3.Estimate_DateTime = (SELECT MAX(SE4.Estimate_DateTime)
									FROM BI_ASAP_Rep.dbo.Salvage_Estimate SE4 WITH(NOLOCK)
									WHERE SE4.Estimate_Type_Code = 'ACV' AND SE4.Estimation_Source_Code = 'IAA' 
											AND SE4.Salvage_ID = OS.Salvage_ID AND ISNULL(SE4.Estimate_Amount,0) > 0) 

UPDATE #TMP_Open_Stock_ACV SET 
	ECR = SE3.Estimate_Amount
--SELECT SE3.Salvage_ID ,SE3.Estimate_Amount  AS [IAA_ACV]
FROM BI_ASAP_Rep.dbo.Salvage_Estimate SE3 WITH(NOLOCK)
INNER JOIN #TMP_Open_Stock_ACV OS 
	ON SE3.Salvage_ID = OS.Salvage_ID
WHERE SE3.Estimate_Type_Code = 'ECR' AND
		SE3.Estimate_DateTime = (SELECT MAX(SE4.Estimate_DateTime)
									FROM BI_ASAP_Rep.dbo.Salvage_Estimate SE4 WITH(NOLOCK)
									WHERE SE4.Estimate_Type_Code = 'ECR'  
											AND SE4.Salvage_ID = OS.Salvage_ID AND ISNULL(SE4.Estimate_Amount,0) > 0) 
/***************Stock Status****************************/
CREATE TABLE #TMP_SI_Data(
	 [Salvage_ID] [int] NOT NULL
	,[Current_Stock_Status_Code] [char] (3) NULL
	,[Current_Stock_Status_Description] [varchar] (40) NULL
	,[Process_Status_Code] [char] (3) NULL
	,[Process_Status_Desscription] [varchar] (40) NULL
	,[Sold_Ind] [bit] NULL
	,[Title_Problem_Ind] [bit] NULL
	,[Missing_Parts_Ind] [bit] NOT NULL)
INSERT INTO #TMP_SI_Data(Salvage_ID ,Current_Stock_Status_Code ,Current_Stock_Status_Description 
	,Process_Status_Code ,Process_Status_Desscription ,Sold_Ind ,Title_Problem_Ind ,Missing_Parts_Ind)
SELECT SI.Salvage_ID 
		,ISNULL(SI.Status_Code,'ACT') AS Current_Stock_Status_Code 
		,S1.Status_Description AS Current_Stock_Status_Description
		,ISNULL(SI.Process_Status_Code,'ACT') AS Process_Status_Code
		,S2.Status_Description AS Process_Status_Desscription
		,CASE WHEN SI.Sold_DateTime IS NOT NULL THEN 1
			   ELSE 0 END AS Sold_Ind
		,CASE WHEN SI.Process_Status_Code = 'J40' THEN 1
				ELSE 0 END AS Title_Problem_Ind
		,SI.Missing_Parts_Ind		
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Info](
		@MinLSN,@MaxLSN,'all with merge') AS SI
   LEFT OUTER JOIN BI_ASAP_Rep.dbo.[Status] AS S1 WITH(NOLOCK)  
		ON ISNULL(SI.Status_Code,'ACT') = S1.Status_Code
   LEFT OUTER JOIN BI_ASAP_Rep.dbo.[Status] AS S2 WITH(NOLOCK)  
		ON ISNULL(SI.Process_Status_Code,'ACT') = S2.Status_Code
WHERE [__$operation] = 5		

CREATE TABLE #TMP_ASI_Data(
   Salvage_ID	[int] NOT NULL
   ,IsAffiliateStock [int] NOT NULL)
INSERT INTO #TMP_ASI_Data(Salvage_ID ,IsAffiliateStock)
SELECT Salvage_ID, 1 AS IsAffiliateStock 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Affiliate_Salvage_Info]( @MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5

CREATE TABLE #TMP_IsStandardSale_Stage(
  Salvage_ID [int] NOT NULL
  ,Cancel_Salvage_IND tinyint
  ,Assignment_Type_Code char (3)	
  --,Sold_DateTime DATETIME NULL default('2999-12-31')
  ,Sold_DayId [int] NULL default(29991231)
  --,Settlement_Datetime DATETIME NULL default('2999-12-31')
  ,Settlement_DayID [int] NULL default(29991231)
  ,Affiliate_Stock_Ind tinyint default(0)
  ,Standard_Sale_Ind tinyint default(0))
INSERT INTO #TMP_IsStandardSale_Stage (Salvage_ID)
SELECT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
UNION
SELECT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Info](@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
UNION		
SELECT DISTINCT Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Settlement](@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
UNION
SELECT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Affiliate_Salvage_Info](@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5


UPDATE TS SET
--SELECT S.Salvage_ID,
	Cancel_Salvage_Ind = S.Cancel_Salvage_Ind,
	Assignment_Type_Code = S.Assignment_Type_Code
FROM [BI_ASAP_Rep].dbo.Salvage S WITH(NOLOCK)	
	INNER JOIN #TMP_IsStandardSale_Stage TS
		ON S.Salvage_ID = TS.Salvage_ID 
   
UPDATE TS SET
--SELECT TS.Salvage_ID,
	--Sold_DateTime = SI.Sold_DateTime,
	Sold_DayId = CONVERT(INT, CONVERT(CHAR(8), SI.Sold_DateTime , 112))
FROM [BI_ASAP_Rep].[dbo].[Salvage_Info] AS SI WITH(NOLOCK)
	INNER JOIN #TMP_IsStandardSale_Stage AS TS 
		ON TS.Salvage_ID = SI.Salvage_ID
	WHERE SI.Sold_DateTime IS NOT NULL 
	
	
	
UPDATE TS SET
--SELECT TMP.Salvage_ID,
	--Settlement_Datetime = TMP.Settlement_Date,
	Settlement_DayID = TMP.Settlement_Day_ID
FROM  #TMP_IsStandardSale_Stage AS TS
	INNER JOIN (SELECT SS.Salvage_ID ,Max(SU.Settlement_Date) AS Settlement_Date,
				CONVERT(INT, CONVERT(CHAR(8), Max(SU.Settlement_Date) , 112)) AS Settlement_Day_ID
				FROM BI_ASAP_Rep.dbo.Salvage_Settlement AS SS WITH (NOLOCK)
					INNER JOIN BI_ASAP_Rep.dbo.Settlement_Unit AS SU WITH(NOLOCK)
						ON	SS.Settlement_Unit_ID = SU.Settlement_Unit_ID AND SS.Status_Code = 'FSF' 
							AND	SS.Settlement_Type_Code	<>  'PB' AND SU.Payment_Status_Code IN ('UPD', 'PD')
					INNER JOIN #TMP_IsStandardSale_Stage S
						ON SS.Salvage_ID = S.Salvage_ID
				GROUP BY SS.Salvage_ID) AS TMP
	ON TMP.Salvage_ID = TS.Salvage_ID

UPDATE TS SET
--SELECT TS.Salvage_ID, 
	 Affiliate_Stock_Ind = 1
FROM [BI_ASAP_Rep].[dbo].[Affiliate_Salvage_Info] AS ASI WITH(NOLOCK)
	INNER JOIN #TMP_IsStandardSale_Stage AS TS 
		ON TS.Salvage_ID = ASI.Salvage_ID

UPDATE TS SET
--SELECT TS.Salvage_ID,
Standard_Sale_Ind = CASE WHEN (Sold_DayId <= (SELECT CONVERT(INT, CONVERT(CHAR(8),BusinessDateTime, 112)) FROM dbo.BusinessDay WITH(NOLOCK))
											AND Affiliate_Stock_Ind = 0)
						      OR
						      (Settlement_DayID <= (SELECT CONVERT(INT, CONVERT(CHAR(8),BusinessDateTime, 112)) FROM dbo.BusinessDay WITH(NOLOCK))
												AND Affiliate_Stock_Ind = 1)
					  THEN 1 ELSE 0 END
FROM #TMP_IsStandardSale_Stage AS TS
	WHERE Cancel_Salvage_IND = 0 AND Assignment_Type_Code NOT IN ( 'TTL', 'TOW')
	
--Current Buyer_ID
CREATE TABLE #TMP_Current_Buyer_ID(
	  [Salvage_ID] [int] NOT NULL
	, [Buyer_Id] [int] NULL)
INSERT INTO #TMP_Current_Buyer_ID(Salvage_ID, Buyer_Id)
SELECT DISTINCT SS.Salvage_ID ,ISNULL(SS.Buyer_ID,0) AS Buyer_ID
FROM [BI_ASAP_Rep].cdc.dbo_Salvage_Sale_CT AS SS WITH(NOLOCK)
		INNER JOIN 	(SELECT Salvage_ID, MAX(Sold_DateTime) AS Latest_Sold_DateTime 
					 FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Sale](
								@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5 
					 GROUP BY Salvage_ID) TSS
		ON SS.Salvage_ID = TSS.Salvage_ID AND SS.Sold_DateTime = TSS.Latest_Sold_DateTime
	  WHERE SS.[__$operation] IN (2,4)


CREATE TABLE #TMP_SEH_Data_SalvageIDs( [Salvage_ID] [int] NOT NULL)
INSERT INTO #TMP_SEH_Data_SalvageIDs(Salvage_ID)
SELECT DISTINCT Salvage_ID  
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](
		@MinLSN,@MAXLSN,'all with merge') 
	WHERE Salvage_Event_Code IN ('BYCN','BYRE','FSEN','STLV','SAL1') AND [__$operation] = 5

CREATE TABLE #TMP_SEH_Data_BuyerRenegeCancel(
	  Salvage_ID [int] NOT NULL
	, Buyer_Renged_Ind [tinyint] NOT NULL
	, Buyer_Renged_After_Settlement_Ind [tinyint] NOT NULL
	, Buyer_Renged_Count [tinyint] NOT NULL
	, Buyer_Cancel_Ind [tinyint] NOT NULL
	, Buyer_Cancel_After_Settlement_Ind [tinyint] NOT NULL
	, Buyer_Cancel_Count [tinyint] NOT NULL
	, Buyer_Renged_Cancel_Count [tinyint] NOT NULL
	, Buyer_Renged_Cancel_Count_Before_Settlement [tinyint] NOT NULL
	, MAX_BuyerRenegeCancelDateTime [datetime] NOT NULL
    , Buyer_Reneged_Type [char] (4))
INSERT INTO #TMP_SEH_Data_BuyerRenegeCancel(Salvage_ID ,Buyer_Renged_Ind , Buyer_Renged_After_Settlement_Ind
	, Buyer_Renged_Count , Buyer_Cancel_Ind , Buyer_Cancel_After_Settlement_Ind , Buyer_Cancel_Count 
	, Buyer_Renged_Cancel_Count , Buyer_Renged_Cancel_Count_Before_Settlement 
	, MAX_BuyerRenegeCancelDateTime , Buyer_Reneged_Type)
SELECT TSEH.Salvage_ID
	   ,MAX(CASE WHEN DSE.EventCode IN ('BYRE')
			     THEN 1 ELSE 0	END) Buyer_Renged_Ind
	   ,MAX(CASE WHEN DSE.EventCode IN ('BYRE') AND FSE.IsProviderReported = 0
				 THEN 1 ELSE 0	END) Buyer_Renged_After_Settlement_Ind
	   ,ISNULL(SUM(CASE WHEN DSE.EventCode IN ('BYRE')
						THEN 1 END),0) Buyer_Renged_Count
	   ,MAX(CASE WHEN DSE.EventCode IN ('BYCN')
				 THEN 1 ELSE 0	END) Buyer_Cancel_Ind
	   ,MAX(CASE WHEN DSE.EventCode IN ('BYCN') AND FSE.IsProviderReported = 0
					  THEN 1 ELSE 0	END) Buyer_Cancel_After_Settlement_Ind
	   ,ISNULL(SUM(CASE WHEN DSE.EventCode IN ('BYCN')
				THEN 1 END),0) Buyer_Cancel_Count
	   ,ISNULL(SUM(CASE WHEN DSE.EventCode IN ('BYRE','BYCN')
				THEN 1 END),0) Buyer_Renged_Cancel_Count
	   ,ISNULL(SUM(CASE WHEN DSE.EventCode IN ('BYRE','BYCN') AND FSE.IsProviderReported = 1
				THEN 1 END),0) Buyer_Renged_Cancel_Count_Before_Settlement
	   ,ISNULL(MAX(CASE WHEN DSE.EventCode IN ('BYRE','BYCN') 
				THEN FSE.EventDateTime END),'2999-12-31') MAX_BuyerRenegeCancelDateTime
	   ,'' AS Buyer_Reneged_Type
FROM dbo.FactStockEvent AS FSE WITH(NOLOCK)
	INNER JOIN dbo.DimStockEventType AS DSE WITH(NOLOCK)
		ON FSE.EventTypeID = DSE.EventTypeID
	INNER JOIN #TMP_SEH_Data_SalvageIDs AS TSEH WITH(NOLOCK)
		ON TSEH.Salvage_ID = FSE.ASAPSalvageID
WHERE DSE.EventCode IN ('BYRE','BYCN','FSEN','STLV')
GROUP BY TSEH.Salvage_ID
ORDER BY 1

CREATE UNIQUE CLUSTERED INDEX IX_TMP_SEH_Data_BuyerRenegeCancel
    ON #TMP_SEH_Data_BuyerRenegeCancel (Salvage_ID) 
	WITH FILLFACTOR = 100

UPDATE T SET
--SELECT T.Salvage_ID,
       Buyer_Reneged_Type = CASE WHEN DSE.EventCode = 'BYRE' AND  FSE.EventDateTime = T.MAX_BuyerRenegeCancelDateTime
										THEN 'BYRE'
								 WHEN DSE.EventCode = 'BYCN' AND  FSE.EventDateTime = T.MAX_BuyerRenegeCancelDateTime
										THEN 'BYCN' END
FROM dbo.FactStockEvent AS FSE WITH(NOLOCK)
	INNER JOIN dbo.DimStockEventType AS DSE WITH(NOLOCK)
		ON FSE.EventTypeID = DSE.EventTypeID
	INNER JOIN #TMP_SEH_Data_BuyerRenegeCancel  AS T
	ON FSE.ASAPSalvageID = T.Salvage_ID AND FSE.EventDateTime = T.MAX_BuyerRenegeCancelDateTime
WHERE DSE.EventCode IN ('BYRE','BYCN')


-- Gets the data from Salvage_Checkin table using the CDC Current table and inserts it into a Temp table. 	 
-- To get the Agreement Term Type Code and Description 
CREATE TABLE #TMP_SCI_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Agreement_Term_Type_Code] [char](3) NOT NULL
	, [Agreement_Term_Type_Description] [varchar](40) NOT NULL 
	, [Previous_Agreement_Term_Type_Code] [char](3) NULL DEFAULT('')
	, [Previous_Agreement_Term_Type_Description] [varchar](40) NULL DEFAULT('')
	, Flipped_to_PA_Ind [tinyint] DEFAULT(0) 
	, Agreement_Changed_Ind [tinyint] DEFAULT(0)) 
INSERT INTO #TMP_SCI_Data(Salvage_ID ,Agreement_Term_Type_Code ,Agreement_Term_Type_Description)
SELECT  DISTINCT SCI.Salvage_ID ,SCI.Agreement_Term_Type_Code ,ATT.Agreement_Term_Type_Description 
FROM [BI_ASAP_Rep].dbo.Salvage_Contract_Info AS SCI  WITH(NOLOCK)
	INNER JOIN
		(SELECT DISTINCT [Salvage_ID]
			FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Contract_Info](
				@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] IN (5)) T_SCI 
	ON T_SCI.Salvage_ID = SCI.Salvage_ID 
	INNER JOIN [BI_ASAP_Rep].[dbo].[Agreement_Term_Type] AS ATT WITH(NOLOCK)
		ON SCI.Agreement_Term_Type_Code = ATT.Agreement_Term_Type_Code 
	
CREATE UNIQUE CLUSTERED INDEX IX_TMP_SCI_Data
    ON #TMP_SCI_Data (Salvage_ID) 
	WITH FILLFACTOR = 100



CREATE TABLE #TMP_PATC_Data(
	 [Salvage_ID] [int] NOT NULL
	,Current_Agreement_Term_Type_Code [char](3) NOT NULL
    ,AgreementTermTypeCode [char](3) NOT NULL)
INSERT INTO #TMP_PATC_Data(Salvage_ID ,Current_Agreement_Term_Type_Code ,AgreementTermTypeCode)     
SELECT  T_SCI.Salvage_ID , T_SCI.Agreement_Term_Type_Code AS Current_Agreement_Term_Type_Code 
		,DS.AgreementTermTypeCode
FROM #TMP_SCI_Data  AS T_SCI 
    INNER JOIN dbo.DimStock DS WITH(NOLOCK)
		ON T_SCI.Salvage_ID = DS.ASAPSalvageID


UPDATE SCID SET
--SELECT SCID.Salvage_ID,Current_Agreement_Term_Type_Code , 
	 Previous_Agreement_Term_Type_Code = TPD.AgreementTermTypeCode 
	,Previous_Agreement_Term_Type_Description = ATT2.Agreement_Term_Type_Description 
	,Agreement_Changed_Ind = 1
	,Flipped_to_PA_Ind = CASE WHEN Current_Agreement_Term_Type_Code = 'PAA' THEN 1 ELSE 0 END 
FROM #TMP_SCI_Data AS SCID
	INNER JOIN #TMP_PATC_Data TPD
		ON SCID.Salvage_ID = TPD.Salvage_ID 
    INNER JOIN [BI_ASAP_Rep].[dbo].[Agreement_Term_Type] AS ATT2 WITH(NOLOCK)
		ON TPD.AgreementTermTypeCode = ATT2.Agreement_Term_Type_Code		
WHERE TPD.Current_Agreement_Term_Type_Code <> TPD.AgreementTermTypeCode	
	

 

/***** Populating IsOnHold from from Salvage_Event table - EDW 5.2-Sprint 10*****/	 

CREATE TABLE #TMP_Salvage_Event_History_Data_HOLD([Salvage_ID] [int] NOT NULL)
INSERT INTO #TMP_Salvage_Event_History_Data_HOLD(Salvage_ID)
SELECT DISTINCT Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5 AND Salvage_Event_Code IN ('HDCF','HDRM','HDCN','HDCR','ASCN',  'HLCO','HRCO')

CREATE TABLE #TMP_Salvage_Event_History_Data_HOLD2([Salvage_ID] [int] NOT NULL, OnHoldDate DATETIME, OffHoldDate DATETIME, IsOnHold tinyint)
INSERT INTO #TMP_Salvage_Event_History_Data_HOLD2(Salvage_ID,OnHoldDate, OffHoldDate)
SELECT DISTINCT SEH.Salvage_ID, 
	MAX(CASE WHEN Salvage_Event_Code IN ('HDCF') THEN Event_DateTime ELSE NULL END) AS OnHoldDate,
	MAX(CASE WHEN Salvage_Event_Code IN ('HDRM','HDCN','HDCR','ASCN') THEN Event_DateTime ELSE NULL END) AS OffHoldDate
FROM [BI_ASAP_Rep].dbo.Salvage_Event_History SEH WITH (NOLOCK)
	INNER JOIN #TMP_Salvage_Event_History_Data_HOLD SEHDH WITH (NOLOCK)
		ON SEH.Salvage_ID = SEHDH.Salvage_ID 
WHERE Salvage_Event_Code IN ('HDCF','HDRM','HDCN','HDCR','ASCN')
GROUP BY SEH.Salvage_ID

CREATE TABLE #TMP_Salvage_Event_History_Data_HOLD3([Salvage_ID] [int] NOT NULL, OnHoldDate DATETIME, OffHoldDate DATETIME, IsOnHold tinyint)
INSERT INTO #TMP_Salvage_Event_History_Data_HOLD3(Salvage_ID,OnHoldDate, OffHoldDate)
SELECT DISTINCT SEH.Salvage_ID, 
	MAX(CASE WHEN Salvage_Event_Code IN ('HLCO') THEN Event_DateTime ELSE NULL END) AS OnHoldDate,
	MAX(CASE WHEN Salvage_Event_Code IN ('ASCN','HRCO') THEN Event_DateTime ELSE NULL END) AS OffHoldDate
FROM [BI_ASAP_Rep].dbo.Salvage_Event_History SEH WITH (NOLOCK)
	INNER JOIN #TMP_Salvage_Event_History_Data_HOLD SEHDH WITH (NOLOCK)
		ON SEH.Salvage_ID = SEHDH.Salvage_ID 
WHERE Salvage_Event_Code IN ('ASCN',  'HLCO','HRCO')
GROUP BY SEH.Salvage_ID

UPDATE H2 SET 
--SELECT 
IsOnHold = CASE 
	WHEN OnHoldDate IS NULL THEN 0 
	WHEN OnHoldDate IS NOT NULL AND OffHoldDate IS NULL THEN 1 
	WHEN OnHoldDate IS NOT NULL AND OffHoldDate IS NOT NULL AND OnHoldDate > OffHoldDate THEN 1  
	ELSE 0 END --AS IsOnHold ,H2.*,Stock_number
FROM #TMP_Salvage_Event_History_Data_HOLD2 H2
	INNER JOIN BI_ASAP_Rep.dbo.Salvage S 
		ON H2.Salvage_ID = s.Salvage_ID

UPDATE H2 SET 
--SELECT
IsOnHold =1
FROM #TMP_Salvage_Event_History_Data_HOLD3 H3
	INNER JOIN BI_ASAP_Rep.dbo.Salvage S 
		ON H3.Salvage_ID = s.Salvage_ID
	INNER JOIN #TMP_Salvage_Event_History_Data_HOLD2 H2
		ON H3.Salvage_ID = H2.Salvage_ID
WHERE ISNULL(H2.IsOnHold,0) = 0 AND 
	CASE WHEN H3.OnHoldDate IS NULL THEN 0 
		 WHEN H3.OnHoldDate IS NOT NULL AND H3.OffHoldDate IS NULL THEN 1 
		 WHEN H3.OnHoldDate IS NOT NULL AND H3.OffHoldDate IS NOT NULL AND H3.OnHoldDate > H3.OffHoldDate THEN 1  
		 ELSE 0 END = 1

INSERT INTO #TMP_Salvage_Event_History_Data_HOLD2(Salvage_ID,IsOnHold,OffHoldDate,OnHoldDate)
SELECT Salvage_ID,CASE 
	WHEN H3.OnHoldDate IS NULL THEN 0 
	WHEN H3.OnHoldDate IS NOT NULL AND H3.OffHoldDate IS NULL THEN 1 
	WHEN H3.OnHoldDate IS NOT NULL AND H3.OffHoldDate IS NOT NULL AND H3.OnHoldDate > H3.OffHoldDate THEN 1  
	ELSE 0 END,OffHoldDate,OnHoldDate from #TMP_Salvage_Event_History_Data_HOLD3 H3
WHERE Salvage_ID NOT IN (SELECT Salvage_ID FROM #TMP_Salvage_Event_History_Data_HOLD2)

     
/******************HOld & Court Order Hold Counts***********************************/

--IF OBJECT_ID('tempdb..#TMP_CT_SEH_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SEH_SalvageIDs
CREATE TABLE #TMP_CT_SEH_SalvageIDs
([Salvage_ID] [INT] NOT NULL)


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
  
--IF OBJECT_ID('tempdb..#TMP_Hold_Counts') IS NOT NULL DROP TABLE #TMP_Hold_Final_Dates
 CREATE TABLE #TMP_Hold_Counts
 (Salvage_ID        [INT] NOT NULL,
  Hold_Counts       [INT] NULL  ) 
  
--IF OBJECT_ID('tempdb..#TMP_CourtOrder_Hold_Counts') IS NOT NULL DROP TABLE #TMP_Hold_Final_Dates
CREATE TABLE #TMP_CourtOrder_Hold_Counts
(Salvage_ID        [INT] NOT NULL,
 CourtOrder_Hold_Counts       [INT] NULL)
  
 

--DECLARE @MinLSN Binary(10)
--		,@MaxLSN Binary (10)
--SELECT @MinLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Salvage_Event_History')
--SELECT @MaxLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()
INSERT INTO #TMP_CT_SEH_SalvageIDs(Salvage_ID)
SELECT DISTINCT Salvage_ID
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge')
WHERE  Salvage_Event_Code IN ('HDCF','HDRM','HDCN','HDCR','ASCN','HLCO')


INSERT INTO #TMP_FactStockEvent
(ASAPSalvageID , EventDateTime ,EventCode)
SELECT ASAPSalvageID, EventDateTime, EventCode
FROM dbo.FactStockEvent as FSE WITH (NOLOCK)
	INNER JOIN dbo.DimStockEventType as DS WITH (NOLOCK)
		ON FSE.EventTypeID = DS.EventTypeID          
	INNER JOIN #TMP_CT_SEH_SalvageIDs AS TMP
		ON TMP.Salvage_ID = FSE.ASAPSalvageID
WHERE EventCode in ('HDCF','HDRM','HDCN','HDCR','ASCN')	  


INSERT INTO #TMP_Hold_Dates
(Salvage_ID , HoldCode ,RelCode ,Hold_Start_Date ,Hold_End_Date)
SELECT S.Salvage_ID
	,SEH1.EventCode AS HoldCode
	,SEH5.EventCode AS RelCode
	,Hold_Start_Date = SEH1.EventDateTime
	,Hold_End_Date = (SEH5.EventDateTime)
FROM BI_ASAP_Rep.dbo.Salvage as S WITH (NOLOCK)
	INNER JOIN #TMP_FactStockEvent SEH1 WITH (NOLOCK)
		ON S.Salvage_ID = SEH1.ASAPSalvageID AND SEh1.EventCode = 'HDCF'
	LEFT OUTER JOIN #TMP_FactStockEvent SEH3 WITH (NOLOCK)
		ON SEH1.ASAPSalvageID = SEH3.ASAPSalvageID AND SEH3.EventCode = 'HDCF'
			AND SEH3.EventDateTime = (SELECT TOP 1 SEH2.EventDateTime
										FROM #TMP_FactStockEvent as SEH2 WITH (NOLOCK)
										WHERE SEH2.EventCode = 'HDCF'
										AND SEH2.ASAPSalvageID = SEH1.ASAPSalvageID
										AND SEH2.EventDateTime > SEH1.EventDateTime)
	LEFT OUTER JOIN #TMP_FactStockEvent as SEH5 WITH (NOLOCK) 
		ON SEH1.ASAPSalvageID = SEH5.ASAPSalvageID AND SEH5.EventCode in ('HDRM','HDCN','HDCR','ASCN')
		AND SEH5.EventDateTime = (SELECT Top 1 SEH4.EventDateTime FROM #TMP_FactStockEvent as SEH4
									WHERE SEH4.EventCode in ('HDRM','HDCN','HDCR','ASCN')
										AND SEH4.ASAPSalvageID = SEH1.ASAPSalvageID
										AND SEH4.EventDateTime > SEH1.EventDateTime
										AND SEH4.EventDateTime < ISNULL(SEH3.EventDateTime,'2999-12-31')
										ORDER BY 
										   CASE WHEN EventCode =  'HDRM' THEN 1
												WHEN EventCode =  'HDCN' THEN 2
												WHEN EventCode =  'HDCR' THEN 3
												WHEN EventCode =  'ASCN' THEN 4 END,                          
												SEH4.EventDateTime )
									
INSERT INTO #TMP_Hold_Final_Dates
(Salvage_ID , HoldCode ,RelCode , Hold_Start_Date ,Hold_End_Date)
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


INSERT INTO #TMP_Hold_Counts(Salvage_ID , Hold_Counts)
SELECT Salvage_ID,ISNULL(COUNT(HoldCode),0) AS Count_Of_Holds
FROM #TMP_Hold_Final_Dates GROUP BY Salvage_ID  


INSERT INTO #TMP_CourtOrder_Hold_Counts(Salvage_ID ,CourtOrder_Hold_Counts)
SELECT ASAPSalvageID 
	,ISNULL(SUM(CASE WHEN DSE.EventCode IN ('HLCO') THEN 1 END),0) CourtOrder_Hold_Count
FROM #TMP_CT_SEH_SalvageIDs as T
	INNER JOIN dbo.FactStockEvent as FSE WITH (NOLOCK)				
		ON T.Salvage_ID = FSE.ASAPSalvageID
	INNER JOIN dbo.DimStockEventType as DSE WITH (NOLOCK)
		ON DSE.EventTypeID = FSE.EventTypeID
WHERE DSE.EventCode in ('HLCO')
GROUP BY ASAPSalvageID


--****************************BUlkSale logic ************************************************************

CREATE TABLE #TMP_BUlk_Sale_Salvage_ID(Salvage_ID INT NOT NULL)
INSERT INTO #TMP_BUlk_Sale_Salvage_ID(Salvage_ID)
(SELECT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5 AND Salvage_Event_Code IN ('SAL9','BYRE','BYCN'))


CREATE TABLE #TMP_Bulk_Datetime(Salvage_ID INT NOT NULL, Bulk_Sale_Date Datetime,Buyer_Renege_Cancel_Sale_Datetime Datetime)
INSERT INTO #TMP_Bulk_Datetime(Salvage_ID,Bulk_Sale_Date,Buyer_Renege_Cancel_Sale_Datetime)
SELECT ASAPSalvageID as Salvage_ID,
ISNULL(Max(CASE WHEN DSE.EventCode='SAL9' THEN FSE.EventDateTime
                              END),'1900-01-01') Bulk_Sale_Date,
ISNULL(Max(CASE WHEN DSE.EventCode IN('BYCN', 'BYRE') THEN FSE.EventDateTime
                              END),'1900-01-01') Buyer_Renege_Cancel_Sale_Datetime 
FROM dbo.FactStockEvent AS FSE WITH (NOLOCK)
            INNER JOIN dbo.DimStockEventType as DSE WITH (NOLOCK)
      ON FSE.EventTypeID = DSE.EventTypeID
            INNER JOIN #TMP_BUlk_Sale_Salvage_ID SI
      ON FSE.ASAPSalvageID = SI.Salvage_ID
WHERE EventCode IN ('BYCN','BYRE','SAL9') 
      AND IsMostRecentEvent = 1
GROUP BY ASAPSalvageID

CREATE TABLE #TMP_Bulk_Sale_Ind(Salvage_ID INT NOT NULL,Bulk_Sale_Ind INT)
INSERT INTO #TMP_Bulk_Sale_Ind(Salvage_ID,Bulk_Sale_Ind)
SELECT Salvage_ID,
CASE WHEN Bulk_Sale_Date > Buyer_Renege_Cancel_Sale_Datetime THEN 1 
     ELSE 0 END as Bulk_Sale_Ind
FROM #TMP_Bulk_Datetime


--********************Temp table used to Populate GrossReturn ************-- 

CREATE TABLE #TMP_Gross_Return_Salvage_ID(
	 Salvage_ID INT NOT NULL
	,Sale_Amount money NULL)
INSERT INTO #TMP_Gross_Return_Salvage_ID(Salvage_ID)

SELECT Salvage_ID 
FROM [BI_ASAP_Rep].dbo.Salvage WITH(NOLOCK)
WHERE Salvage_ID IN (SELECT DISTINCT Salvage_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Sale](@MinLSN,@MaxLSN,'all with merge'))
				--Should not have [__$operation] = 5 as records are deleted from table after Renege/Cancel
--Only the Salvage_ID's present in Salvage table are picked, in order not pick the Archived Salvage_ID's. 
UNION
SELECT DISTINCT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Expense](@MinLSN,@MaxLSN,'all with merge') 
WHERE Charge_Type_ID = 315 AND [__$operation] = 5
UNION
SELECT DISTINCT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Settlement](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5
UNION
SELECT DISTINCT Salvage_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5



UPDATE GRS SET
--SELECT T.Salvage_ID,
	Sale_Amount = SS.Sale_Amount 
FROM #TMP_Gross_Return_Salvage_ID GRS 
INNER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Settlement] AS SS WITH(NOLOCK)
	ON SS.Salvage_ID = GRS.Salvage_ID
	INNER JOIN 
  		(SELECT GRS.Salvage_ID ,MAX(SE.Salvage_Settlement_ID) AS Max_Salvage_Settlement_ID
		 FROM [BI_ASAP_Rep].[dbo].[Salvage_Settlement] SE WITH(NOLOCK)
		 	 INNER JOIN #TMP_Gross_Return_Salvage_ID AS GRS
			 ON SE.Salvage_ID = GRS.Salvage_ID	   		    
		  GROUP BY GRS.Salvage_ID) T
	 ON SS.Salvage_ID = T.Salvage_ID AND SS.Salvage_Settlement_ID = T.Max_Salvage_Settlement_ID 


--**********************Advance Storage Info  Objects ****************************

CREATE TABLE #TMP_Advance_Storage_Info_ID
(Advance_Storage_Info_Id int not null)


CREATE TABLE #TMP_AdvanceStorage(AdvanceStorageInfoID int
	 ,Salvage_ID int 
	,AdvanceRateType nchar(4) 
	,IAAAdvanceRateType nchar(4)
	,AdvanceStorageAmount money
	,AdvanceStorageTax money
	,IAAAdvanceStorageAmount money
	,IAAAdvanceStorageTax money
	,DaysInAdvanceStorageProvider decimal(18,9)
	,DaysInAdvanceStorageIAA decimal(18,9)
	,DaysInAdvanceStorageTotal decimal(18,9))


INSERT INTO #TMP_Advance_Storage_Info_ID
(Advance_Storage_Info_Id)
(SELECT   Advance_Storage_Info_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Advance_Storage_Info]
(@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5
UNION 
SELECT   Advance_Storage_Info_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Advance_Storage_Info_Detail]
(@MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5)

INSERT INTO #TMP_AdvanceStorage
(AdvanceStorageInfoID,Salvage_ID ,AdvanceRateType ,IAAAdvanceRateType ,AdvanceStorageAmount ,AdvanceStorageTax 
	,IAAAdvanceStorageAmount ,IAAAdvanceStorageTax ,DaysInAdvanceStorageProvider ,DaysInAdvanceStorageIAA 
	,DaysInAdvanceStorageTotal)
SELECT MAX(ASI.Advance_Storage_Info_ID),Salvage_ID
	,MAX(CASE WHEN ASID.Responsible_Party_Code = 'SPR' THEN ASID.Storage_Location_Rate_Type_Code ELSE '' END) AS AdvanceRateType
	,MAX(CASE WHEN ASID.Responsible_Party_Code = 'IAA' THEN ASID.Storage_Location_Rate_Type_Code ELSE '' END) AS IAAAdvanceRateType
	,SUM(CASE WHEN ASID.Responsible_Party_Code = 'SPR' THEN ASID.Total_Storage_Amount ELSE 0 END) AS AdvanceStorageAmount
	,SUM(CASE WHEN ASID.Responsible_Party_Code = 'SPR' THEN ASID.Tax_Amount ELSE 0 END) AS AdvanceStorageAmountTax
	,SUM(CASE WHEN ASID.Responsible_Party_Code = 'IAA' THEN ASID.Total_Storage_Amount ELSE 0 END) AS IAAAdvanceStorageAmount
	,SUM(CASE WHEN ASID.Responsible_Party_Code = 'IAA' THEN ASID.Tax_Amount ELSE 0 END) AS IAAAdvanceStorageTax
	,SUM(CASE WHEN ASID.Responsible_Party_Code = 'SPR' THEN ASID.Number_Storage_Days ELSE 0 END) AS DaysInAdvanceStorageProvider
	,SUM(CASE WHEN ASID.Responsible_Party_Code = 'IAA' THEN ASID.Number_Storage_Days ELSE 0 END) AS DaysInAdvanceStorageIAA
	,ASI.Number_Storage_Days AS DaysInAdvanceStorageTotal
FROM BI_ASAP_Rep.dbo.Advance_Storage_Info AS ASI WITH(NOLOCK)
	INNER JOIN BI_ASAP_Rep.dbo.Advance_Storage_Info_Detail AS ASID WITH(NOLOCK)
		ON ASI.Advance_Storage_Info_ID = ASID.Advance_Storage_Info_ID
	INNER JOIN #TMP_Advance_Storage_Info_ID AS TASID WITH(NOLOCK)
		ON ASI.Advance_Storage_Info_ID = TASID.Advance_Storage_Info_Id
GROUP BY Salvage_ID ,ASI.Number_Storage_Days

-- Gets the data from Salvage_Buyer_Finance table using the CDC function and inserts it into a Temp table. 
  CREATE TABLE #TMP_SBF_Data(
	  Salvage_ID [int] NOT NULL
	,Finance_Company_Code [varchar](10) NOT NULL
	,Finance_Company_Code_ASAP [varchar](10) NOT NULL
	,Finance_Amount [money] NULL
	,AFCFlooringSource VARCHAR(50) NULL)
INSERT INTO #TMP_SBF_Data (Salvage_ID ,Finance_Company_Code,Finance_Company_Code_ASAP ,Finance_Amount,AFCFlooringSource)
SELECT Salvage_ID 
   ,CASE WHEN Finance_status <> 'can' AND Finance_Company_Code = 'oth' THEN 'Other'
		 WHEN Finance_status <> 'can' AND Finance_Company_Code <> 'oth' THEN Finance_Company_Code
		 ELSE 'None' END AS Finance_Company_Code
   ,Finance_Company_Code AS Finance_Company_Code_ASAP
   ,CASE WHEN Finance_status = 'can' THEN 0 ELSE Finance_Amount END AS Finance_Amount
   ,CASE WHEN Finance_Company_Code IN ('AFC','AFB') THEN 'ASAP' ELSE '' END AS AFCFlooringSource
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Buyer_Finance](
   @MinLSN,@MaxLSN,'all with merge')  
   WHERE [__$operation] = 5 

UPDATE SFB SET AFCFlooringSource = AFC.AlphaID
--SELECT * 
FROM #TMP_SBF_Data SFB WITH (NOLOCK) INNER JOIN
[BI_ASAP_Rep].[dbo].[Salvage_Note] SN WITH (NOLOCK)
	ON SFB.Salvage_ID = SN.Salvage_ID
INNER JOIN (SELECT KeyType,AlphaID FROM dbo.CodesTable WITH (NOLOCK) WHERE CodeType = 'AFCFlooringSource') AFC
	ON ISNULL(LEFT(SN.Note_Text,4),'ASAP') = AFC.KeyType
  WHERE Salvage_Event_Category_Code = 'BPY'
  --AND LEFT(Note_Text,4) IN ('ASAP','AUCT','MSTE','MAPP','MPAD') 
  AND Finance_Company_Code_ASAP IN ('AFC','AFB')

--********************************Title Problem Ind*******************************--

--DECLARE @MinLSN Binary(10)
--            ,@MaxLSN Binary (10)

--SELECT @MinLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Salvage_Info')
--SELECT @MaxLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()



CREATE TABLE #TMP_Title_Problem_Ind
(Salvage_ID INT NOT NULL,
Status_Datetime DATETIME NULL,
Process_Code VARCHAR(10) NULL,
Status_Code  VARCHAR(10) NULL)
INSERT INTO #TMP_Title_Problem_Ind
(Salvage_ID)
SELECT Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Info](@MinLSN,@MaxLSN,'all with merge')  
   WHERE [__$operation] = 5 
UNION
SELECT DISTINCT Salvage_ID 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Process_Status](@MinLSN,@MaxLSN,'all with merge')  
   WHERE [__$operation] = 5 



UPDATE #TMP_Title_Problem_Ind SET 
--SELECT SPS.Salvage_ID ,
      Status_DateTime = SPS.Status_DateTime ,
      Process_Code = SPS.Process_Code ,
      Status_Code =SPS.Status_Code
FROM #TMP_Title_Problem_Ind TID
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Process_Status SPS WITH(NOLOCK)
      ON  SPS.Salvage_ID = TID.Salvage_ID
WHERE SPS.Process_Code = 'TTL' --AND SPS.Status_Code = 'J40'
      AND  SPS.Status_DateTime = (SELECT MAX(SPS2.Status_DateTime) 
                                         FROM BI_ASAP_Rep.dbo.Salvage_Process_Status SPS2 WITH(NOLOCK)
                                         INNER JOIN #TMP_Title_Problem_Ind TID1
                                                ON TID1.Salvage_ID = SPS2.Salvage_ID
                                         WHERE SPS2.Process_Code = 'TTL' AND SPS.Salvage_ID = SPS2.Salvage_ID)
                              
                              
--*********************** SoldModelYearAge  And SoldModelYearBand **************************************

DECLARE @Sold_Date Datetime
        
SELECT @Sold_Date =  BusinessDateTime  FROM dbo.BusinessDay

CREATE TABLE #TMPSoldModel_SalvageID
(Salvage_ID INT NOT NULL)
INSERT INTO #TMPSoldModel_SalvageID
(Salvage_ID)
SELECT Salvage_ID FROM BI_ASAP_Rep.dbo.Salvage_Info WHERE Sold_DateTime IS NULL
UNION 
SELECT Salvage_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Info](@MinLSN,@MaxLSN,'all with merge')  
   WHERE [__$operation] = 5 
UNION 
SELECT Salvage_ID FROM  
[BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Contract_Info](@MinLSN,@MaxLSN,'all with merge')  SCI
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Provider_Contract SPC 
ON SCI.Salvage_Contract_ID  = SPC.Salvage_Contract_ID
WHERE [__$operation] = 5                                          

CREATE TABLE #TMP_Model
(ASAPSalvageID INT NOT NULL,
Sold_Date Datetime,
Sold_Year INT,
Model_year INT,
Model_Year_Change_Month_Number INT,
Model_Year_Change_Day_Number INT
)

CREATE TABLE #TMP_MODEL_AGE_BAND
(ASAPSalvageID INT NOT NULL
,Model_Year_Band NVARCHAR(8)
,Sold_Model_Year_Age INT
,Model_Year_Band2 NVARCHAR(8))



INSERT INTO #TMP_Model
(ASAPSalvageID,
 Sold_Date,
 Sold_Year,
 Model_year,
 Model_Year_Change_Month_Number,
 Model_Year_Change_Day_Number)
SELECT 
 S.Salvage_ID
,Sold_Date = CASE WHEN IsNULL(Sold_DateTime,'2999-12-31') > DATEADD(D,1,@Sold_Date) THEN @Sold_Date ELSE Sold_DateTime END
,Sold_Year = DATEPART(YEAR,CASE WHEN IsNULL(Sold_DateTime,'2999-12-31') > DATEADD(D,1,@Sold_Date) THEN @Sold_Date ELSE Sold_DateTime END)
,Model_Year= CASE WHEN S.[Model_Year] < 1900 THEN YEAR(GETDATE())-9
			             WHEN S.[Model_Year] > YEAR(GETDATE()) + 2 	THEN YEAR(GETDATE())
			             ELSE ISNULL(S.[Model_Year],0)	END
,Model_Year_Change_Month_Number = CASE WHEN SPC.Model_Year_Change_Month_Number = 0 THEN 12 ELSE SPC.Model_Year_Change_Month_Number END
,Model_Year_Change_Day_Number = CASE WHEN SPC.Model_Year_Change_Day_Number = 0 THEN 31 ELSE SPC.Model_Year_Change_Day_Number END
FROM 
BI_ASAP_Rep.dbo.Salvage S WITH (NOLOCK)
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Info SI WITH (NOLOCK)
ON S.Salvage_ID = SI.Salvage_ID
INNER JOIN #TMPSoldModel_SalvageID TSM
ON S.Salvage_ID = TSM.Salvage_ID
LEFT OUTER  JOIN BI_ASAP_Rep.dbo.Salvage_Contract_Info SCI
ON S.Salvage_ID = SCI.Salvage_ID
LEFT OUTER  JOIN BI_ASAP_Rep.dbo.Salvage_Provider_Contract SPC WITH (NOLOCK)
ON SCI.Salvage_Contract_ID = SPC.Salvage_Contract_ID


INSERT INTO #TMP_MODEL_AGE_BAND
(ASAPSalvageID,
 Model_Year_Band,
 Sold_Model_Year_Age,
 Model_Year_Band2)
SELECT 
ASAPSalvageID 
,Model_Year_band = CASE (CASE WHEN Sold_Date > (convert(datetime,cast(Model_Year_Change_Month_Number as varchar(2)) +'/'+
								   cast(Model_Year_Change_Day_Number as varchar(2))+'/'+
								    cast(Sold_Year as varchar(4)),101))
		        then Sold_Year + 1  else Sold_Year End - (Model_Year))
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
,Sold_Model_Year_Age =  (CASE WHEN  Sold_Date > (convert(datetime,cast(Model_Year_Change_Month_Number as varchar(2)) +'/'+ cast(Model_Year_Change_Day_Number as varchar(2))+'/'+ cast(Sold_Year as varchar(4)),101))
                            THEN Sold_Year + 1 ELSE Sold_Year END  ) - Model_year
,Model_Year_band2 = CASE (CASE WHEN Sold_Date > (convert(datetime,cast(Model_Year_Change_Month_Number as varchar(2)) +'/'+
								   cast(Model_Year_Change_Day_Number as varchar(2))+'/'+
								    cast(Sold_Year as varchar(4)),101))
		        then Sold_Year + 1  else Sold_Year End - (Model_Year))
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
FROM #TMP_Model TM


--***************************Run And Drive Enhancement Ind ************************--

CREATE TABLE #TMP_RunAndDrive_Auctions(Auction_Entry_ID INT NOT NULL)

CREATE TABLE #TMP_Latest_Auction(
	Salvage_ID INT NOT NULL
	,Latest_Auction_Date Datetime NULL)
 
 CREATE TABLE #TMP_RunDriveInd
	(Salvage_ID INT NOT NULL
	,Run_and_Drive_Auction_Ind INT NULL)

 CREATE TABLE #TMP_IsNegotiatedSale
	(Salvage_ID INT NOT NULL
	,IsNegotiatedSale INT NULL)

INSERT INTO #TMP_RunAndDrive_Auctions(Auction_Entry_ID)
SELECT Auction_Entry_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Entry](@MinLSN,@MaxLSN,'all with merge')  
   WHERE [__$operation] = 5 

INSERT INTO #TMP_Latest_Auction(Salvage_ID,Latest_Auction_Date)
SELECT Salvage_ID,MAX(AE.Update_DateTime) AS Latest_Auction_Date
FROM BI_ASAP_Rep.dbo.Auction_Entry AE WITH (NOLOCK)
	INNER JOIN BI_ASAP_Rep.dbo.Auction_ITEM AI WITH (NOLOCK)
		ON AE.Auction_Item_ID = AI.Auction_Item_ID
	INNER JOIN #TMP_RunAndDrive_Auctions TRAI
		ON AE.Auction_Entry_ID = TRAI.Auction_Entry_ID
GROUP BY Salvage_ID

--INSERT INTO #TMP_RunDriveInd(Salvage_ID ,Run_and_Drive_Auction_Ind)
--SELECT AI.Salvage_ID,Max(CAST(AE.Run_And_Drive_Ind AS INT)) AS Run_and_Drive_Auction_Ind
--FROM BI_ASAP_Rep.dbo.Auction_Entry AE 
--	INNER JOIN BI_ASAP_Rep.dbo.Auction_ITEM AI WITH (NOLOCK)
--		ON AE.Auction_Item_ID = AI.Auction_Item_ID
--	INNER JOIN #TMP_Latest_Auction TLA 
--		ON AI.Salvage_ID = TLA.Salvage_ID AND TLA.Latest_Auction_Date = AE.Update_Datetime
--GROUP BY AI.Salvage_ID

INSERT INTO #TMP_RunDriveInd(Salvage_ID ,Run_and_Drive_Auction_Ind)
SELECT AI.Salvage_ID,Max(CAST(AE.Run_And_Drive_Ind AS INT)) AS Run_and_Drive_Auction_Ind	--Get the Max R&D Inf for the stock
			FROM BI_ASAP_Rep.dbo.Auction_Entry AE														--Get the Max R&D Inf for the stock 
				INNER JOIN BI_ASAP_Rep.dbo.Auction_ITEM AI WITH (NOLOCK)								--Get the Max R&D Inf for the stock
					ON AE.Auction_Item_ID = AI.Auction_Item_ID											--Get the Max R&D Inf for the stock
				INNER JOIN (SELECT AI.Salvage_ID,ASched.Auction_Schedule_ID														--Get the Schedule ID for the Max Auction Date for the Stock
							FROM BI_ASAP_Rep.dbo.Auction_ITEM AI WITH (NOLOCK)													--Get the Schedule ID for the Max Auction Date for the Stock
							INNER JOIN BI_ASAP_REP.dbo.Auction_Schedule ASched WITH (NOLOCK)									--Get the Schedule ID for the Max Auction Date for the Stock
								ON AI.Auction_Schedule_ID = ASched.Auction_Schedule_ID											--Get the Schedule ID for the Max Auction Date for the Stock
							INNER JOIN (SELECT Salvage_ID,MAX(ASched.Auction_DateTime) AS Latest_Auction_Date	--Get the MAX Auction Date for the Stock
										FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Entry](@MinLSN,@MaxLSN,'all with merge') AE						--Get the MAX Auction Date for the Stock
										INNER JOIN BI_ASAP_Rep.dbo.Auction_ITEM AI WITH (NOLOCK)				--Get the MAX Auction Date for the Stock
											ON AE.Auction_Item_ID = AI.Auction_Item_ID							--Get the MAX Auction Date for the Stock
										INNER JOIN BI_ASAP_REP.dbo.Auction_Schedule ASched WITH (NOLOCK)		--Get the MAX Auction Date for the Stock
											ON AI.Auction_Schedule_ID = ASched.Auction_Schedule_ID				--Get the MAX Auction Date for the Stock
										GROUP BY Salvage_ID) LAD												--Get the MAX Auction Date for the Stock
								ON AI.Salvage_ID = LAD.Salvage_ID AND ASched.Auction_DateTime = LAD.Latest_Auction_Date) TLA	--Get the Schedule ID for the Max Auction Date for the Stock
												ON AI.Salvage_ID = TLA.Salvage_ID AND TLA.Auction_Schedule_ID = AE.Auction_Schedule_ID--Get the Max R&D Inf for the stock
										GROUP BY AI.Salvage_ID

INSERT INTO #TMP_IsNegotiatedSale(Salvage_ID ,IsNegotiatedSale)
SELECT AI.Salvage_ID,Max(CAST(Negotiated_Sale_Ind AS INT)) AS IsNegotiatedSale
FROM BI_ASAP_Rep.dbo.Auction_Entry AE 
	INNER JOIN BI_ASAP_Rep.dbo.Auction_ITEM AI WITH (NOLOCK)
		ON AE.Auction_Item_ID = AI.Auction_Item_ID
	INNER JOIN #TMP_Latest_Auction TLA 
		ON AI.Salvage_ID = TLA.Salvage_ID AND TLA.Latest_Auction_Date = AE.Update_Datetime
GROUP BY AI.Salvage_ID

INSERT INTO #TMP_IsNegotiatedSale(Salvage_ID ,IsNegotiatedSale)
SELECT 
    DISTINCT Salvage_ID,1 as IsNegotiatedSale
FROM BI_ASAP_Rep.dbo.Salvage_Event_History AS seh WITH(NOLOCK)
WHERE seh.Salvage_Event_Code =  'SA15' 
AND seh.Salvage_ID NOT IN 
	(SELECT DISTINCT seh2.Salvage_ID 
	FROM BI_ASAP_Rep.dbo.Salvage_Event_History AS seh2 WITH(NOLOCK) 
	WHERE seh2.Salvage_ID =  seh.Salvage_ID 
		AND seh2.Event_DateTime >  seh.Event_DateTime 
		AND seh2.Salvage_Event_Code IN ('BYRE', 'BYCN'))
AND seh.Salvage_ID NOT IN (SELECT Salvage_ID FROM #TMP_IsNegotiatedSale)

--**************************** IsECI *******************--

--CREATE TABLE #TMP_ECI_Ind_Data(
--	[ECI_Input_DATA_ID] [int] NOT NULL,
--	[Salvage_ID] [int] NOT NULL,
--	[Process_Status] [varchar](1) NULL,
--	[ECI_Ind] [tinyint] NOT NULL)
--INSERT INTO #TMP_ECI_Ind_Data(ECI_Input_DATA_ID ,Salvage_ID ,Process_Status ,ECI_Ind)
--SELECT T2.ECI_Input_DATA_ID ,T2.Salvage_ID ,Process_Status 
--		,CASE WHEN Process_Status = 'C' THEN 1 ELSE 0 END AS ECI_Ind
--FROM [BI_ASAP_Rep].[dbo].[ECI_Input_DATA] ECI2 WITH(NOLOCK) 
--	INNER JOIN 
--		(SELECT MAX(ECI.ECI_Input_DATA_ID) AS ECI_Input_DATA_ID ,ECI.Salvage_ID
--			FROM [BI_ASAP_Rep].[dbo].[ECI_Input_DATA] ECI WITH(NOLOCK)
--				INNER JOIN (SELECT ECI_Input_Data_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_ECI_Input_DATA](
--								@MinLSN,@MaxLSN,'all with merge') 
--								WHERE [__$operation] = 5 AND Salvage_ID IS NOT NULL) T
--				ON T.ECI_Input_DATA_ID = ECI.ECI_Input_DATA_ID
--			GROUP BY Salvage_ID) T2
--		ON T2.ECI_Input_DATA_ID = ECI2.ECI_Input_DATA_ID AND T2.Salvage_ID = ECI2.Salvage_ID 

CREATE TABLE #TMP_ECI_Ind_Data(
      [Salvage_ID] [int] NOT NULL,
      [ECI_Ind] [tinyint] NOT NULL)
INSERT INTO #TMP_ECI_Ind_Data(Salvage_ID ,ECI_Ind)
SELECT Salvage_ID , 1 AS ECI_Ind
FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge') AS SEH
WHERE  SEH.Salvage_Event_Code='ECIC' AND [__$operation] = 5



---***************************** Salvage Enhancement Indicators *************************

CREATE TABLE #TMP_Enhancement_Salvage_IDs
(Salvage_ID INT NOT NULL)

CREATE TABLE #TMP_ENhancement_IND
(Salvage_ID INT NOT NULL,
 Bandage_Enhancement_IND INT NULL,
 Battery_Service_Enhancement_IND INT NULL,
 Detail_Enhancement_IND INT NULL,
 Flood_Service_Enhancement_IND INT NULL,
 Full_Wrap_Enhancement_IND INT NULL,
 Patch_Enhancement_IND INT NULL,
 Run_And_Drive_Enhancement_IND INT NULL,
 Key_Enhancement_IND INT NULL 
 )

CREATE TABLE #TMP_Airbag_Ind
(Salvage_ID INT NOT NULL
,Airbag_Deployed_Ind INT NULL
,Airbag__Missing_ind  INT NULL)

CREATE NONCLUSTERED INDEX IX_Salvage_ID ON #TMP_Enhancement_Salvage_IDs	(Salvage_ID) 

INSERT INTO #TMP_Enhancement_Salvage_IDs
(Salvage_ID)
SELECT DISTINCT Salvage_ID 
FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider_Charge( @MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5

INSERT INTO #TMP_Enhancement_Salvage_IDs
(Salvage_ID)
SELECT DISTINCT Salvage_ID 
FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Expense( @MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5 and Salvage_ID NOT IN (SELECT Salvage_ID FROM #TMP_Enhancement_Salvage_IDs)

INSERT INTO #TMP_Enhancement_Salvage_IDs
(Salvage_ID)
SELECT DISTINCT Salvage_ID 
FROM 
[BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5 and Salvage_ID NOT IN (SELECT Salvage_ID FROM #TMP_Enhancement_Salvage_IDs)



INSERT INTO #TMP_ENhancement_IND
(Salvage_ID,
 Bandage_Enhancement_IND,
 Battery_Service_Enhancement_IND,
 Detail_Enhancement_IND,
 Flood_Service_Enhancement_IND,
 Full_Wrap_Enhancement_IND,
 Patch_Enhancement_IND,
 Run_And_Drive_Enhancement_IND,
 Key_Enhancement_IND) 

SELECT T.Salvage_ID 
,Bandage_Enhancement_IND = MAX(CASE WHEN CTSPC.Charge_Type_ID IN (127, 270) OR CTSEP.Charge_Type_ID IN (127, 270) OR (CTSE.Charge_Type_ID IN (127, 270) AND SE.Status_Code IN ('WPN','WCP'))
		                           THEN	1 ELSE 0 END)
,Battery_Service_Enhancement_IND = MAX(CASE	WHEN CTSPC.Charge_Type_ID IN (99, 242) OR CTSPC.Charge_Type_ID IN (99, 242) OR(CTSE.Charge_Type_ID IN (99,242) AND SE.Status_Code IN ('WPN','WCP'))
		                                    THEN 1 ELSE	0 END)
,Detail_Enhancement_IND = MAX(CASE WHEN	CTSPC.Charge_Type_ID IN (101,244,104,247,300,301,100,243,113,256) OR CTSEP.Charge_Type_ID IN (101,244,104,247,300,301,100,243,113,256) OR (CTSE.Charge_Type_ID IN (101,244,104,247,300,301,100,243,113,256) AND SE.Status_Code IN ('WPN','WCP'))
		                           THEN	1 ELSE 0 END)
,Flood_Service_Enhancement_IND = MAX(CASE WHEN CTSPC.Charge_Type_ID IN (302,303,498,499,500,501,502,503) OR CTSEP.Charge_Type_ID IN (302,303,498,499,500,501,502,503) OR (CTSE.Charge_Type_ID IN (302,303,498,499,500,501,502,503) AND SE.Status_Code IN ('WPN','WCP'))
		                           THEN	1 ELSE	0 END)
,Full_Wrap_Enhancement_IND = MAX(CASE WHEN CTSPC.Charge_Type_ID IN	(129, 272) OR CTSEP.Charge_Type_ID IN	(129, 272) OR(CTSE.Charge_Type_ID IN (129, 272) AND SE.Status_Code IN ('WPN','WCP'))
		                              THEN 1 ELSE 0	END	)
,Patch_Enhancement_IND = MAX(CASE WHEN CTSPC.Charge_Type_ID IN (128, 271) OR CTSEP.Charge_Type_ID IN (128, 271) OR(CTSE.Charge_Type_ID IN (128, 271) AND SE.Status_Code IN ('WPN','WCP'))
		                          THEN 1 ELSE 0 END)
,Run_And_Drive_Enhancement_IND = MAX(CASE WHEN CTSPC.Charge_Type_ID IN (126, 269) OR CTSEP.Charge_Type_ID IN (126, 269) OR(CTSE.Charge_Type_ID IN (126, 269) AND SE.Status_Code IN ('WPN','WCP'))
		                          THEN 1 ELSE 0 END)	
,Key_Enhancement_IND =  MAX(CASE WHEN CTSPC.Charge_Type_ID IN (249,263) OR CTSEP.Charge_Type_ID IN (249,263) OR (CTSE.Charge_Type_ID IN (249,263) AND SE.Status_Code IN ('WPN','WCP'))
                                   THEN 1 ELSE 0 END)	 
FROM #TMP_Enhancement_Salvage_IDs T 
   LEFT OUTER JOIN  BI_ASAP_Rep.dbo.Salvage_Provider_Charge SPC WITH (NOLOCK)
    ON T.Salvage_ID = SPC.Salvage_ID 
   LEFT OUTER JOIN  [BI_ASAP_Rep].[dbo].[Charge_Type] CTSPC WITH (NOLOCK)
    ON SPC.Charge_Type_ID = CTSPC.Charge_Type_ID
   LEFT OUTER JOIN  BI_ASAP_Rep.dbo.Salvage_Expense SEP WITH (NOLOCK)
    ON T.Salvage_ID = SEP.Salvage_ID
   LEFT OUTER JOIN BI_ASAP_Rep.[dbo].[Charge_Type] CTSEP WITH (NOLOCK)
    ON SEP.Charge_Type_ID = CTSEP.Charge_Type_ID   
   LEFT OUTER JOIN BI_Asap_Rep.dbo.Salvage_Enhancement  AS SE WITH (NOLOCK)
    ON T.Salvage_ID = SE.Salvage_ID 
   LEFT OUTER JOIN BI_ASAP_Rep.dbo.Charge_Type AS CTSE WITH (NOLOCK)
    ON SE.Charge_Type_ID = CTSE.Charge_Type_ID
   GROUP BY 
   T.Salvage_ID 
 
-- Inspect Services
INSERT	INTO #TMP_Enhancement_Salvage_IDs_InspectionServices
		(
		Salvage_ID,
		Salvage_Enhancement_Level_ID,
		Enhancement_Level_ID  
		)
SELECT	[Salvage_ID],
		ISNULL(Salvage_Enhancement_Level_ID,0),
		ISNULL(Enhancement_Level_ID,0)  -- Default to 'None' if not in Salvage_Enhancement_LEvel
FROM 
[BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement_LEvel](@MinLSN,@MaxLSN,'all with merge')
             WHERE [__$operation] = 5

--FROM #TMP_Stock_Data_Stage tsds2 WITH (NOLOCK)
--LEFT OUTER JOIN BI_ASAP_REP.dbo.Salvage_Enhancement_LEvel sel2 WITH (NOLOCK)
--ON tsds2.[ASAPSalvageID] = sel2.Salvage_ID

INSERT INTO #TMP_Airbag_Ind
(Salvage_ID
,Airbag_Deployed_Ind
,Airbag__Missing_ind)
SELECT 
 SFP.Salvage_ID
,CASE WHEN  (MAX(CASE WHEN (SFP.Feature_Value='Deployed') THEN 1 ELSE 0 END)) = 1 THEN 1 ELSE 0 END AS Airbag_Deployed_Ind
,MAX(CASE WHEN  (SFP.Feature_Value='Missing') and SFP.Feature_Code in ('0002', '0003', '0028', '007e','007f'  ) THEN 1 ELSE 0 END) AS Airbag_Missing_Ind
FROM 
BI_ASAP_REP.dbo.Salvage_Feature SFP WITH (NOLOCK)
INNER JOIN (SELECT Salvage_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Feature](@MinLSN,@MaxLSN,'all with merge')
             WHERE [__$operation] = 5  ) SF
ON SFP.Salvage_ID = SF.Salvage_ID 
GROUP BY 
SFP.Salvage_ID


CREATE TABLE #TMP_Stock_Settlement_Unit_Data(	
		[Salvage_ID] [int],	
		[Zero_Deficit_Adjustment_Amount] [decimal](11, 2) NULL,
		[Zero_Deficit_Settlement_Ind] [bit] NULL, 
		[Min_Remit_Discount] [decimal](11, 2) NULL,
		[Min_Remit_Ind]	[bit] NULL	,
		[Net_Split_Adjustment] [Decimal](11,2) NULL,
        [Net_Split_Adjustment_Ind] [BIT] NULL)
INSERT INTO #TMP_Stock_Settlement_Unit_Data(Salvage_ID ,Zero_Deficit_Adjustment_Amount
		,Zero_Deficit_Settlement_Ind ,Min_Remit_Discount ,Min_Remit_Ind,Net_Split_Adjustment,Net_Split_Adjustment_Ind)
SELECT SS.Salvage_ID  
	, SU.Zero_Deficit_Adjustment AS Zero_Deficit_Adjustment_Amount
	, SU.Zero_Deficit_Ind AS Zero_Deficit_Settlement_Ind
	, SU.Min_Remit_Discount
	, SU.Min_Remit_Ind
	, SU.Net_Split_Adjustment
	, SU.Net_Split_Ind
FROM BI_ASAP_Rep.dbo.Salvage_Settlement AS SS WITH (NOLOCK)
	INNER JOIN [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Settlement_Unit](
   @MinLSN,@MaxLSN,'all with merge') AS SU 
		ON	SS.Settlement_Unit_ID = SU.Settlement_Unit_ID 
				AND SS.Status_Code = 'FSF' 
				AND SS.Settlement_Type_Code	<>  'PB' 
				AND su.Payment_Status_Code <> 'VOD' 
	INNER JOIN (
				SELECT SS.Salvage_ID  
				,Max(SU.Settlement_Date) AS Settlement_Date
				FROM BI_ASAP_Rep.dbo.Salvage_Settlement AS SS WITH (NOLOCK)
				INNER JOIN [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Settlement_Unit](
					@MinLSN,@MaxLSN,'all with merge') AS SU
					ON	SS.Settlement_Unit_ID = SU.Settlement_Unit_ID 
							AND SS.Status_Code = 'FSF' 
							AND SS.Settlement_Type_Code	<>  'PB' 
							AND su.Payment_Status_Code <> 'VOD'
				WHERE SU.[__$operation] = 5						
				GROUP BY SS.Salvage_ID ) T
		ON T.Salvage_ID = SS.Salvage_ID AND T.Settlement_Date = SU.Settlement_Date 
WHERE SU.[__$operation] = 5	

--------------------------Adding Assignment Source field for CSA Today----------------------------------------------------
--	IF OBJECT_ID('tempdb..#Tmp_Assignment_Source_CSA') IS NOT NULL DROP TABLE #Tmp_Assignment_Source_CSA

CREATE TABLE #Tmp_Assignment_Source_CSA (Salvage_ID INT, Code char(4))

INSERT INTO #Tmp_Assignment_Source_CSA (Salvage_ID ,Code)
SELECT TOP 1 seh.Salvage_ID, seh.Salvage_Event_Code 
FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_event_history](@MinLSN, @MaxLSN, 'all with merge') AS SEH
WHERE  seh.salvage_event_code IN ('AVIS','BRMU','CHRY','FARM','GEIC','JUCR','USAA','EVAA','CSAT','AASC','ADP', 'AIMS' ,'CCCS' , 'EAUT', 'ESUR', 'PROC' , 'MITC')
	 AND [__$operation] = 5 
ORDER BY seh.[Update_DateTime] DESC

--------------------------Adding Provider Refund Amount----------------------------------------------------
--	IF OBJECT_ID('tempdb..#Tmp_ProviderRefundAmount') IS NOT NULL DROP TABLE #Tmp_ProviderRefundAmount

CREATE TABLE #Tmp_ProviderRefundAmount (Salvage_ID INT, RefundAmount MONEY)

INSERT INTO #Tmp_ProviderRefundAmount (Salvage_ID ,RefundAmount)
SELECT PR.[Salvage_ID], SUM(Amount) AS RefundAmount
  FROM [BI_ASAP_Rep].[dbo].[Provider_Refund] PR WITH (NOLOCK)
  INNER JOIN (SELECT DISTINCT Salvage_ID FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Provider_Refund](@MinLSN, @MaxLSN, 'all with merge') WHERE [__$operation] = 5) NewRefunds 
  ON PR.Salvage_ID = NewRefunds.Salvage_ID
  WHERE [Status_Code] <>'VOD'
  GROUP BY PR.[Salvage_ID]

--------------------------Manufacturer Origin--------------------------

UPDATE DS 
SET DS.ManufacturerOrigin = lo.ManufacturerOrigin
FROM #TMP_Stock_Data_Stage DS 
INNER JOIN LKManufacturerOrgin lo WITH (NOLOCK)
	ON lo.Make = DS.MakeName


/***************PreviousProcessStatus****************************/
CREATE TABLE #TMP_PSS_Data(
	 [Salvage_ID] [int] NOT NULL
	,[Previous_Process_Status_Code] [char] (3) NULL
	,[Previous_Process_Status_Description] [varchar] (40) NULL
	,[Update_DateTime] [DateTime] NULL
	,[RowNumber] [int] NULL)

INSERT INTO #TMP_PSS_Data(
        Salvage_ID  
	   ,Previous_Process_Status_Code 
	   ,Previous_Process_Status_Description 
	   ,Update_DateTime
	   ,RowNumber
	   )
SELECT SI.Salvage_ID 
		,ISNULL(SI.Status_Code,'ACT') AS Previous_Process_Status_Code
		,S1.Status_Description AS Previous_Process_Status_Description
		,SI.Update_DateTime
		,ROW_NUMBER() OVER (PARTITION BY SI.Salvage_Id Order by SI.Update_DateTime DESC) AS RowNumber
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Process_Status](
		@MinLSN,@MaxLSN,'all with merge') AS SI
   LEFT OUTER JOIN BI_ASAP_Rep.dbo.[Status] AS S1 WITH(NOLOCK)  
		ON ISNULL(SI.Status_Code,'ACT') = S1.Status_Code
WHERE [__$operation] = 5	

UPDATE DS
SET DS.[PreviousProcessStatusCode] = PP.Previous_Process_Status_Code 
    ,DS.[PreviousProcessStatusDescription] = PP.Previous_Process_Status_Description 
FROM EDW.dbo.DimStock DS
INNER JOIN #TMP_PSS_Data PP
ON DS.ASAPSalvageId = PP.Salvage_Id
WHERE PP.RowNumber = 2

/*****************************************************************************************
Inserting and Updating the data from all Temp tables into the DimStock table. No deletes.
******************************************************************************************/

CREATE TABLE #temp_SalvageRecordCount (ChangeType VARCHAR(10))

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_Stock_Data_Stage

--SELECT * FROM #TMP_Stock_Data_Stage

BEGIN TRAN 

	MERGE [dbo].[DimStock] AS DST
	USING #TMP_Stock_Data_Stage AS SRC
		   ON SRC.ASAPSalvageID = DST.ASAPSalvageID
		 
	WHEN MATCHED THEN
	UPDATE SET 
			 DST.[StockNumber]				=	SRC.[StockNumber]
			,DST.[StorageLocationID]        =   SRC.[StorageLocationID]
            ,DST.[AffiliateId]              =   SRC.[AffiliateId]
			,DST.[AssignorBranchNumber]     =   SRC.[AssignorBranchNumber]
            ,DST.[AdministrativeBranchNumber]=  SRC.[AdministrativeBranchNumber]
            ,DST.[AssignmentTypeCode]		=	SRC.[AssignmentTypeCode]
			,DST.[AssignmentTypeDescription]=	SRC.[AssignmentTypeDescription]
			,DST.[BranchNumber]             =   SRC.[AdministrativeBranchNumber] --We need to remover the BranchNumber field from EDW because it is null in ASAP
			,DST.[BodyStyleName]			=	SRC.[BodyStyleName]
			,DST.[ColorDescription]			=	SRC.[ColorDescription]
			,DST.[CountryOfManufacture]		=	SRC.[CountryOfManufacture]
			,DST.[DamageSeverityCode]		=   SRC.[DamageSeverityCode]
			,DST.[DamageTypeCode]           =   SRC.[DamageTypeCode]
			,DST.[ExteriorColorCode]        =   SRC.[ExteriorColorCode]
			,DST.[FullBodyStyle]			=	SRC.[FullBodyStyle]
			,DST.[ASAPFullBodyStyle]		=	Coalesce(SRC.[FullBodyStyle],DST.[FullBodyStyle])
			,DST.[FullStockNumber]          =   SRC.[FullStockNumber]
            ,DST.[MakeName]                 =   RTRIM(Coalesce(SRC.[MakeName],DST.[MakeName],SRC.[ASAP_MakeName]))
            ,DST.[ASAPMakeName]             =   RTRIM(SRC.[ASAP_MakeName])
			,DST.[ModelName] 				=	RTRIM(Coalesce(SRC.[ModelName],DST.[ModelName],SRC.[ASAP_ModelName]))
			,DST.[ASAPModelName] 			=	RTRIM(SRC.[ASAP_ModelName]) 
			,DST.[ModelYear]				=	SRC.[ModelYear] 
			,DST.[Mileage] 					=	SRC.[Mileage] 
			,DST.[OdometerReadingTypeCode] 	=	SRC.[OdometerReadingTypeCode] 
			,DST.[OdometerReadingTypeDescription] 	=	SRC.[OdometerReadingTypeDescription] 
			,DST.[StockType]                =   SRC.[StockType]
			,DST.[StockTypeDescription]     =   SRC.[StockTypeDescription]
			,DST.[SpecialtyCategoryDescription] = CASE WHEN SRC.[StockTypeDescription] IN ('ATV','Scooter') THEN 'OTHER'
			                                            ELSE SRC.[SpecialtyCategoryDescription] END 
            ,DST.[StockSubType]             =   SRC.[StockSubType]
            ,DST.[StockSubTypeDescription]  =   SRC.[StockSubTypeDescription]
			,DST.[VIN] 						=	SRC.[VIN] 
			,DST.[VINSerialNumber]			=	SRC.[VINSerialNumber]
			,DST.[SeriesName] 				=	SRC.[SeriesName]
			,DST.[ASAPSeriesName] 			=	Coalesce(DST.[SeriesName],SRC.[SeriesName])
			,DST.[SaleRunCount]             =   SRC.[SaleRunCount]
			,DST.[AssignorUserID]           =   SRC.[AssignorUserID]
            ,DST.[AssignorFirstName]        =   SRC.[AssignorFirstName]
            ,DST.[AssignorLastName]         =   SRC.[AssignorLastName]
			,DST.[StockCancelReasonCode]    =   SRC.[StockCancelReasonCode]
			,DST.[StockCancelReasonDescription] = SRC.[StockCancelReasonDescription]
            ,DST.[StockHoldReasonCode]      =   SRC.[StockHoldReasonCode] 
            ,DST.[StockHoldReasonDescription] = SRC.[StockHoldReasonDescription]
            ,DST.[KeysRetainedCodeatSale]   =   SRC.[KeysRetainedCodeatSale]
            ,DST.[IsReportedToNICB]         =   SRC.[IsReportedToNICB]
            ,DST.[IsCallForSaleAuthorization]=  SRC.[IsCallForSaleAuthorization]
			,DST.[IsHybrid]                 =   SRC.[IsHybrid]
			,DST.[IsSpecialty]              =   SRC.[IsSpecialty]
			,DST.[IsStockSPE]               =   SRC.[IsStockSPE]
			,DST.[IsTowable]                =   SRC.[IsTowable]
			,DST.[IsVIC]                    =   SRC.[IsVIC]
			,DST.[IsDeliveredToBranch]      =   SRC.[IsDeliveredToBranch]
			,DST.[IsPriorityTow]            =   SRC.[IsPriorityTow]
			,DST.[StockSegment] 			=	SRC.[StockSegment] 
			,DST.[MotorCycleSegment]        =   SRC.[MotorCycleSegment]
			,DST.[IsCancel]					=   SRC.[IsCancel]
			,DST.[IsValidMileage]           =   SRC.[IsValidMileage]
			,DST.[IsSoldKeysApplicable]     =   SRC.[SoldKeysApplicableInd]
			,DST.[AssignmentMethod]         =   SRC.[AssignmentMethod]
		    ,ETLLoadUpdateID                =   @ETLLoadID_sp
			,DST.EngineInformation			=   SRC.EngineInformation
			,DST.TransmissionType			=	SRC.TransmissionType
			,DST.FuelType					=   SRC.FuelType
			,DST.ASAPFuelType				=   Coalesce(DST.FuelType,SRC.FuelType)
			,DST.DriveLineType				=   SRC.DriveLineType
			,DST.ASAPDriveLineType			=   Coalesce(DST.DriveLineType,SRC.DriveLineType)
			,DST.IsFastTrack				=	SRC.IsFastTrack
			,DST.AssignmentOrigin			=	SRC.AssignmentOrigin
			,DST.IsOdoProbed				=   SRC.IsOdoProbed
			,DST.RecommendedSaleDocState	=	SRC.RecommendedSaleDocState
			,DST.RecommendedSaleDocType		=	SRC.RecommendedSaleDocType
			,DST.RecommendedSaleDocBrand	=	SRC.RecommendedSaleDocBrand
			, dst.EVMBaseShippingWeight	= COALESCE(SRC.EVMBaseShippingWeight,ISNULL(dst.EVMBaseShippingWeight,''))
			, dst.EVMCylindersDescription= COALESCE(SRC.EVMCylindersDescription,ISNULL(dst.EVMCylindersDescription,''))
			, dst.EVMVehicleClass		= COALESCE(SRC.EVMVehicleClass,ISNULL(dst.EVMVehicleClass,''))
			, dst.IsEVMUpdated = COALESCE(SRC.IsEVMUpdated,ISNULL(dst.IsEVMUpdated,0))
			, dst.ManufacturerOrigin = SRC.ManufacturerOrigin
			,DST.[VehicleGrade]=SRC.[VehicleGrade]
	WHEN NOT MATCHED  THEN
	INSERT ([ASAPSalvageID],[StockNumber] ,[StorageLocationID]
	        , [AffiliateId], [AssignorBranchNumber], [AdministrativeBranchNumber]
			, [AssignmentTypeCode], [AssignmentTypeDescription], [BranchNumber], [BodyStyleName], [ColorDescription], [CountryOfManufacture]		
			, [DamageSeverityCode], [DamageTypeCode], [ExteriorColorCode], [FullBodyStyle], [ASAPFullBodyStyle], [FullStockNumber], [KeysRetainedCodeatCheckIn]
			, [MakeName], [ASAPMakeName], [ModelName], [ASAPModelName], [ModelYear], [Mileage], [OdometerReadingTypeCode], [OdometerReadingTypeDescription]
			, [StockType], [StockTypeDescription], [StockSubType], [StockSubTypeDescription], [VIN], [VINSerialNumber], [SeriesName], [ASAPSeriesName], [SaleRunCount]
			, [AssignorUserID], [AssignorFirstName], [AssignorLastName], [StockCancelReasonCode], [StockCancelReasonDescription], [StockHoldReasonCode]      
			, [StockHoldReasonDescription], [KeysRetainedCodeatSale], [IsReportedToNICB], [IsCallForSaleAuthorization], [IsHybrid], [IsSpecialty]             
			, [IsStockSPE], [IsTowable], [IsVIC], [IsDeliveredToBranch], [IsPriorityTow], [StockSegment], [MotorCycleSegment]
			, [IsCancel],[IsValidMileage], [IsSoldKeysApplicable],[AssignmentMethod] ,[OriginalBranchNumber],[OriginalProviderID],[ETLLoadID],[ETLLoadUpdateID]
			,SpecialtyCategoryDescription,EngineInformation,TransmissionType,FuelType,ASAPFuelType,DriveLineType,ASAPDriveLineType,IsFastTrack,AssignmentOrigin
			,IsOdoProbed
			,RecommendedSaleDocState
			,RecommendedSaleDocType
			,RecommendedSaleDocBrand
			,EVMBaseShippingWeight,EVMCylindersDescription,EVMVehicleClass,IsEVMUpdated,ManufacturerOrigin,[VehicleGrade])

		VALUES ([ASAPSalvageID],[StockNumber] ,[StorageLocationID]
			, [AffiliateId], [AssignorBranchNumber], [AdministrativeBranchNumber]
			, [AssignmentTypeCode], [AssignmentTypeDescription], [BranchNumber], [BodyStyleName], [ColorDescription], [CountryOfManufacture]		
			, [DamageSeverityCode], [DamageTypeCode], [ExteriorColorCode], [FullBodyStyle], [FullBodyStyle], [FullStockNumber], [KeysRetainedCode]
			, RTRIM(Coalesce(SRC.[MakeName],SRC.[ASAP_MakeName])), RTRIM([ASAP_MakeName]), RTRIM([ModelName]), RTRIM([ASAP_ModelName]), [ModelYear], [Mileage], [OdometerReadingTypeCode], [OdometerReadingTypeDescription]
			, [StockType], [StockTypeDescription], [StockSubType], [StockSubTypeDescription], [VIN], [VINSerialNumber], [SeriesName], [SeriesName], [SaleRunCount]
			, [AssignorUserID], [AssignorFirstName], [AssignorLastName], [StockCancelReasonCode], [StockCancelReasonDescription], [StockHoldReasonCode]      
			, [StockHoldReasonDescription], [KeysRetainedCodeatSale], [IsReportedToNICB], [IsCallForSaleAuthorization], [IsHybrid], [IsSpecialty]             
			, [IsStockSPE], [IsTowable], [IsVIC], [IsDeliveredToBranch], [IsPriorityTow], [StockSegment], [MotorCycleSegment]
			, [IsCancel], [IsValidMileage],[SoldKeysApplicableInd],[AssignmentMethod],[OriginalBranchNumber],[OriginalProviderID],@ETLLoadID_sp, @ETLLoadID_sp
			, CASE WHEN SRC.[StockTypeDescription] IN ('ATV','Scooter') THEN 'OTHER' ELSE SRC.[SpecialtyCategoryDescription] END
			,EngineInformation,TransmissionType,FuelType,FuelType,DriveLineType,DriveLineType,IsFastTrack,AssignmentOrigin,IsOdoProbed
			,RecommendedSaleDocState
			,RecommendedSaleDocType
			,RecommendedSaleDocBrand
			,EVMBaseShippingWeight,EVMCylindersDescription,EVMVehicleClass,ISNULL(IsEVMUpdated,0),ManufacturerOrigin,[VehicleGrade])
	
	OUTPUT $action INTO #temp_SalvageRecordCount;


SELECT @InsertRowCount_sp = COUNT(*)  FROM #temp_SalvageRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount_sp = COUNT(*)  FROM #temp_SalvageRecordCount WHERE ChangeType = 'UPDATE'

--Updating the SellFromStorageLocationID for Stocks which were not Sold.
UPDATE S
SET SellFromStorageLocationID = ISNULL(TS.Sell_From_Storage_Location_ID,0)
    , ETLLoadUpdateID = @ETLLoadID_sp
--SELECT TS.ASAPSalvageID ,TS.SellFromStorageLocationID 
FROM #Tmp_Salvage_ID_2_Update TS WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S 
		    ON S.ASAPSalvageID = TS.Salvage_ID
	INNER JOIN [BI_ASAP_REP].dbo.Salvage_Info si WITH (NOLOCK)
			ON si.Salvage_ID = TS.Salvage_ID
WHERE si.Sold_Ind = 0 AND Checkin_DateTime < '12/31/2999'

-- Updating data in DimStock from #TMP_SLH_Data (Salvage_Location_History)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SLH_Data

UPDATE S SET
	  AisleNumber = Aisle_Number 
	, StallNumber = Stall_Number 
	, ETLLoadUpdateID = @ETLLoadID_sp
FROM #TMP_SLH_Data AS SL WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON SL.Salvage_ID = S.ASAPSalvageID 

SET @UpdateRowCount_sp += @@ROWCOUNT

UPDATE S SET
	IsNoRowSlot = CASE WHEN ISNULL(Aisle_Number,'') = '' AND Stall_Number IS NULL THEN 1 ELSE 0 END 
	, ETLLoadUpdateID = @ETLLoadID_sp
FROM #TMP_SLH_Data AS SL WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON SL.Salvage_ID = S.ASAPSalvageID 

SET @UpdateRowCount_sp += @@ROWCOUNT

Update DS SET IsNoRowSlot=0 	
--SELECT IsNoRowSlot
from dbo.DimStock DS WITH (NOLOCK)
WHERE 	IsNoRowSlot=1 and (DS.StockID IN (SELECT StockID FROM #TMP_Stock_Data_Stage) OR DS.StockID IN (SELECT StockID FROM #TMP_SLH_Data)) AND
(DS.CurrentStockStatusCode IN ( 'CLO', 'CAN', 'TWO', 'RNT', 'INA', 'VOD')
	OR DS.assignmenttypecode IN ('TTL', 'TOW') 
	OR DS.IsCancel = 1
	OR DS.IsAffiliateStock = 1) 

INSERT INTO #TMP_CT_Salavage_Event_REAB_BT
([StockID] )
SELECT  DS.StockID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge' ) AS SER
INNER JOIN Dimstock  DS WITH (NOLOCK) 
 ON (DS.ASAPSalvageID = SER.Salvage_id and Salvage_Event_Code in ('REAB'))
WHERE [__$operation] = 5 

INSERT INTO #TMP_CT_Salavage_Event_IBTR_IBND_BT
([StockID]  ,
 [Event_DateTime] )

SELECT  DS.StockID,
     Max(SER.event_Datetime)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge' ) AS SER
INNER JOIN Dimstock  DS WITH (NOLOCK) 
 ON (DS.ASAPSalvageID = SER.Salvage_id and Salvage_Event_Code in ('IBTR','IBND'))

WHERE [__$operation] = 5 
GROUP BY DS.StockID


INSERT INTO #TMP_CT_Salavage_Event_IBCN_BT
([StockID]  ,
 [Event_DateTime] )

SELECT  DS.StockID,
     Max(SER.event_Datetime)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge' ) AS SER
INNER JOIN Dimstock  DS WITH (NOLOCK) 
 ON (DS.ASAPSalvageID = SER.Salvage_id and Salvage_Event_Code in ('IBCN'))

WHERE [__$operation] = 5 
GROUP BY DS.StockID


Update S
set IsBranchTransfer = 1 
From #TMP_Stock_Data_Stage AS TSDS with (nolock)
INNER JOIN dbo.Dimstock AS S with (nolock)
on TSDS.ASAPSalvageID = S.ASAPSalvageID 
where S.IsBranchTransfer = 0
and  S.OriginalBranchNumber <> 0
and S.AdministrativeBranchNUmber <> S.OriginalBranchNumber

Update S
set OriginalBranchNumber =  isnull(TSOU.OriginalBranchNumber,0) 
From #TMP_CT_Salavage_Original_Update  AS TSOU with (nolock)
INNER JOIN dbo.Dimstock AS S with (nolock)
on TSOU.ASAPSalvageID = S.ASAPSalvageID 
where TSOU.ran=1 and TSOU.OriginalBranchNumber is not NULL



Update S
set IsBranchTransfer = 1 
From #TMP_CT_Salavage_Event_REAB_BT AS TSERT 
INNER JOIN dbo.Dimstock AS S with (nolock)
on TSERT.stockID = S.stockID 

Update S
set IsBranchTransfer = 1 
From #TMP_CT_Salavage_Event_IBTR_IBND_BT AS TSEIIB 
INNER JOIN dbo.Dimstock AS S with (nolock)
on TSEIIB .stockID = S.stockID
LEFT OUTER JOIN #TMP_CT_Salavage_Event_IBCN_BT TSEIB 
on TSEIIB.stockid = TSEIB.stockid
where  TSEIIB.[Event_DateTime] > TSEIB.[Event_DateTime] and IsBranchTransfer=0


INSERT INTO #TMP_CT_Salavage_Provider_Original (asapsalvageid ,OriginalProviderID, ran )
SELECT	SP1.salvage_id  AS salvage_id ,
		SP1.Salvage_Provider_ID,
		rank () over (partition by  SP1.salvage_id order by SP1.update_datetime) ran 
FROM [BI_ASAP_Rep].cdc.dbo_salvage_Provider_info_ct AS SP1
INNER JOIN [BI_ASAP_Rep].cdc.dbo_salvage_Provider_info_ct  As SP2
ON ( SP1.__$seqval = SP2.__$seqval
AND SP1.__$operation = 3
AND SP2.__$operation = 4
and SP1.Salvage_Provider_ID <> SP2.Salvage_Provider_ID)
inner join DimStock ds with (nolock) on (ds.ASAPSalvageID  = Sp1.Salvage_ID and IsProviderOfficeTransfer =0 )

Update S
set OriginalProviderID =  isnull(TSPO.OriginalProviderID,0),
IsProviderOfficeTransfer = 1,
IsProviderGroupTransfer   = Case when (SPC.Salvage_Provider_Group_ID  <> SPO.Salvage_Provider_Group_ID  and SPO.Salvage_Provider_Group_ID<>0 ) then 1 else 0 end ,
ETLLoadUpdateID                =   @ETLLoadID_sp 
From #TMP_CT_Salavage_Provider_Original   AS TSPO with (nolock)
INNER JOIN dbo.Dimstock AS S with (nolock)
on TSPO.ASAPSalvageID = S.ASAPSalvageID 
LEFT OUTER JOIN [BI_ASAP_REP].[dbo].[Salvage_Provider] AS SPC with (NOLOCK)
ON (SPC.Salvage_Provider_ID = S.CurrentProviderID)
LEFT OUTER JOIN [BI_ASAP_REP].[dbo].[Salvage_Provider] AS SPO with (NOLOCK)
ON (SPO.Salvage_Provider_ID = S.OriginalProviderID)
where TSPO.ran=1 and TSPO.OriginalProviderID is not NULL



SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_ST_Data (Salvage_Title)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_ST_Data
 
UPDATE S SET
--SELECT
	  SaleDocumentTypeId          = Sale_Document_Type_ID
	, SaleDocumentTypeDescription = Sale_Document_Type_Description
	, SaleDocBrand                = Sale_Document_Brand
	, SaleDocNumber               = Sale_Document_Number
	, TitleBrand                  = Title_Received_Brand 
	, TitleNumber                 = Title_Received_Number
	, TitleState                  = Title_State
	, Titletype                   = Title_type
	, AuctionTypeCode             = Auction_Type_Code
	, AuctionTypeDescription      = Auction_Type_Description 
	, IsChargeTitleProcessFee     = Charge_Title_Process_Fee_Ind 
	, IsStockProviderProcessTitle = Salvage_Provider_Process_Title_Ind 
	, TitleProcessingBranchNumber = Title_Processing_Branch_Number
	, IsTitleServiceUsed          = Title_Service_Used_Ind
	, SaleDocumentTypeCategory    = Sale_Document_Type_Category
	, CertState					  = Cert_State
	, SaleDocNote                 = Sale_Document_Note
	, ETLLoadUpdateID             = @ETLLoadID_sp
FROM #TMP_ST_Data ST WITH (NOLOCK)
	INNER JOIN dbo.DimStock  S
		ON ST.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_SD_Data (Salvage_Damage)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SD_Data

UPDATE S SET
--SELECT 
	    [DamageCodePrimary]          = ISNULL(Damage_Code_Primary,'')
      , [DamageCodeSecondary]        = ISNULL(Damage_Code_Secondary,'')
      , [DamageDescriptionPrimary]   = ISNULL(LEFT(DP.Damage_Description, 40),'')
      , [DamageDescriptionSecondary] = ISNULL(LEFT(DS.Damage_Description, 40),'')
      , [ETLLoadUpdateID]            = @ETLLoadID_sp
FROM #TMP_SD_Data AS TSD WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S 
		ON TSD.Salvage_ID = S.ASAPSalvageID
	LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Damage AS DP WITH(NOLOCK)
		ON TSD.Damage_Code_Primary = DP.Damage_Code 
	LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Damage AS DS WITH(NOLOCK)
		ON TSD.Damage_Code_Secondary = DS.Damage_Code
		
SET @UpdateRowCount_sp += @@ROWCOUNT
	
-- Updating data in DimStock from #TMP_SSP_Data (Salvage_Special_Purchase)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SSP_Data	

UPDATE S SET
	    [SpecialPurchaseTypeCode]        = Special_Purchase_Type_Code
      , [SpecialPurchaseTypeDescription] = Special_Purchase_Type_Description
      , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SSP_Data	AS SSP WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON SSP.Salvage_ID = S.ASAPSalvageID
		
SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_SAI_Data (Salvage_Alternate_Info)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SAI_Data
	
UPDATE S SET
	   [AlternateStockNumber] = Alternate_Stock_Number_1
	 , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SAI_Data AS SD WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON SD.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_SC_Data (Salvage_Checkin)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SC_Data

UPDATE S SET
	   [EngineStatusCode] = Engine_Status_Code
	  ,[IsRunAndDriveAtCheckIn] = ISNULL(Run_and_Drive_Ind,0)
	  ,[IsRunAndDriveAuction] = ISNULL(Run_and_Drive_Ind,0) -- Should be same as Checkin. Unless it goes to Auction.
	 , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SC_Data AS SC WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON SC.Salvage_ID = S.ASAPSalvageID
		
SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_SLCD_Data (Salvage_Loss)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SLCD_Data

UPDATE S SET 
	   [LossTypeCode]        = Loss_Type_Code
	 , [LossTypeDescription] = Loss_Type_Description
	 , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SLCD_Data AS SLCD WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S 
		ON SLCD.Salvage_ID = S.ASAPSalvageID
		
SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_SCI_Data (Salvage_Contract_Info)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SCI_Data
 
UPDATE S SET
	   [AgreementTermTypeCode]                = Agreement_Term_Type_Code
	 , [AgreementTermTypeDescription]         = Agreement_Term_Type_Description
	 , [PreviousAgreementTermTypeCode]        = Previous_Agreement_Term_Type_Code
	 , [PreviousAgreementTermTypeDescription] = Previous_Agreement_Term_Type_Description
	 , IsFlippedtoPA                          = Flipped_to_PA_Ind
	 , IsAgreementChanged                     = Agreement_Changed_Ind
	 , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SCI_Data AS SCI WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S 
		ON SCI.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

---- Updating data in DimStock from #TMP_STow_Data (Salvage_Tow) EDW Phase 2-Sprint 7
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_STow_Data

UPDATE S SET
--	  IsBuyerTow  = Buyer_Tow_Ind
--	, IsReturnTow = Return_Tow_Ind
--	, IsDryRun    = Dry_Run_Ind
	  BuyerTowMiles = ISNULL(Buyer_Tow_Miles,BuyerTowMiles)
	, InboundTowMiles = ISNULL(Provider_Tow_Miles,InboundTowMiles)
	, [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_STow_Data STD WITH (NOLOCK)
	INNER JOIN  dbo.DimStock AS S 
		ON STD.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_SP_Data (Salvage_Pickup)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SP_Data

UPDATE S SET
	IsExpressPickupTow = Express_Pickup_Ind
  ,PickupLocationID = Pickup_Location_ID
  ,[ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SP_Data SPD WITH (NOLOCK)
	INNER JOIN  dbo.DimStock AS S 
		ON SPD.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

Update DS
Set IsResidence =DSL.IsResidence
From dbo.DimStock DS
inner join #TMP_SP_Data TR 
	on TR.Salvage_id = DS.asapsalvageid
inner join dbo.DimStorageLocation DSL with (nolock)
	on DSL.Storagelocationid = DS.Pickuplocationid

-- Updating data in DimStock from #TMP_SI_Data (Salvage_info)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SI_Data

UPDATE S SET
	CurrentStockStatusCode          = SI.Current_Stock_Status_Code
	, CurrentStockStatusDescription = SI.Current_Stock_Status_Description
	, ProcessStatusCode             = SI.Process_Status_Code
	, ProcessStatusDescription      = SI.Process_Status_Desscription
	, IsStockSold                   = Sold_Ind
	--, IsTitleProblem                = Title_Problem_Ind
	,IsMissingParts                 = Missing_Parts_Ind 
	, [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SI_Data SI WITH (NOLOCK)
	INNER JOIN  dbo.DimStock AS S 
		ON SI.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT


---- Updating data in DimStock from #TMP_SSale_Data (Salvage_Sale) IBFSoldIND,SalePrice,SaleTax EDW Phase 3  -Sprint 11
-- This Update should occur after IsStockSold
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SSale_Data

UPDATE dbo.DimStock SET 
--SELECT DS.ASAPSalvageID ,Sale_Amount ,Tax_Amount ,IsStockSold ,SalePrice AS DS_SP ,SaleTaxAmount AS DS_ST ,IsIBFSold AS DS_IBF,
	 IsIBFSold = CASE WHEN (Sale_Amount IS NULL OR Tax_Amount IS NULL) AND IsStockSold = 1 THEN DS.IsIBFSold
				  ELSE T_SS.IBN_Ind END 
	,SalePrice = CASE WHEN (Sale_Amount IS NULL OR Tax_Amount IS NULL) AND IsStockSold = 1 THEN DS.SalePrice 
					  ELSE T_SS.Sale_Amount END 
    ,SaleTaxAmount = CASE WHEN (Sale_Amount IS NULL OR Tax_Amount IS NULL) AND IsStockSold = 1 THEN DS.SaleTaxAmount 
					      ELSE T_SS.Tax_Amount END 
   	,[ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SSale_Data T_SS
	INNER JOIN dbo.DimStock DS WITH(NOLOCK)
		ON T_SS.Salvage_ID = DS.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_SPI_Data (Salvage_Provider_Info)
--This update should run after IsIBFSold
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SPI_Data

UPDATE S SET
	  ClaimNumber        = Claim_Number
	, HandlerUserID      = Handler_User_ID
	, AdjustorFirstName  = Adjustor_First_Name
	, AdjustorLastName   = Adjustor_Last_Name
	, CallerUserID       = Caller_User_ID
	, ClaimHandler       = Claim_Handler
	, PolicyNumber       = Policy_Number 
	, OwnerFirstName     = Owner_First_Name
	, OwnerLastName      = Owner_Last_Name
	, MinimumBidAmount   = Minimum_Bid_Amount
	, IsMinimumBidOutlier  = CASE WHEN Minimum_Bid_Amount < 10 OR Minimum_Bid_Amount >= 99999 OR Minimum_Bid_Amount IS NULL THEN 1 ELSE 0 END
	, InsuredFirstName   = Insured_First_Name 
	, InsuredLastName    = Insured_Last_Name
	, MinimumBidSource   = Minimum_Bid_Source 
	, IBFRunCount        = IBFRunCount 
	, IsIBFACQualified   = IBF_AC_Qualified_Ind 
	, IsIBFQualified     = CASE WHEN S.IsIBFSold = 1 THEN 0 ELSE IBF_Qualified_Ind END 
	, IsIBFDisplay       = CASE WHEN S.IsIBFSold =1 THEN 0 ELSE IBF_AC_Qualified_Ind END  -- IBN AC is nothing but IBF Display 
	, IsIBFRunThresholdReached = IBF_Run_Threshold_Reached_Ind 
	, IsIBFSPApproved    = CASE WHEN AgreementTermTypeCode = 'PAA' THEN 0 ELSE SPID.IBF_SP_Approved_Ind END
	, IsOffSiteSale      = Offsite_Sale_Ind
	, CurrentProviderID  = Provider_ID
	, HadIBFDisplay      = CASE WHEN S.HadIBFDisplay = 1 THEN 1 ELSE SPID.IBF_AC_Qualified_Ind END
	,AlternatePayeeID    = SPID.AlternatePayeeID 
	, ETLLoadUpdateID    = @ETLLoadID_sp
	,ClaimKey			 = SPID.ClaimKey
	,AdjusterPhone		 = SPID.AdjustorPhone
FROM #TMP_SPI_Data AS SPID WITH (NOLOCK)
	INNER JOIN dbo.DimStock AS S
		ON SPID.Salvage_ID = S.ASAPSalvageID 
			LEFT OUTER JOIN [BI_ASAP_REP].[dbo].[Salvage_Provider] AS SPC with (NOLOCK)
        ON (SPC.Salvage_Provider_ID = S.CurrentProviderID)
			LEFT OUTER JOIN [BI_ASAP_REP].[dbo].[Salvage_Provider] AS SPO with (NOLOCK)
        ON (SPO.Salvage_Provider_ID = S.OriginalProviderID)

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock for IBF renege


UPDATE DS
SET 
IsIBFRenege = CASE WHEN RenegeCancel_DateTime > SOLD_Datetime  THEN 1 ELSE 0 END
FROM 
dbo.DimStock DS 
INNER JOIN #TMP_IBF_Renege TR
ON TR.Salvage_ID = DS.ASAPSalvageID
INNER JOIN #TMP_IBN_Sold TS
ON TR.Salvage_ID = TS.Salvage_ID



-- Updating data in DimStock from #TMP_ASI_Data (Affiliate_Salvage_info)
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_ASI_Data

UPDATE S SET
	IsAffiliateStock = ASI.IsAffiliateStock
  , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_ASI_Data ASI WITH (NOLOCK)
	INNER JOIN  dbo.DimStock AS S 
		ON ASI.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_IsStandardSale_Stage 
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_IsStandardSale_Stage

UPDATE S SET
	IsStandardSale = TSS.Standard_Sale_Ind
  , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_IsStandardSale_Stage TSS WITH (NOLOCK)
	INNER JOIN  dbo.DimStock AS S 
		ON TSS.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating data in DimStock from #TMP_Current_Buyer_ID 
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Current_Buyer_ID

UPDATE S SET
	CurrentBuyerID = TCB.Buyer_Id
  , [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_Current_Buyer_ID AS TCB WITH (NOLOCK)
	INNER JOIN  dbo.DimStock AS S 
		ON TCB.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT


-- Updating data in DimStock from #TMP_SExpense_Data EDW Phase 3 S11
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SExpense_Data

UPDATE dbo.DimStock SET
--SELECT Salvage_ID ,PA_Price, 
	PAPrice = TSD.PA_Price
	,[ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_SExpense_Data TSD
	INNER JOIN dbo.DimStock DS WITH(NOLOCK)
		ON TSD.Salvage_ID = DS.ASAPSalvageID
		
SET @UpdateRowCount_sp += @@ROWCOUNT	


-- Updating data in DimStock from #TMP_Stock_Cancelled_Date EDW Phase 3 S11
-- Updating the PA price to NULL for Assignment Cancelled Stocks.

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Stock_Cancelled_Date

UPDATE dbo.DimStock SET
--SELECT Salvage_ID ,PA_Price, 
	PAPrice = NULL
	,[ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_Stock_Cancelled_Date TSD
	INNER JOIN dbo.DimStock DS WITH(NOLOCK)
		ON TSD.Salvage_ID = DS.ASAPSalvageID
		
SET @UpdateRowCount_sp += @@ROWCOUNT


-- Updating data in DimStock from #TMP_Salvage_Event_History_Data EDW Phase 2-Sprint 7
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Salvage_Event_History_Data

UPDATE S SET 
--SELECT Salvage_ID ,IsBulkSale,IsHadHold,IsHadReleaseProblem,
	--IsBulkSale =CASE WHEN IsStockSold = 0 THEN 0 
	--             ELSE COALESCE(Bulk_Sale_IND,0) END,
	IsHadHold = COALESCE(Had_Hold_IND,IsHadHold,0),
--	IsHadReleaseProblem = COALESCE(Had_Release_Problem_IND,IsHadReleaseProblem,0),
	IsHadTitleProblem = COALESCE(Had_Title_Problem_IND,IsHadTitleProblem,0)
	, [ETLLoadUpdateID] = @ETLLoadID_sp
FROM (SELECT Salvage_ID, 
           SUM(Bulk_Sale_IND) AS Bulk_Sale_IND,
		   SUM(Had_Hold_IND) AS Had_Hold_IND,
--		   SUM(Had_Release_Problem_IND) AS Had_Release_Problem_IND,
		   SUM(Had_Title_Problem_IND) AS Had_Title_Problem_IND
		   FROM #TMP_Salvage_Event_History_Data  WITH (NOLOCK) GROUP BY Salvage_ID) TMP_SExpense_Data
	INNER JOIN  dbo.DimStock AS S 
		ON TMP_SExpense_Data.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

---- Updating data in DimStock from #TMP_BCC_Data EDW Phase 2-Sprint 7
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_BCC_Data

UPDATE S SET --SELECT S.ASAPSalvageID,IsCreditCardSale ,Credit_Card_Sale_Ind,
	IsCreditSale = Credit_Card_Sale_Ind
	, [ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_BCC_Data AS TMP_SExpense_Data WITH (NOLOCK)
	INNER JOIN  dbo.DimStock AS S 
		ON TMP_SExpense_Data.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT

 --Updating data in DimStock from #TMP_Open_Stock_ACV  
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Open_Stock_ACV

UPDATE DS SET 
--SELECT ACV_Value,ActualCashValue,Blackbook_Value,CMV,
	ActualCashValue = COALESCE(SP_ACV ,ActualCashValue ,0)
	,BlackbookACV = COALESCE(IAA_ACV ,BlackbookACV ,0)
	,ECR = COALESCE(TOSA.ECR ,DS.ECR ,0)
	,[ETLLoadUpdateID] = @ETLLoadID_sp
FROM #TMP_Open_Stock_ACV AS TOSA
	INNER JOIN  dbo.DimStock AS DS 
		ON DS.ASAPSalvageID = TOSA.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT



/* Updating IsACVOutLier in DimStock.
This update should go after updating ACV values and updating StockType in DimStock. 
Stock type
1 - Automobile
2 - Motorcycle
3 - Truck */


UPDATE dbo.DimStock SET
--SELECT DS.ASAPSalvageID ,IsACVOutLier,
IsACVOutlier = CASE WHEN ISNULL(StockNumber,0) = ISNULL(ActualCashValue,0) THEN 1
					WHEN ISNULL(ActualCashValue,0) <= 200 THEN 1
					WHEN StockType IN (1,2) AND ISNULL(ActualCashValue,0) > 135000 THEN 1 
					WHEN StockType NOT IN (1,2) AND ISNULL(ActualCashValue,0) > 550000 THEN 1
					WHEN ISNULL(BlackbookACV,0) > 0 AND ISNULL(ActualCashValue,0) > 20000 AND (ISNULL(ActualCashValue,0)/ISNULL(BlackbookACV,0)) > 4 THEN 1 
					ELSE 0 END
, ETLLoadUpdateID = @ETLLoadID_sp					 
FROM dbo.DimStock DS WITH(NOLOCK) 
INNER JOIN 
		(SELECT Salvage_ID AS ASAPSalvageID FROM #TMP_Open_Stock_ACV
		UNION
		SELECT ASAPSalvageID FROM #TMP_Stock_Data_Stage) S
	ON DS.ASAPSalvageID = S.ASAPSalvageID
		


-- Updating data in DimStock from #TMP_SEH_Data_BuyerRenegeCancel 
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SEH_Data_BuyerRenegeCancel

UPDATE S SET
--SELECT BRC.Salvage_ID,
	  IsBuyerReneged				= Buyer_Renged_Ind 
	, IsBuyerRenegedAfterSettlement = Buyer_Renged_After_Settlement_Ind 
	, BuyerRenegedCount				= Buyer_Renged_Count 
	, IsBuyerCancel					= Buyer_Cancel_Ind 
	, IsBuyerCancelAfterSettlement  = Buyer_Cancel_After_Settlement_Ind 
	, BuyerCancelCount				= Buyer_Cancel_Count 
	, BuyerRenegedCancelCount		= Buyer_Renged_Cancel_Count 
	, BuyerRenegedCancelCountBeforeSettlement = Buyer_Renged_Cancel_Count_Before_Settlement 
	, BuyerRenegedType				= Buyer_Reneged_Type
	, ETLLoadUpdateID				= @ETLLoadID_sp
FROM #TMP_SEH_Data_BuyerRenegeCancel BRC WITH (NOLOCK)
	INNER JOIN dbo.DimStock  S
		ON BRC.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT


-- Updating Holdcount in DimStock from #TMP_Salvage_Event_History_Data_HOLD2 

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Hold_Counts

UPDATE S SET
	--SELECT BRC.Salvage_ID,
	IsOnHold = ISNULL(BRC.IsOnHold,0)
	,ETLLoadUpdateID = @ETLLoadID_sp
FROM #TMP_Salvage_Event_History_Data_HOLD2 BRC WITH (NOLOCK)
	INNER JOIN dbo.DimStock  S
		ON BRC.Salvage_ID = S.ASAPSalvageID
SET @UpdateRowCount_sp += @@ROWCOUNT

UPDATE S SET
	--SELECT BRC.Salvage_ID,
	IsOnHold = 0
	,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock  S
	LEFT OUTER JOIN BI_ASAP_REP.dbo.Salvage BRC WITH (NOLOCK)
		ON BRC.Salvage_ID = S.ASAPSalvageID
WHERE S.IsOnHold = 1 AND BRC.Salvage_ID IS NULL


-- Updating Holdcount in DimStock from #TMP_Hold_Counts 

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Hold_Counts

UPDATE dbo.DimStock SET 
--SELECT DS.ASAPSalvageID,
	 Holdcount  = ISNULL(H.HOld_Counts,0)
	,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock as DS 
	INNER JOIN #TMP_Hold_Counts  as H
		ON DS.ASAPSalvageID = H.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT	

-- Updating CourtOrderHoldCount in DimStock from #TMP_CourtOrder_Hold_Counts 

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_CourtOrder_Hold_Counts

UPDATE dbo.DimStock SET 
----SELECT DS.ASAPSalvageID,
	CourtOrderHoldCount = IsNULL(CH.CourtOrder_Hold_Counts,0)
	,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock as DS 
	INNER JOIN #TMP_CourtOrder_Hold_Counts  as CH
		ON DS.ASAPSalvageID = CH.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT


-- Updating IsBulkSale in DimStock from #TMP_Bulk_Sale_Ind 

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Bulk_Sale_Ind

UPDATE dbo.DimStock SET 
	IsBulkSale = CASE WHEN IsStockSold = 0 THEN 0 
					 ELSE COALESCE(Bulk_Sale_IND,0) END
	,ETLLoadUpdateID = @ETLLoadID_sp	             
FROM dbo.DimStock AS DS 
	INNER JOIN #TMP_Bulk_Sale_Ind as BI
		ON DS.ASAPSalvageID = BI.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT


/* Updating GrossReturn in DimStock.
This update should go after updating 
	IsBuyerRenegedAfterSettlement, IsBuyerCancelAfterSettlement,
	SalePrice ,PAPrice ,AgreementTermTypeCode */

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Gross_Return_Salvage_ID

UPDATE S SET
	GrossReturn = 	CASE WHEN (S.IsBuyerCancelAfterSettlement = 1  OR S.IsBuyerRenegedAfterSettlement = 1)  THEN ISNULL(GR.Sale_Amount,S.GrossReturn)
						 ELSE (CASE WHEN S.AgreementTermTypeCode = 'PAA' THEN S.PAPrice ELSE S.SalePrice END) 
						 END 	       	
	,ETLLoadUpdateID = @ETLLoadID_sp
FROM #TMP_Gross_Return_Salvage_ID GR WITH (NOLOCK)
	INNER JOIN dbo.DimStock  S
		ON GR.Salvage_ID = S.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT


--** Updating Advance Storage info Objects to DimStock from Temp Table

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_AdvanceStorage

UPDATE DS SET 
--SELECT DS.ASAPSalvageID,
	AdvanceStorageInfoID = TAS.AdvanceStorageInfoID
	,AdvanceRateType		  = TAS.AdvanceRateType
	,IAAAdvanceRateType		  = TAS.IAAAdvanceRateType
	,AdvanceStorageAmount	  = TAS.AdvanceStorageAmount
	,AdvanceStorageTax		  = TAS.AdvanceStorageTax
	,IAAAdvanceStorageAmount  = TAS.IAAAdvanceStorageAmount
	,IAAAdvanceStorageTax	  = TAS.IAAAdvanceStorageTax
	,DaysInAdvanceStorageProvider = TAS.DaysInAdvanceStorageProvider
	,DaysInAdvanceStorageIAA   = TAS.DaysInAdvanceStorageIAA
	,DaysInAdvanceStorageTotal = TAS.DaysInAdvanceStorageTotal
	,ETLLoadUpdateID		   = @ETLLoadID_sp
FROM dbo.DimStock AS DS 
	INNER JOIN #TMP_AdvanceStorage AS TAS
		ON DS.ASAPSalvageID = TAS.Salvage_ID
		
SET @UpdateRowCount_sp += @@ROWCOUNT		



SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_SBF_Data

UPDATE dbo.DimStock SET
--SELECT Salvage_ID,
    BuyerFinanceCompanyCode = Finance_Company_Code 
   ,BuyerFinanceAmount = Finance_Amount
   ,ETLLoadUpdateID		   = @ETLLoadID_sp
   ,AFCFlooringSource = CASE WHEN Finance_Amount = 0 THEN '' ELSE SBF.AFCFlooringSource END
FROM #TMP_SBF_Data SBF WITH(NOLOCK)
  	INNER JOIN dbo.DimStock DS WITH(NOLOCK)
  		ON SBF.Salvage_ID = DS.ASAPSalvageID


SET @UpdateRowCount_sp += @@ROWCOUNT		


SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Title_Problem_Ind

UPDATE DS SET
--SELECT  DS.FullStockNumber,T.*,
IsTitleProblem = CASE --WHEN DS.ProcessStatusCode = 'J40' THEN 1
                      WHEN ISNULL(Process_Code,'') <> 'TTL' THEN 0
                      WHEN ISNULL(Process_Code,'') = 'TTL' THEN (CASE WHEN DS.ProcessStatusCode = 'J40' THEN 1
                                                                             ELSE CASE WHEN T.Status_Code = 'J40' THEN 1  ELSE 0 END 
                                                                               END) 
                          END
,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock DS 
	INNER JOIN #TMP_Title_Problem_Ind T 
		ON DS.ASAPSalvageID = T.Salvage_ID 


SET @UpdateRowCount_sp += @@ROWCOUNT		

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_MODEL_AGE_BAND

UPDATE DS SET 
	SoldModelYearAge = TM.Sold_Model_Year_Age,
	SoldModelYearBand = TM.Model_Year_Band,
	ETLLoadUpdateID = @ETLLoadID_sp,
	SoldModelYearBand2 = TM.Model_Year_Band2
FROM dbo.DimStock DS 
	INNER JOIN #TMP_MODEL_AGE_BAND TM
		ON DS.ASAPSalvageID = TM.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT	

-- The run and drive Auction should go only after updating the Run and Drive at checkin IND
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_RunDriveInd

UPDATE DS SET 
 IsRunAndDriveAuction = T.Run_and_Drive_Auction_Ind
,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock Ds 
	INNER JOIN #TMP_RunDriveInd T
		ON DS.ASAPSalvageID = T.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT	

-- UPDATE IsNegotiatedSale
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_IsNegotiatedSale

UPDATE DS SET 
 IsNegotiatedSale = T.IsNegotiatedSale
,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock Ds 
	INNER JOIN #TMP_IsNegotiatedSale T
		ON DS.ASAPSalvageID = T.Salvage_ID
WHERE IsStockSold = 1

SET @UpdateRowCount_sp += @@ROWCOUNT

-- Updating IsECI in DimStock from #TMP_ECI_Ind_Data 

SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_ECI_Ind_Data

UPDATE DS SET
--SELECT TECI.* ,
	IsECI = ECI_Ind
	,ETLLoadUpdateID = @ETLLoadID_sp
FROM #TMP_ECI_Ind_Data TECI
	INNER JOIN dbo.DimStock DS 
		ON TECI.Salvage_ID = DS.ASAPSalvageID

SET @UpdateRowCount_sp += @@ROWCOUNT	


SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_ENhancement_IND

UPDATE DS 
SET 
IsBandageEnhancement = ISNULL(T.Bandage_Enhancement_IND,0)
,IsDetailEnhancement = ISNULL(T.Detail_Enhancement_IND,0)
,IsFloodServiceEnhancement = ISNULL(T.Flood_Service_Enhancement_IND,0)
,IsFullWrapEnhancement = ISNULL(T.Full_Wrap_Enhancement_IND,0)
,IsPatchEnhancement = ISNULL(T.Patch_Enhancement_IND,0)
,IsEnhancementShrinkWrap = ISNULL((CASE	WHEN T.Bandage_Enhancement_IND = 1
											  OR T.Full_Wrap_Enhancement_IND = 1
											  OR T.Patch_Enhancement_IND	= 1 THEN 1 ELSE	0 END),0)
,IsRunAndDriveEnhancement = ISNULL(T.Run_And_Drive_Enhancement_IND,0)
,IsKeyEnhancment = ISNULL(T.Key_Enhancement_IND,0)
FROM
dbo.DimStock DS
INNER JOIN #TMP_ENhancement_IND T 
  ON Ds.ASAPSalvageID = T.Salvage_ID


SET @UpdateRowCount_sp += @@ROWCOUNT	

 
--------------VEHICLE INSPECTION SERVICES UPDATE!
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Enhancement_Salvage_IDs_InspectionServices

UPDATE  DS 
--SET	  	EnhancementLevelID = CASE WHEN ISS.Enhancement_Level_ID = 0 THEN 3 ELSE ISS.Enhancement_Level_ID END, --Change for Level 3
SET		
        --[IsInspectionServiceCancelled] = IIF(ISNULL(ISS.Enhancement_Level_ID,0)>0,0,[IsInspectionServiceCancelled]),
		EnhancementLevelID = ISNULL(ISS.Enhancement_Level_ID,0),
		InspectionServiceLevel = ISNULL(EL.[Enhancement_Level_Desc], 'None')
FROM	dbo.DimStock DS WITH (NOLOCK)
		INNER JOIN #TMP_Enhancement_Salvage_IDs_InspectionServices ISS WITH (NOLOCK)
			ON ISS.Salvage_ID = DS.ASAPSalvageID
		LEFT OUTER JOIN [BI_ASAP_Rep].[dbo].[Enhancement_Level] EL WITH (NOLOCK)
			ON EL.[Enhancement_Level_ID] = ISS.Enhancement_Level_ID  

SET @UpdateRowCount_sp += @@ROWCOUNT

------Air Bag Indicators
SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Airbag_Ind

UPDATE DS
SET 
	IsAirbagMissing = ISNULL(T.Airbag__Missing_ind,0)
	,IsAirbagDeployed = ISNULL(T.Airbag_Deployed_Ind,0)
FROM 
dbo.DimStock DS 
INNER JOIN #TMP_Airbag_Ind T 
ON DS.ASAPSalvageID = T.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT

UPDATE DS SET 
	IsEnhancement = 1
FROM dbo.DimStock DS
INNER JOIN (SELECT DISTINCT Salvage_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement](@MinLSN,@MaxLSN,'all with merge') 
WHERE [__$operation] = 5 AND Charge_Type_ID NOT IN (126, 269) AND Status_Code IN ('WPN','WCP')) SE 
ON DS.ASAPSalvageID = SE.Salvage_ID

SET @UpdateRowCount_sp += @@ROWCOUNT

 --Updating data in DimStock from #TMP_Stock_Settlement_Unit_Data   
SELECT @ExtractRowCount_sp += COUNT(*)  FROM #TMP_Stock_Settlement_Unit_Data

UPDATE DS SET
      MinRemitDiscount = ISNULL(Min_Remit_Discount,0)
      ,ZeroDeficitAdjustmentAmount = ISNULL(Zero_Deficit_Adjustment_Amount,0)
      ,IsMinRemit = ISNULL(Min_Remit_Ind,0)
      ,IsZeroDeficitSettlement = ISNULL(Zero_Deficit_Settlement_Ind,0)
      ,NetSplitAdjustment = ISNULL([Net_Split_Adjustment] ,0)
      ,NetSplitAdjustmentInd=ISNULL([Net_Split_Adjustment_Ind] ,0)
      ,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock DS
	INNER JOIN #TMP_Stock_Settlement_Unit_Data TSSUA
		ON DS.ASAPSalvageID = TSSUA.Salvage_ID 

SET @UpdateRowCount_sp += @@ROWCOUNT

 --Updating data in DimStock from #Tmp_Assignment_Source_CSA   
SELECT @ExtractRowCount_sp += COUNT(*)  FROM #Tmp_Assignment_Source_CSA

UPDATE DS SET
	AssignmentSource= (case when TAS.code in ('AVIS','BRMU','CHRY','FARM','GEIC','JUCR','USAA') then 'Direct' when TAS.code in ('EVAA','CSAT') then 'CSA' when TAS.code = 'AASC' then 'AASC' when TAS.code = 'ADP' then 'Audatex' when TAS.code = 'AIMS' then 'AIMS' when TAS.code = 'CCCS' then 'CCC'when TAS.code = 'EAUT' then 'EAUT' when TAS.code = 'ESUR' then 'Process Claims' when TAS.code = 'PROC' then 'Process Claims' when TAS.code = 'MITC' then 'MITC' else 'Manual' END)
    ,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock DS
	INNER JOIN #Tmp_Assignment_Source_CSA TAS
		ON DS.ASAPSalvageID = TAS.Salvage_ID 

SET @UpdateRowCount_sp += @@ROWCOUNT

 --Updating data in DimStock from #Tmp_ProviderRefundAmount   
SELECT @ExtractRowCount_sp += COUNT(*)  FROM #Tmp_ProviderRefundAmount

UPDATE DS SET
	ProviderRefundAmount= PRA.RefundAmount
    ,ETLLoadUpdateID = @ETLLoadID_sp
FROM dbo.DimStock DS
	INNER JOIN #Tmp_ProviderRefundAmount PRA
		ON DS.ASAPSalvageID = PRA.Salvage_ID 

SET @UpdateRowCount_sp += @@ROWCOUNT


---- Updating data in DimStock from #TMP_Provider_Sale_Run_Counts 

--SELECT @ExtractRowCount_sp += COUNT(*) FROM #TMP_Provider_Sale_Run_Counts

--UPDATE DS SET
----SELECT Salvage_ID ,
--	 SaleRunCountExcludingCancel = Sale_Run_Count_Excluding_Cancel
--	,SaleRunCountExCancelExNoBid = Sale_Run_Count_ExCancel_ExNoBid
--	,ETLLoadUpdateID		   = @ETLLoadID_sp
--FROM #TMP_Provider_Sale_Run_Counts AS SRC
--	INNER JOIN dbo.DimStock AS DS 
--		ON DS.ASAPSalvageID = SRC.Salvage_ID

--SET @UpdateRowCount_sp += @@ROWCOUNT

--###
--==========================================================================================

--	Update the ASAP fields from Polk

--==========================================================================

COMMIT TRAN

--================================================================================
--================================================================================
--	Update the Adjustor names to keep them in sync with DimUser
--================================================================================
--	Populate the User temp table from the CDC table
--================================================================================
BEGIN TRAN

INSERT INTO #TMP_User	(
		  User_ID
		, User_Name
		, EffectiveDayID
		, Effective_Date
		, ExpirationDayID
		, Expiration_Date
		, First_Name
		, Middle_Initial
		, Last_Name
		, AdjustorPhone	)
SELECT 
	 User_ID
	,User_Name
	,CONVERT(INT, CONVERT(CHAR(8), Effective_Date, 112)) AS EffectiveDayID
	,Effective_Date
	,CONVERT(INT, CONVERT(CHAR(8), Expiration_Date, 112)) AS ExpirationDayID
	,Expiration_Date
	,First_Name
	,Middle_Initial
	,Last_Name
	,Phone_Number_1
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_User_Info] (	@MINLSN, @MaxLSN, 'all with merge'  )  
WHERE [__$operation] = 5
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%CANCELLED%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%DO NOT ACTIVATE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%DO NOT USE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%DON''T USE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%IACTIVE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%IANCTIVE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%INAACTIVE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%INACIVE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%INACTIV%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%IN-ACTIVE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%NOT ACTIVE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') NOT LIKE '%REMOVE%'
AND ISNULL(First_Name,'')+ISNULL(Last_Name,'') <> ''


CREATE UNIQUE CLUSTERED INDEX IX_TMP_UserID
    ON #TMP_User (User_ID)

--================================================================================
--========================== Update the Adjustor Name ============================
--================================================================================
Update	ds
Set   AdjustorFirstName		= 	dadj.First_Name
	, AdjustorLastName		=	dadj.Last_Name
	,AdjusterPhone			=	dadj.AdjustorPhone
FROM dbo.DimStock		AS	ds 
INNER Join	#TMP_User	AS	dadj WITH (NOLOCK)
				on	dadj.User_ID	=	ds.HandlerUserID
Where			ISNULL(ds.AdjustorLastName,'')		= ''
				OR UPPER(RTRIM(LTRIM(ds.AdjustorFirstName)))	<>	UPPER(RTRIM(LTRIM(dadj.First_Name)))
				OR UPPER(RTRIM(LTRIM(ds.AdjustorlastName)))	<>	UPPER(RTRIM(LTRIM(dadj.Last_Name)))	

Update ds Set AdjustorLastName	= dadj.LastName
		, AdjustorFirstName	= dadj.FirstName
From dbo.DimStock		AS	ds
INNER	Join	dbo.DimUser	AS	dadj WITH (NOLOCK)
				on	dadj.UserID		=	ds.HandlerUserID
				WHERE (ISNULL(AdjustorLastName,'')	= '' OR ISNULL(AdjustorFirstName,'')='')
				AND (ISNULL(dadj.LastName,'')<>'' OR ISNULL(dadj.FirstName,'')<>'')
--================================================================================
--========================== Update the Assignor Name ============================
--================================================================================

Update DS
Set		  AssignorLastName	= dass.Last_Name
		, AssignorFirstName	= dass.First_Name
FROM dbo.DimStock		AS	ds
INNER Join	#TMP_User	AS	dass WITH (NOLOCK)
				on	dass.User_ID		=	ds.AssignorUserID
Where 
		 UPPER(RTRIM(LTRIM(dass.Last_Name))) <> UPPER(RTRIM(LTRIM(ds.AssignorLastName)))
			AND	UPPER(RTRIM(LTRIM(dass.Last_Name)))	<> ' '

Update ds
Set		AssignorLastName	= dass.LastName
		, AssignorFirstName	= dass.FirstName
From dbo.DimStock		AS	ds
INNER JOIN dbo.DimUser	AS	dass WITH (NOLOCK)
				on	dass.UserID		=	ds.HandlerUserID
				WHERE (ISNULL(AssignorLastName,'')	= '' OR ISNULL(AssignorFirstName,'')='')
				AND (ISNULL(dass.LastName,'')<>'' OR ISNULL(dass.FirstName,'')<>'')

--================================================================================
--========================== Update the ClaimHandler Name ========================
--================================================================================

Update ds
Set   ClaimHandler	= dhdlr.First_Name + ' ' + dhdlr.Last_Name
From dbo.DimStock		AS	ds
INNER JOIN #TMP_User	AS	dhdlr WITH (NOLOCK)
				on	dhdlr.User_ID	=	ds.CallerUserID
Where 
		 UPPER(RTRIM(LTRIM(dhdlr.First_Name + ' ' + dhdlr.Last_Name))) <> UPPER(RTRIM(LTRIM(ds.ClaimHandler)))
			AND	UPPER(RTRIM(LTRIM(dhdlr.First_Name + ' ' + dhdlr.Last_Name))) <> ' '

Update ds
Set
		  ClaimHandler	= dass.FirstName + ' ' + dass.LastName

From dbo.DimStock		AS	ds
INNER JOIN dbo.DimUser	AS	dass WITH (NOLOCK)
				on	dass.UserID		=	ds.HandlerUserID
				WHERE (ISNULL(ClaimHandler,'')	= '' )
				AND (ISNULL(dass.LastName,'')<>'' OR ISNULL(dass.FirstName,'')<>'')

--================================================================================

COMMIT TRAN

--================================================================================
--========================== Update IshadTitleProblem=============================
--================================================================================

  SELECT SN.Salvage_ID,Max(SN.Update_DateTime) AS TitleReceivedRemovedDateTime
  INTO #TMP_TitleRcvdRmvd
  FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge') SN
  INNER JOIN EDW.dbo.DimStock AS DS WITH (NOLOCK)
	ON DS.ASAPSalvageID = SN.Salvage_ID
  WHERE  [__$operation] = 5
  AND SN.Note_Text like '%All title problems are removed because the Title Received Date is removed%'
  Group BY SN.Salvage_ID

   SELECT SEH.Salvage_ID
		,MAX(Event_DateTime) AS MAXEventDateTime
 Into #TMP_MaxEventHadTP
 FROM EDW.dbo.DimStock AS DS WITH (NOLOCK)
 INNER JOIN [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN,@MaxLSN,'all with merge') AS SEH 
  ON DS.ASAPSalvageID = SEH.Salvage_ID
WHERE [__$operation] = 5
AND Salvage_Event_Code IN ('TPRG') 
GROUP BY SEH.Salvage_ID

  SELECT TR.Salvage_ID,
         CASE WHEN TR.MAXEventDateTime  > TRR.TitleReceivedRemovedDateTime THEN 1 ELSE 0 END AS HadTitleProblemFlag,
         MAXEventDateTime ,TitleReceivedRemovedDateTime
  INTO #TMP_HadTitleProblem
  FROM  #TMP_MaxEventHadTP AS TR WITH (NOLOCK)
  INNER JOIN #TMP_TitleRcvdRmvd AS TRR WITH (NOLOCK)
   ON TR.Salvage_ID = TRR.Salvage_ID

     UPDATE DS
  SET IsHadTitleProblem = HadTitleProblemFlag
   FROM  EDW.dbo.DimStock AS DS WITH (NOLOCK)
  INNER JOIN #TMP_HadTitleProblem AS ME WITH (NOLOCK)
  ON ME.Salvage_ID = DS.ASAPSalvageID
  WHERE IsHadTitleProblem <> HadTitleProblemFlag

--================================================================================
--================================================================================


EXEC [dbo].[usp_ETLLoadAudit]
	@StartEnd = 'E',
	@ETLLoadID = @ETLLoadID_sp,
	@ExtractRowCount = @ExtractRowCount_sp,
	@InsertRowCount = @InsertRowCount_sp,
	@UpdateRowCount = @UpdateRowCount_sp

IF OBJECT_ID('tempdb..#TMP_Stock_Data_Stage') IS NOT NULL DROP TABLE #TMP_Stock_Data_Stage
IF OBJECT_ID('tempdb..#TMP_User') IS NOT NULL DROP TABLE #TMP_User
IF OBJECT_ID('tempdb..#TMP_Stock_Polk_Match_Source') IS NOT NULL DROP TABLE #TMP_Stock_Polk_Match_Source
IF OBJECT_ID('tempdb..#TMP_Polk_Prefix_Match_Source') IS NOT NULL DROP TABLE #TMP_Polk_Prefix_Match_Source
IF OBJECT_ID('tempdb..#TMP_Polk_First_Group') IS NOT NULL DROP TABLE #TMP_Polk_First_Group
IF OBJECT_ID('tempdb..#TMP_Polk_Second_Group') IS NOT NULL DROP TABLE #TMP_Polk_Second_Group
IF OBJECT_ID('tempdb..#TMP_Polk_Fourth_Group') IS NOT NULL DROP TABLE #TMP_Polk_Fourth_Group
IF OBJECT_ID('tempdb..#TMP_Polk_Fifth_Group') IS NOT NULL DROP TABLE #TMP_Polk_Fifth_Group
IF OBJECT_ID('tempdb..#TMP_Polk_Third_Group') IS NOT NULL DROP TABLE #TMP_Polk_Third_Group
IF OBJECT_ID('tempdb..#TMP_Unique_Stock_Item_Polk_Xref') IS NOT NULL DROP TABLE #TMP_Unique_Stock_Item_Polk_Xref
IF OBJECT_ID('tempdb..#TMP_FullBodyStyle_Data') IS NOT NULL DROP TABLE #TMP_FullBodyStyle_Data
IF OBJECT_ID('tempdb..#Tmp_Salvage_ID_2_Update') IS NOT NULL DROP TABLE #Tmp_Salvage_ID_2_Update
IF OBJECT_ID('tempdb..#Tmp_Storage_Location_Update') IS NOT NULL DROP TABLE #Tmp_Storage_Location_Update
IF OBJECT_ID('tempdb..#TMP_SLH_Data') IS NOT NULL DROP TABLE #TMP_SLH_Data
IF OBJECT_ID('tempdb..#TMP_SPI_Data') IS NOT NULL DROP TABLE #TMP_SPI_Data
IF OBJECT_ID('tempdb..#TMP_MinBS_Data') IS NOT NULL DROP TABLE #TMP_MinBS_Data
IF OBJECT_ID('tempdb..#TMP_CSADATE_Data') IS NOT NULL DROP TABLE #TMP_CSADATE_Data
IF OBJECT_ID('tempdb..#TMP_ST_Data') IS NOT NULL DROP TABLE #TMP_ST_Data
IF OBJECT_ID('tempdb..#TMP_SD_Data') IS NOT NULL DROP TABLE #TMP_SD_Data
IF OBJECT_ID('tempdb..#TMP_SSP_Data') IS NOT NULL DROP TABLE #TMP_SSP_Data
IF OBJECT_ID('tempdb..#TMP_SAI_Data') IS NOT NULL DROP TABLE #TMP_SAI_Data
IF OBJECT_ID('tempdb..#TMP_SC_Data') IS NOT NULL DROP TABLE #TMP_SC_Data
IF OBJECT_ID('tempdb..#TMP_SLCD_Data') IS NOT NULL DROP TABLE #TMP_SLCD_Data
IF OBJECT_ID('tempdb..#TMP_SCI_Data') IS NOT NULL DROP TABLE #TMP_SCI_Data
IF OBJECT_ID('tempdb..#TMP_PATC_Data') IS NOT NULL DROP TABLE #TMP_PATC_Data
IF OBJECT_ID('tempdb..#TMP_STow_Data') IS NOT NULL DROP TABLE #TMP_STow_Data
IF OBJECT_ID('tempdb..#TMP_SP_Data') IS NOT NULL DROP TABLE #TMP_SP_Data
IF OBJECT_ID('tempdb..#TMP_SSale_Data') IS NOT NULL DROP TABLE #TMP_SSale_Data
IF OBJECT_ID('tempdb..#TMP_SI_Data') IS NOT NULL DROP TABLE #TMP_SI_Data
IF Object_id('tempdb..#TMP_ASI_Data') IS NOT NULL DROP TABLE #TMP_ASI_Data
IF OBJECT_ID('tempdb..#TMP_IsStandardSale_Stage') IS NOT NULL DROP TABLE #TMP_IsStandardSale_Stage
IF OBJECT_ID('tempdb..#TMP_Current_Buyer_ID') IS NOT NULL DROP TABLE #TMP_Current_Buyer_ID
IF OBJECT_ID('tempdb..#temp_SalvageRecordCount') IS NOT NULL DROP TABLE #temp_SalvageRecordCount
IF OBJECT_ID('tempdb..#TMP_SExpense_Data') IS NOT NULL DROP TABLE #TMP_SExpense_Data
IF OBJECT_ID('tempdb..#TMP_Stock_Cancelled_Date') IS NOT NULL DROP TABLE #TMP_Stock_Cancelled_Date
IF OBJECT_ID('tempdb..#TMP_BCC_Data') IS NOT NULL DROP TABLE #TMP_BCC_Data
IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data
IF OBJECT_ID('tempdb..#TMP_SEH_Data_SalvageIDs') IS NOT NULL DROP TABLE #TMP_SEH_Data_SalvageIDs
IF OBJECT_ID('tempdb..#TMP_SEH_Data_BuyerRenegeCancel') IS NOT NULL DROP TABLE #TMP_SEH_Data_BuyerRenegeCancel
IF OBJECT_ID('tempdb..#TMP_Had_Title_Problem') IS NOT NULL DROP TABLE #TMP_Had_Title_Problem
IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data_HOLD3') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data_HOLD3
IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data_HOLD2') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data_HOLD2
IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data_HOLD') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data_HOLD
IF OBJECT_ID('tempdb..#TMP_Open_Stock_ACV') IS NOT NULL DROP TABLE #TMP_Open_Stock_ACV
IF OBJECT_ID('tempdb..#Tmp_Assignment_Source') IS NOT NULL DROP TABLE #Tmp_Assignment_Source
IF OBJECT_ID('tempdb..#TMP_CT_SEH_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SEH_SalvageIDs
IF OBJECT_ID('tempdb..#TMP_FactStockEvent') IS NOT NULL DROP TABLE #TMP_FactStockEvent
IF OBJECT_ID('tempdb..#TMP_Hold_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Dates
IF OBJECT_ID('tempdb..#TMP_Hold_Final_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Final_Dates
IF OBJECT_ID('tempdb..#TMP_Hold_Counts') IS NOT NULL DROP TABLE #TMP_Hold_Counts
IF OBJECT_ID('tempdb..#TMP_CourtOrder_Hold_Counts') IS NOT NULL DROP TABLE #TMP_CourtOrder_Hold_Counts
IF OBJECT_ID('tempdb..#TMP_BUlk_Sale_Salvage_ID') IS NOT NULL DROP TABLE #TMP_BUlk_Sale_Salvage_ID
IF OBJECT_ID('tempdb..#TMP_Bulk_Datetime') IS NOT NULL DROP TABLE #TMP_Bulk_Datetime
IF OBJECT_ID('tempdb..#TMP_Bulk_Sale_Ind') IS NOT NULL DROP TABLE #TMP_Bulk_Sale_Ind
IF OBJECT_ID('tempdb..#TMP_Gross_Return_Salvage_ID') IS NOT NULL DROP TABLE #TMP_Gross_Return_Salvage_ID
IF OBJECT_ID('tempdb..#TMP_AdvanceStorage') IS NOT NULL DROP TABLE #TMP_AdvanceStorage
IF OBJECT_ID('tempdb..#TMP_Advance_Storage_Info_ID') IS NOT NULL DROP TABLE #TMP_Advance_Storage_Info_ID
IF OBJECT_ID('tempdb..#TMP_SBF_Data') IS NOT NULL DROP TABLE #TMP_SBF_Data
IF OBJECT_ID('tempdb..#TMP_Title_Problem_Ind') IS NOT NULL DROP TABLE #TMP_Title_Problem_Ind
IF OBJECT_ID('tempdb..#TMPSoldModel_SalvageID') IS NOT NULL DROP TABLE #TMPSoldModel_SalvageID
IF OBJECT_ID('tempdb..#TMP_MODEL_AGE_BAND') IS NOT NULL DROP TABLE #TMP_MODEL_AGE_BAND
IF OBJECT_ID('tempdb..#TMP_Model') IS NOT NULL DROP TABLE #TMP_Model
IF OBJECT_ID('tempdb..#TMP_RunAndDrive_Auctions') IS NOT NULL DROP TABLE #TMP_RunAndDrive_Auctions
IF OBJECT_ID('tempdb..#TMP_Latest_Auction') IS NOT NULL DROP TABLE #TMP_Latest_Auction
IF OBJECT_ID('tempdb..#TMP_RunDriveInd') IS NOT NULL DROP TABLE #TMP_RunDriveInd
IF OBJECT_ID('tempdb..#TMP_ECI_Ind_Data') IS NOT NULL DROP TABLE #TMP_ECI_Ind_Data
IF OBJECT_ID('tempdb..#TMP_Enhancement_Salvage_IDs') IS NOT NULL DROP TABLE #TMP_Enhancement_Salvage_IDs
IF OBJECT_ID('tempdb..#TMP_ENhancement_IND') IS NOT NULL DROP TABLE #TMP_ENhancement_IND
IF OBJECT_ID('tempdb..#TMP_Airbag_Ind') IS NOT NULL DROP TABLE #TMP_Airbag_Ind
IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Event_IBCN_BT') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_IBCN_BT
IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Event_IBTR_IBND_BT') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_IBTR_IBND_BT
IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Event_REAB_BT') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_REAB_BT
IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Original_Update') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Original_Update
IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Provider_Original') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Provider_Original
IF OBJECT_ID('tempdb..#TMP_IBF_Renege') IS NOT NULL DROP TABLE #TMP_IBF_Renege
IF OBJECT_ID('tempdb..#TMP_IBN_Sold') IS NOT NULL DROP TABLE #TMP_IBN_Sold
IF OBJECT_ID('tempdb..#TMP_Stock_Settlement_Unit_Data') IS NOT NULL DROP TABLE #TMP_Stock_Settlement_Unit_Data
IF OBJECT_ID('tempdb..#Tmp_ProviderRefundAmount') IS NOT NULL DROP TABLE #Tmp_ProviderRefundAmount
IF OBJECT_ID('tempdb..#TMP_VINDecodeWk') IS NOT NULL DROP TABLE #TMP_VINDecodeWk
IF OBJECT_ID('tempdb..#TMP_VINDecodeReturn') IS NOT NULL DROP TABLE #TMP_VINDecodeReturn
IF OBJECT_ID('tempdb..#VINDecodeSourceExtract') IS NOT NULL DROP TABLE #VINDecodeSourceExtract
IF OBJECT_ID('tempdb..#TMP_Enhancement_Salvage_IDs_InspectionServices') IS NOT NULL DROP TABLE #TMP_Enhancement_Salvage_IDs_InspectionServices
IF OBJECT_ID('tempdb..#TMP_Enhancement_Salvage_IDs_InspectionServices_RuleXML') IS NOT NULL DROP TABLE #TMP_Enhancement_Salvage_IDs_InspectionServices_RuleXML
IF OBJECT_ID('tempdb..#TMP_InspectionserviceIndicators') IS NOT NULL DROP TABLE #TMP_InspectionserviceIndicators
IF OBJECT_ID('tempdb..#TMP_TitleRcvdRmvd') IS NOT NULL DROP TABLE #TMP_TitleRcvdRmvd
IF OBJECT_ID('tempdb..#TMP_MaxEventHadTP') IS NOT NULL DROP TABLE #TMP_MaxEventHadTP
IF OBJECT_ID('tempdb..#TMP_HadTitleProblem') IS NOT NULL DROP TABLE #TMP_HadTitleProblem
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
		
		IF OBJECT_ID('tempdb..#TMP_Stock_Data_Stage') IS NOT NULL DROP TABLE #TMP_Stock_Data_Stage
		IF OBJECT_ID('tempdb..#TMP_Stock_Polk_Match_Source') IS NOT NULL DROP TABLE #TMP_Stock_Polk_Match_Source
		IF OBJECT_ID('tempdb..#TMP_Polk_Prefix_Match_Source') IS NOT NULL DROP TABLE #TMP_Polk_Prefix_Match_Source
		IF OBJECT_ID('tempdb..#TMP_Polk_First_Group') IS NOT NULL DROP TABLE #TMP_Polk_First_Group
		IF OBJECT_ID('tempdb..#TMP_Polk_Second_Group') IS NOT NULL DROP TABLE #TMP_Polk_Second_Group
		IF OBJECT_ID('tempdb..#TMP_Polk_Fourth_Group') IS NOT NULL DROP TABLE #TMP_Polk_Fourth_Group
		IF OBJECT_ID('tempdb..#TMP_Polk_Fifth_Group') IS NOT NULL DROP TABLE #TMP_Polk_Fifth_Group
		IF OBJECT_ID('tempdb..#TMP_Polk_Third_Group') IS NOT NULL DROP TABLE #TMP_Polk_Third_Group
		IF OBJECT_ID('tempdb..#TMP_Unique_Stock_Item_Polk_Xref') IS NOT NULL DROP TABLE #TMP_Unique_Stock_Item_Polk_Xref
		IF OBJECT_ID('tempdb..#TMP_FullBodyStyle_Data') IS NOT NULL DROP TABLE #TMP_FullBodyStyle_Data
		IF OBJECT_ID('tempdb..#Tmp_Salvage_ID_2_Update') IS NOT NULL DROP TABLE #Tmp_Salvage_ID_2_Update
		IF OBJECT_ID('tempdb..#Tmp_Storage_Location_Update') IS NOT NULL DROP TABLE #Tmp_Storage_Location_Update
		IF OBJECT_ID('tempdb..#TMP_SLH_Data') IS NOT NULL DROP TABLE #TMP_SLH_Data
		IF OBJECT_ID('tempdb..#TMP_SPI_Data') IS NOT NULL DROP TABLE #TMP_SPI_Data
		IF OBJECT_ID('tempdb..#TMP_MinBS_Data') IS NOT NULL DROP TABLE #TMP_MinBS_Data
		IF OBJECT_ID('tempdb..#TMP_CSADATE_Data') IS NOT NULL DROP TABLE #TMP_CSADATE_Data
		IF OBJECT_ID('tempdb..#TMP_ST_Data') IS NOT NULL DROP TABLE #TMP_ST_Data
		IF OBJECT_ID('tempdb..#TMP_SD_Data') IS NOT NULL DROP TABLE #TMP_SD_Data
		IF OBJECT_ID('tempdb..#TMP_SSP_Data') IS NOT NULL DROP TABLE #TMP_SSP_Data
		IF OBJECT_ID('tempdb..#TMP_SAI_Data') IS NOT NULL DROP TABLE #TMP_SAI_Data
		IF OBJECT_ID('tempdb..#TMP_SC_Data') IS NOT NULL DROP TABLE #TMP_SC_Data
		IF OBJECT_ID('tempdb..#TMP_SLCD_Data') IS NOT NULL DROP TABLE #TMP_SLCD_Data
		IF OBJECT_ID('tempdb..#TMP_SCI_Data') IS NOT NULL DROP TABLE #TMP_SCI_Data
		IF OBJECT_ID('tempdb..#TMP_PATC_Data') IS NOT NULL DROP TABLE #TMP_PATC_Data
		IF OBJECT_ID('tempdb..#TMP_STow_Data') IS NOT NULL DROP TABLE #TMP_STow_Data
		IF OBJECT_ID('tempdb..#TMP_SP_Data') IS NOT NULL DROP TABLE #TMP_SP_Data
		IF OBJECT_ID('tempdb..#TMP_SSale_Data') IS NOT NULL DROP TABLE #TMP_SSale_Data
		IF OBJECT_ID('tempdb..#TMP_SI_Data') IS NOT NULL DROP TABLE #TMP_SI_Data
		IF Object_id('tempdb..#TMP_ASI_Data') IS NOT NULL DROP TABLE #TMP_ASI_Data
		IF OBJECT_ID('tempdb..#TMP_IsStandardSale_Stage') IS NOT NULL DROP TABLE #TMP_IsStandardSale_Stage
		IF OBJECT_ID('tempdb..#TMP_Current_Buyer_ID') IS NOT NULL DROP TABLE #TMP_Current_Buyer_ID
		IF OBJECT_ID('tempdb..#temp_SalvageRecordCount') IS NOT NULL DROP TABLE #temp_SalvageRecordCount
		IF OBJECT_ID('tempdb..#TMP_SExpense_Data') IS NOT NULL DROP TABLE #TMP_SExpense_Data
		IF OBJECT_ID('tempdb..#TMP_Stock_Cancelled_Date') IS NOT NULL DROP TABLE #TMP_Stock_Cancelled_Date
		IF OBJECT_ID('tempdb..#TMP_BCC_Data') IS NOT NULL DROP TABLE #TMP_BCC_Data
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data
		IF OBJECT_ID('tempdb..#TMP_SEH_Data_SalvageIDs') IS NOT NULL DROP TABLE #TMP_SEH_Data_SalvageIDs
		IF OBJECT_ID('tempdb..#TMP_SEH_Data_BuyerRenegeCancel') IS NOT NULL DROP TABLE #TMP_SEH_Data_BuyerRenegeCancel
		IF OBJECT_ID('tempdb..#TMP_Had_Title_Problem') IS NOT NULL DROP TABLE #TMP_Had_Title_Problem
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data_HOLD3') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data_HOLD3
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data_HOLD2') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data_HOLD2
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_History_Data_HOLD') IS NOT NULL DROP TABLE #TMP_Salvage_Event_History_Data_HOLD
		IF OBJECT_ID('tempdb..#TMP_Open_Stock_ACV') IS NOT NULL DROP TABLE #TMP_Open_Stock_ACV
		IF OBJECT_ID('tempdb..#Tmp_Assignment_Source') IS NOT NULL DROP TABLE #Tmp_Assignment_Source
        IF OBJECT_ID('tempdb..#TMP_CT_SEH_SalvageIDs') IS NOT NULL DROP TABLE #TMP_CT_SEH_SalvageIDs
		IF OBJECT_ID('tempdb..#TMP_FactStockEvent') IS NOT NULL DROP TABLE #TMP_FactStockEvent
		IF OBJECT_ID('tempdb..#TMP_Hold_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Dates
		IF OBJECT_ID('tempdb..#TMP_Hold_Final_Dates') IS NOT NULL DROP TABLE #TMP_Hold_Final_Dates
		IF OBJECT_ID('tempdb..#TMP_Hold_Counts') IS NOT NULL DROP TABLE #TMP_Hold_Counts
		IF OBJECT_ID('tempdb..#TMP_CourtOrder_Hold_Counts') IS NOT NULL DROP TABLE #TMP_CourtOrder_Hold_Counts
		IF OBJECT_ID('tempdb..#TMP_BUlk_Sale_Salvage_ID') IS NOT NULL DROP TABLE #TMP_BUlk_Sale_Salvage_ID
		IF OBJECT_ID('tempdb..#TMP_Bulk_Datetime') IS NOT NULL DROP TABLE #TMP_Bulk_Datetime
		IF OBJECT_ID('tempdb..#TMP_Bulk_Sale_Ind') IS NOT NULL DROP TABLE #TMP_Bulk_Sale_Ind
		IF OBJECT_ID('tempdb..#TMP_Gross_Return_Salvage_ID') IS NOT NULL DROP TABLE #TMP_Gross_Return_Salvage_ID
		IF OBJECT_ID('tempdb..#TMP_AdvanceStorage') IS NOT NULL DROP TABLE #TMP_AdvanceStorage
        IF OBJECT_ID('tempdb..#TMP_Advance_Storage_Info_ID') IS NOT NULL DROP TABLE #TMP_Advance_Storage_Info_ID
		IF OBJECT_ID('tempdb..#TMP_SBF_Data') IS NOT NULL DROP TABLE #TMP_SBF_Data
        IF OBJECT_ID('tempdb..#TMP_Title_Problem_Ind') IS NOT NULL DROP TABLE #TMP_Title_Problem_Ind
        IF OBJECT_ID('tempdb..#TMPSoldModel_SalvageID') IS NOT NULL DROP TABLE #TMPSoldModel_SalvageID
        IF OBJECT_ID('tempdb..#TMP_MODEL_AGE_BAND') IS NOT NULL DROP TABLE #TMP_MODEL_AGE_BAND
        IF OBJECT_ID('tempdb..#TMP_Model') IS NOT NULL DROP TABLE #TMP_Model
		IF OBJECT_ID('tempdb..#TMP_RunAndDrive_Auctions') IS NOT NULL DROP TABLE #TMP_RunAndDrive_Auctions
		IF OBJECT_ID('tempdb..#TMP_Latest_Auction') IS NOT NULL DROP TABLE #TMP_Latest_Auction
		IF OBJECT_ID('tempdb..#TMP_RunDriveInd') IS NOT NULL DROP TABLE #TMP_RunDriveInd
	    IF OBJECT_ID('tempdb..#TMP_ECI_Ind_Data') IS NOT NULL DROP TABLE #TMP_ECI_Ind_Data
		IF OBJECT_ID('tempdb..#TMP_Enhancement_Salvage_IDs') IS NOT NULL DROP TABLE #TMP_Enhancement_Salvage_IDs
		IF OBJECT_ID('tempdb..#TMP_ENhancement_IND') IS NOT NULL DROP TABLE #TMP_ENhancement_IND
		IF OBJECT_ID('tempdb..#TMP_Airbag_Ind') IS NOT NULL DROP TABLE #TMP_Airbag_Ind
		IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Event_IBCN_BT') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_IBCN_BT
		IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Event_IBTR_IBND_BT') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_IBTR_IBND_BT
		IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Event_REAB_BT') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Event_REAB_BT
		IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Original_Update') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Original_Update
		IF OBJECT_ID('tempdb..#TMP_CT_Salavage_Provider_Original') IS NOT NULL DROP TABLE #TMP_CT_Salavage_Provider_Original
		IF OBJECT_ID('tempdb..#TMP_IBF_Renege') IS NOT NULL DROP TABLE #TMP_IBF_Renege
        IF OBJECT_ID('tempdb..#TMP_IBN_Sold') IS NOT NULL DROP TABLE #TMP_IBN_Sold
        IF OBJECT_ID('tempdb..#TMP_Stock_Settlement_Unit_Data') IS NOT NULL DROP TABLE #TMP_Stock_Settlement_Unit_Data
		IF OBJECT_ID('tempdb..#Tmp_ProviderRefundAmount') IS NOT NULL DROP TABLE #Tmp_ProviderRefundAmount
		IF OBJECT_ID('tempdb..#TMP_VINDecodeWk') IS NOT NULL DROP TABLE #TMP_VINDecodeWk
		IF OBJECT_ID('tempdb..#TMP_VINDecodeReturn') IS NOT NULL DROP TABLE #TMP_VINDecodeReturn
		IF OBJECT_ID('tempdb..#VINDecodeSourceExtract') IS NOT NULL DROP TABLE #VINDecodeSourceExtract
		IF OBJECT_ID('tempdb..#TMP_Enhancement_Salvage_IDs_InspectionServices') IS NOT NULL DROP TABLE #TMP_Enhancement_Salvage_IDs_InspectionServices
		IF OBJECT_ID('tempdb..#TMP_Enhancement_Salvage_IDs_InspectionServices_RuleXML') IS NOT NULL DROP TABLE #TMP_Enhancement_Salvage_IDs_InspectionServices_RuleXML
		IF OBJECT_ID('tempdb.. #TMP_InspectionserviceIndicators') IS NOT NULL DROP TABLE #TMP_InspectionserviceIndicators
		IF OBJECT_ID('tempdb..#TMP_TitleRcvdRmvd') IS NOT NULL DROP TABLE #TMP_TitleRcvdRmvd
        IF OBJECT_ID('tempdb..#TMP_MaxEventHadTP') IS NOT NULL DROP TABLE #TMP_MaxEventHadTP
        IF OBJECT_ID('tempdb..#TMP_HadTitleProblem') IS NOT NULL DROP TABLE #TMP_HadTitleProblem

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	

END CATCH



GO


