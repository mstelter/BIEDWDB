
/* 
USE EDW
GO 
==============================================================================
Author		: Juan X. Jacome
Create date	: 08/11/2011
Description	: Populate the EDW DimProvider table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer			Release/Problem Number	
09/29/2011	Preetham Duvva		Updated the script with to be objects and also for nulls. 
10/24/2011	Juan X. Jacome		Added code to drop temp Tables (DBA Code Review)   
06/26/2012  Rob Durkee			Added Account Manager      
7/20/2012   Sidd Murao          Modifeid update for Account Manager    
09/11/2012  Venkata Vempali     Commented code update for Account Manager
                                Modified a Join from INNER to LEFT OUTER at Update #TMP_Provider 
09/18/2012  Preetham Duvva      Added code for IsSecondOfferProgramParticipant indicator.                
11/29/2012	MAtt Stelter		Added code to correct Manager for INS
12/04/2012	Juan X. Jacome      Added code PrepaidAdvanceChargeInd, GlobalMaximumAuthorizedAmount
05/28/2013  MAtt Stelter        Added code for IsTitleDirectParticipant indicator. 
07/12/2013  John Stires         Added code for IsNonStandardProvider indicator. 
7/17/2013	Matt Stelter		Changed NON Standard to only INS and ADJ Types
4/3/2013	Matt Stelter		Added AccountManagerCode 
5/5/2014	Matt Stelter		Added IsLiveRepParticipant
6/10/2014	Matt Stelter		Added Title Notification Participant Indicators
6/25/2014   Yogitha Alwarsetty  Modified code for IsNonStandardProvider indicator
3/29/2016	Matt Stelter		Fix for US in State and State In Zip
12/14/2016  Yogitha Alwarsetty  Modified code for IsNonStandardProvider indicator and added TAF Ind
==============================================================================
Usage Example:
Exec usp_ETLDimProvider   SELECT * FROM EDW.dbo.DimProvider WHERE IsNonStandardProvider = 1
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimProvider]
AS     
BEGIN


DECLARE @ETLLoadID        INT,
		@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
	SET NOCOUNT ON;

BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S', @PackageName = 'uspETLDimProvider'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'


--Exec @ETLLoadID

/******Updated Providers******/
IF OBJECT_ID('tempdb..#TMP_UpdatedProviders') IS NOT NULL DROP TABLE #TMP_UpdatedProviders
CREATE TABLE #TMP_UpdatedProviders
( Salvage_Provider_ID [INT] NOT NULL)

/******2nd Offer******/
IF OBJECT_ID('tempdb..#TMP_Sndoffer') IS NOT NULL DROP TABLE #TMP_Sndoffer
CREATE TABLE #TMP_Sndoffer
( Salvage_Provider_ID [INT] NOT NULL,
    SecondOfferProgramParticipantInd [TINYINT] NULL)

/******Title Direct Participant******/
IF OBJECT_ID('tempdb..#TMP_TitleDirect') IS NOT NULL DROP TABLE #TMP_TitleDirect
CREATE TABLE #TMP_TitleDirect
( Salvage_Provider_ID [INT] NOT NULL,
    IsTitleDirectParticipant  [TINYINT] NULL)

/******Live Rep Participant******/
IF OBJECT_ID('tempdb..#TMP_LiveRep') IS NOT NULL DROP TABLE #TMP_LiveRep
CREATE TABLE #TMP_LiveRep
( Salvage_Provider_ID [INT] NOT NULL,
    IsLiveRepParticipant  [TINYINT] NULL)

/******Title Notifications Participant******/
IF OBJECT_ID('tempdb..#TMP_TitleDirectNotifications') IS NOT NULL DROP TABLE #TMP_TitleDirectNotifications
CREATE TABLE #TMP_TitleDirectNotifications
( Salvage_Provider_ID [INT] NOT NULL,
    IsTitleDirectNotificationsParticipant  [TINYINT] NULL,
    IsTitleProblemNotificationsParticipant  [TINYINT] NULL)

/*********************************************************************************************
Create the temp table to consolidate Provider
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_Provider') IS NOT NULL DROP TABLE #TMP_Provider
CREATE TABLE #TMP_Provider
(	[Salvage_Provider_ID] [int] NOT NULL,
	[Salvage_Provider_Name] [varchar](60) NULL,
	[Address_Line_1] [varchar](40) NULL,
	[Address_Line_2] [varchar](40) NULL,
	[City_Name] [varchar](40) NULL,
	[Country_Abbreviation] [char](2) NULL,
	[State_Abbreviation] [char](3) NULL,
	[Zip_Code] [varchar](9) NULL,
	[County_Name] [varchar](30) NULL,
	[Country_Name] [varchar](60) NULL,
	[Phone_Number_1] [decimal](11, 0) NULL,
	[Phone_Extension_1] [decimal](5, 0) NULL,
	[Fax_Number] [decimal](11, 0) NULL,
	[Call_for_Auction_Sale_Authorization_Ind] [bit] NULL,
	[Home_Branch_Number] [int] NULL,
	[Salvage_Provider_Administrative_Branch_Number] [int] NULL,
	[Salvage_Provider_Group_Name] [varchar](60) NULL,
	[Salvage_Provider_Status_Code] [char](3) NULL,
	[Salvage_Provider_Type_Code] [char](3) NULL,
	[Salvage_Provider_Type_Description] [varchar](30) NULL,
	[Express_Pickup_Tow_Ind] [bit] NULL,
	[Alternate_Payee_Access_Ind] [bit] NULL,
	[Prep_Adv_Charge_Ind] [bit] NULL,
	[VIC_Email] [varchar](1200) NULL,
	[VIC_Email_Assignment_Ind] [bit] NULL,
	[VIC_Email_Login_Ind] [bit] NULL,
	[Show_Loss_Code_Ind] [bit] NULL,
	[IBN_Participant_Ind] [tinyint] NULL,
	[Salvage_Provider_Group_ID] [int] NULL,
	[Salvage_Provider_Option_Type_ID] [nvarchar] (30) NULL,
	[EDI_Partner_Ind] [tinyint] NULL,
	[Federal_Tax_ID] [nvarchar](9) NULL,
	[Minimum_Prebill_Amount] [decimal] (11,2) NULL,
    [Option_Name] [nvarchar] (30) NULL,
	[Salvage_Provider_Question_Ind] [tinyint] NULL,
	[Remarketing_Provider_Ind] [tinyint] NULL,
	[Settlement_Packet_Express_Ind] [tinyint] NULL,
	[Assign_All_Salvages_To_Specialty_Ind] [tinyint]NULL, 
	[Account_Manager_ID] [int] null,
	[Account_Manager_Description] [varchar](50) null,
    SecondOfferProgramParticipantInd [TINYINT] NULL,
    IsTitleDirectParticipant [TINYINT] NULL,
	Account_Manager_Code VARCHAR(8) NULL,
	IsLiveRepParticipant [TINYINT] NULL,
    IsTitleDirectNotificationsParticipant  [TINYINT] NULL,
    IsTitleProblemNotificationsParticipant  [TINYINT] NULL,
	ISTPA [TINYINT] NULL
)

/*********************************************************************************************
 Get the ProviderID of Non-Standard Indiactor values from the Codes Table 
*********************************************************************************************/        
IF Object_id('tempdb..#TMP_NonStandardProvider') IS NOT NULL 
DROP TABLE #TMP_NonStandardProvider
Create TABLE #TMP_NonStandardProvider
(
ProviderId INT
)
Insert Into #TMP_NonStandardProvider
(
ProviderId 
)
select Distinct coalesce( CT.NumericID ,DP.ProviderId )ProviderId
from  EDW.dbo.dimprovider dp WITH (NOLOCK)
Inner join  EDW.dbo.CodeStable AS CT WITH (NOLOCK)
 on DP.ProviderId = CT.NumericID 
 or DP.ProviderGroupName = CT.AlphaID
INNER JOIN dbo.CodeStable AS CT2 WITH (NOLOCK)
 ON CT2.CodeType = 'NonStdPrv'
 And CT2.KeyType = 'ProviderTypeCode'
 AND DP.ProviderTypeCode = CT2.AlphaID
where CT.CodeType = 'NonStdPrv'

/*********************************************************************************************
 Get the ProviderID of TPA Indiactor values from the Codes Table 
*********************************************************************************************/        
IF Object_id('tempdb..#TMP_TPA') IS NOT NULL 
DROP TABLE #TMP_TPA
Create TABLE #TMP_TPA
(
ProviderId INT
)
Insert Into #TMP_TPA
(
ProviderId 
)
select Distinct coalesce( CT.NumericID ,DP.ProviderId )ProviderId
from  EDW.dbo.dimprovider dp WITH (NOLOCK)
Inner join  EDW.dbo.CodeStable AS CT WITH (NOLOCK)
 on DP.ProviderId = CT.NumericID 
 or DP.ProviderGroupName = CT.AlphaID
where CT.CodeType = 'TPA'
	
IF OBJECT_ID('tempdb..#tmp_ProviderRecordCount') IS NOT NULL DROP TABLE #tmp_ProviderRecordCount
CREATE TABLE #tmp_ProviderRecordCount(ChangeType VARCHAR(30)) 

/******Get Changed Providers******/

INSERT INTO #TMP_UpdatedProviders
   ([Salvage_Provider_ID]) 	
SELECT	[Salvage_Provider_ID]
FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider(
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5
UNION
SELECT	[Salvage_Provider_ID]
FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider_Program_XRef(
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5

/******2nd Offer******/

--Insert values from CDC Salvage Provider Program XRef

INSERT INTO #TMP_Sndoffer
(
    Salvage_Provider_ID,
    SecondOfferProgramParticipantInd  
)

SELECT DISTINCT Salvage_Provider_ID ,       
	   CASE WHEN Is_Active = 1 THEN 1 ELSE 0 END AS SecondOfferProgramParticipantInd
FROM  [BI_ASAP_Rep].dbo.Salvage_Provider_Program_XRef PPX
INNER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Provider_Program] PPR ON PPX.Salvage_Provider_Program_ID =PPR.Salvage_Provider_Program_ID
WHERE  [Salvage_Provider_Program_Name] = 'Final Offer' 

/******Title Direct Participant******/
--Insert values from CDC Salvage Provider Program XRef

INSERT INTO #TMP_TitleDirect
(Salvage_Provider_ID,
 IsTitleDirectParticipant)

SELECT DISTINCT Salvage_Provider_ID ,       
	   CASE WHEN Is_Active = 1 THEN 1 ELSE 0 END AS IsTitleDirectParticipant
FROM  [BI_ASAP_Rep].dbo.Salvage_Provider_Program_XRef PPX
INNER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Provider_Program] PPR ON PPX.Salvage_Provider_Program_ID =PPR.Salvage_Provider_Program_ID
WHERE  [Salvage_Provider_Program_Name] = 'Title Direct'

/******LiveRep Participant******/
--Insert values from CDC Salvage Provider Program XRef

INSERT INTO #TMP_LiveRep
(Salvage_Provider_ID,
 IsLiveRepParticipant)

SELECT DISTINCT Salvage_Provider_ID ,       
	   CASE WHEN Is_Active = 1 THEN 1 ELSE 0 END AS IsLiveRepParticipant
FROM  [BI_ASAP_Rep].dbo.Salvage_Provider_Program_XRef PPX
INNER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Provider_Program] PPR ON PPX.Salvage_Provider_Program_ID =PPR.Salvage_Provider_Program_ID
WHERE  [Salvage_Provider_Program_Name] = 'LiveRep'  

/******Title Notifications Participant******/

INSERT INTO #TMP_TitleDirectNotifications
(Salvage_Provider_ID,
 IsTitleDirectNotificationsParticipant,
 IsTitleProblemNotificationsParticipant)
SELECT Salvage_Provider_ID ,       
	   MAX(CASE WHEN Is_Active = 1 AND [Salvage_Provider_Program_Name] = 'Title Direct Notifications' THEN 1 ELSE 0 END) AS IsTitleDirectNotificationsParticipant,
	   MAX(CASE WHEN Is_Active = 1 AND [Salvage_Provider_Program_Name] = 'Title Problem Notifications' THEN 1 ELSE 0 END) AS IsTitleProblemNotificationsParticipant
FROM  [BI_ASAP_Rep].dbo.Salvage_Provider_Program_XRef PPX
INNER JOIN [BI_ASAP_Rep].[dbo].[Salvage_Provider_Program] PPR ON PPX.Salvage_Provider_Program_ID =PPR.Salvage_Provider_Program_ID
WHERE  [Salvage_Provider_Program_Name] IN ('Title Direct Notifications','Title Problem Notifications') 
GROUP BY Salvage_Provider_ID

/*********************************************************************************************
Create a temp table and Insert the values from the CDC Salvage Provider table.
*********************************************************************************************/
INSERT INTO #TMP_Provider
   ([Salvage_Provider_ID],
	[Salvage_Provider_Name],
	[Address_Line_1],
	[Address_Line_2],
	[City_Name],
	[Country_Abbreviation],
	[State_Abbreviation],
	[Zip_Code],
	[County_Name],
	[Phone_Number_1],
	[Phone_Extension_1],
	[Fax_Number],
	[Call_for_Auction_Sale_Authorization_Ind],
	[Home_Branch_Number],
	[Salvage_Provider_Administrative_Branch_Number],
	[Salvage_Provider_Status_Code],
	[Salvage_Provider_Type_Code],
	[Alternate_Payee_Access_Ind],
	[Prep_Adv_Charge_Ind],
	[VIC_Email],
	[Show_Loss_Code_Ind],
	[IBN_Participant_Ind],
	[Salvage_Provider_Group_ID],
	[Salvage_Provider_Option_Type_ID], 
	[EDI_Partner_Ind], 
	[Federal_Tax_ID], 
	[Minimum_Prebill_Amount], 
	[Salvage_Provider_Question_Ind], 
	[Remarketing_Provider_Ind], 
	[Settlement_Packet_Express_Ind], 
	[Assign_All_Salvages_To_Specialty_Ind]
	--,[Account_Manager_ID]
	,SecondOfferProgramParticipantInd
	,IsTitleDirectParticipant,
    IsTitleDirectNotificationsParticipant,
    IsTitleProblemNotificationsParticipant,
	IsTPA
	) 	
SELECT	SP.[Salvage_Provider_ID],
	[Salvage_Provider_Name],
	[Address_Line_1],
	[Address_Line_2],
	[City_Name],
	[Country_Abbreviation],
	[State_Abbreviation],
	[Zip_Code],
	[County_Name],
	[Phone_Number_1],
	[Phone_Extension_1],
	[Fax_Number],
	[Call_for_Auction_Sale_Authorization_Ind],
	[Home_Branch_Number],
	[Salvage_Provider_Administrative_Branch_Number],
	[Salvage_Provider_Status_Code],
	[Salvage_Provider_Type_Code],
	[Alternate_Payee_Access_Ind],
	[Prep_Adv_Charge_Ind],
	[VIC_Email],
	[Show_Loss_Code_Ind],
	[IBN_Participant_Ind],
	[Salvage_Provider_Group_ID],
	[Salvage_Provider_Option_Type_ID], 
	[EDI_Partner_Ind], 
	[Federal_Tax_ID], 
	[Minimum_Prebill_Amount], 
	[Salvage_Provider_Question_Ind], 
	[Remarketing_Provider_Ind], 
	[Settlement_Packet_Express_Ind], 
	[Assign_All_Salvages_To_Specialty_Ind]
	--,[Salvage_Provider_Account_Manager_ID]
	,0,0,0,0,0
--FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider(
--   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5
FROM [BI_ASAP_Rep].dbo.Salvage_Provider SP WITH (NOLOCK)
INNER JOIN #TMP_UpdatedProviders UP WITH (NOLOCK)  
	ON SP.Salvage_Provider_ID = UP.Salvage_Provider_ID

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_Provider

--Select @ExtractRowCount_sp
/*********************************************************************************************
Update all the Provider information in the temp table
*********************************************************************************************/
UPDATE #TMP_Provider
SET [Account_Manager_ID]= SP.[Salvage_Provider_Account_Manager_ID]
FROM #TMP_Provider P 
INNER JOIN [BI_ASAP_REP].dbo.Salvage_Provider SP with (Nolock)
ON P.Salvage_Provider_ID = SP.Salvage_Provider_ID

UPDATE #TMP_Provider
SET [Country_Name] = C.Country_Name,
	[Salvage_Provider_Group_Name]= SPG.Salvage_Provider_Group_Name,
	[Salvage_Provider_Type_Description] = SPT.Salvage_Provider_Type_Description,
	[Express_Pickup_Tow_Ind] = SPB.Express_Pickup_Tow_Ind,
	[VIC_Email_Assignment_Ind] = SPB.VIC_Email_Assignment_Ind,
	[VIC_Email_Login_Ind] = SPB.VIC_Email_Login_Ind,
	[Option_Name] = SPO.Option_Name,
	[Account_Manager_Description] = spam.Account_Manager_Description,
	Account_Manager_Code = spam.[CODE]
FROM #TMP_Provider P with (Nolock)
LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Salvage_Provider_Group SPG with (Nolock)
ON P.Salvage_Provider_Group_ID = SPG.Salvage_Provider_Group_ID
INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Provider_Type SPT with (Nolock)
ON P.Salvage_Provider_Type_Code =SPT.Salvage_Provider_Type_Code
LEFT OUTER JOIN [BI_ASAP_REP].dbo.Salvage_Provider_Branch SPB with (Nolock)
ON P.Salvage_Provider_ID = SPB.Salvage_Provider_ID
LEFT OUTER JOIN [BI_ASAP_REP].dbo.Country C with (Nolock)
ON P.Country_Abbreviation = C.Country_Abbreviation
LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Salvage_Provider_Option_Type SPO with (Nolock)
ON P.Salvage_Provider_Option_Type_ID = SPO.Salvage_Provider_Option_Type_ID
LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Salvage_Provider_Account_Manager spam with (Nolock)
ON p.Account_Manager_ID = spam.Salvage_Provider_Account_Manager_ID


--Update table 2nd Offer program participant ind

UPDATE TP
SET SecondOfferProgramParticipantInd = SP.SecondOfferProgramParticipantInd
FROM #TMP_Provider TP WITH(NOLOCK)
INNER JOIN #TMP_Sndoffer SP ON TP.Salvage_Provider_ID = SP.Salvage_Provider_ID

--Update table Title Direct participant ind

UPDATE TP
SET IsTitleDirectParticipant = SP.IsTitleDirectParticipant
FROM #TMP_Provider TP WITH(NOLOCK)
INNER JOIN #TMP_TitleDirect SP ON TP.Salvage_Provider_ID = SP.Salvage_Provider_ID

UPDATE TP
SET IsLiveRepParticipant = SP.IsLiveRepParticipant
FROM #TMP_Provider TP WITH(NOLOCK)
INNER JOIN #TMP_LiveRep SP ON TP.Salvage_Provider_ID = SP.Salvage_Provider_ID

UPDATE TP SET 
	IsTitleDirectNotificationsParticipant = TDN.IsTitleDirectNotificationsParticipant,
	IsTitleProblemNotificationsParticipant = TDN.IsTitleProblemNotificationsParticipant
--SELECT * 
FROM #TMP_Provider TP
INNER JOIN #TMP_TitleDirectNotifications TDN WITH (NOLOCK)
	ON TP.Salvage_Provider_ID = TDN.Salvage_Provider_ID

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimProvider table
This does Update and Insert only.  No Deletes
*********************************************************************************************/


BEGIN TRANSACTION 
  
    MERGE [dbo].[DimProvider] AS DP
    USING #TMP_Provider AS TP
          ON TP.Salvage_Provider_ID = DP.ProviderID
    
    WHEN MATCHED THEN 
        UPDATE SET 
			ProviderName = ISNULL([Salvage_Provider_Name],''),
			AddressLine1 = ISNULL([Address_Line_1],''),
			AddressLine2 = ISNULL([Address_Line_2],''),
			CityName = ISNULL([City_Name],''),
			CountryAbbreviation = ISNULL([Country_Abbreviation],''),
			StateAbbreviation  = ISNULL([State_Abbreviation],''),
			ZipCode = ISNULL([Zip_Code],''),
			CountyName = ISNULL([County_Name],''),
			CountryName = ISNULL([Country_Name],''),
			PhoneNumber1 = ISNULL((CAST([Phone_Number_1] As nvarchar(20))),''),
			PhoneExtension1 = ISNULL((CAST([Phone_Extension_1] As nchar(5))),''), 
			FaxNumber = ISNULL((CAST([Fax_Number] As nvarchar(20))),''),
			IsCallForAuctionSaleAuthorization = ISNULL([Call_for_Auction_Sale_Authorization_Ind],0),
			HomeBranchNumber = ISNULL([Home_Branch_Number],0),
			ProviderAdministrativeBranchNumber = ISNULL([Salvage_Provider_Administrative_Branch_Number],0),
			ProviderGroupName = ISNULL([Salvage_Provider_Group_Name],''),
			ProviderStatusCode = ISNULL([Salvage_Provider_Status_Code],''),
			ProviderTypeCode = ISNULL([Salvage_Provider_Type_Code],''),
			ProviderTypeDescription = ISNULL([Salvage_Provider_Type_Description],''),
			IsExpressPickupTow = ISNULL([Express_Pickup_Tow_Ind],0),
			IsAlternatePayeeAccess = ISNULL([Alternate_Payee_Access_Ind],0),
			IsPrepAdvCharge = ISNULL([Prep_Adv_Charge_Ind],0),
			VICEmail = ISNULL([VIC_Email],''),
			IsVICEmailAssignment = ISNULL([VIC_Email_Assignment_Ind],0),
			IsVICEmailLogin = ISNULL([VIC_Email_Login_Ind],0),
			IsShowLossCode = ISNULL([Show_Loss_Code_Ind],0),
			IsIBNParticipant = ISNULL([IBN_Participant_Ind],0),
			ETLLoadUpdateID = @ETLLoadID,
			IsEDIPartner = ISNULL([EDI_Partner_Ind],0), 
	        FederalTaxID = [Federal_Tax_ID], 
	        MinimumPrebillAmount = [Minimum_Prebill_Amount],
	        OptionName = ISNULL([Option_Name],''),
	        IsSalvageProviderQuestion = ISNULL([Salvage_Provider_Question_Ind],0),
	        IsRemarketingProvider = ISNULL([Remarketing_Provider_Ind],0), 
	        IsSettlementPacketExpress = ISNULL([Settlement_Packet_Express_Ind],0), 
	        IsAssignAllSalvagesToSpecialty = ISNULL([Assign_All_Salvages_To_Specialty_Ind],0),
			AccountManagerID = ISNULL([Account_Manager_ID],0),
			AccountManagerDescription = ISNULL([Account_Manager_Description],'None'),
			IsSecondOfferProgramParticipant =  ISNULL(TP.SecondOfferProgramParticipantInd,0),
			IsTitleDirectParticipant =  ISNULL(TP.IsTitleDirectParticipant,0),
			AccountManagerCode = ISNULL(Account_Manager_Code,'NONE'),
			IsLiveRepParticipant =  ISNULL(TP.IsLiveRepParticipant,0),
			IsTitleDirectNotificationsParticipant = ISNULL(TP.IsTitleDirectNotificationsParticipant,0),
			IsTitleProblemNotificationsParticipant = ISNULL(TP.IsTitleProblemNotificationsParticipant,0),
			IsTPA=ISNULL(TP.IsTPA,0)
	WHEN NOT MATCHED THEN
    INSERT( ProviderID,
				ProviderName,
				AddressLine1,
				AddressLine2,
				CityName,
				CountryAbbreviation,
				StateAbbreviation,
				ZipCode,
				CountyName,
				CountryName,
				PhoneNumber1,
				PhoneExtension1,
				FaxNumber,
				IsCallForAuctionSaleAuthorization,
				HomeBranchNumber,
				ProviderAdministrativeBranchNumber,
				ProviderGroupName,
				ProviderStatusCode,
				ProviderTypeCode,
				ProviderTypeDescription,
				IsExpressPickupTow,
				IsAlternatePayeeAccess,
				IsPrepAdvCharge,
				VICEmail,
				IsVICEmailAssignment,
				IsVICEmailLogin,
				IsShowLossCode,
				IsIBNParticipant,
				ETLLoadID,
				ETLLoadUpdateID,
				IsEDIPartner,
				FederalTaxID,
				MinimumPrebillAmount,
				OptionName,
				IsSalvageProviderQuestion,
				IsRemarketingProvider,
				IsSettlementPacketExpress,
				IsAssignAllSalvagesToSpecialty,
				AccountManagerID,
				AccountManagerDescription,
				IsSecondOfferProgramParticipant,
				IsTitleDirectParticipant,
				AccountManagerCode,
				IsLiveRepParticipant,
				IsTitleDirectNotificationsParticipant,
				IsTitleProblemNotificationsParticipant,
				IsTPA
			) 
	  VALUES ([Salvage_Provider_ID],
			ISNULL([Salvage_Provider_Name],''),
			ISNULL([Address_Line_1],''),
			ISNULL([Address_Line_2],''),
			ISNULL([City_Name],''),
			ISNULL([Country_Abbreviation],''),
			ISNULL([State_Abbreviation],''),
			ISNULL([Zip_Code],''),
			ISNULL([County_Name],''),
			ISNULL([Country_Name],''),
			ISNULL((CAST([Phone_Number_1] As nvarchar(20))),''),
			ISNULL((CAST([Phone_Extension_1] As nchar(5))),''),
			ISNULL((CAST([Fax_Number] As nvarchar(20))),''),
			ISNULL([Call_for_Auction_Sale_Authorization_Ind],0),
			ISNULL([Home_Branch_Number],0),
			ISNULL([Salvage_Provider_Administrative_Branch_Number],0),
			ISNULL([Salvage_Provider_Group_Name],''),
			ISNULL([Salvage_Provider_Status_Code],''),
			ISNULL([Salvage_Provider_Type_Code],''),
			ISNULL([Salvage_Provider_Type_Description],''),
			ISNULL([Express_Pickup_Tow_Ind],0),
			ISNULL([Alternate_Payee_Access_Ind],0),
			ISNULL([Prep_Adv_Charge_Ind],0),
			ISNULL([VIC_Email],''),
			ISNULL([VIC_Email_Assignment_Ind],0),
			ISNULL([VIC_Email_Login_Ind],0),
			ISNULL([Show_Loss_Code_Ind],0),
			ISNULL([IBN_Participant_Ind],0),
			@ETLLoadID,
			@ETLLoadID,
			ISNULL([EDI_Partner_Ind],0),
			[Federal_Tax_ID],
			[Minimum_Prebill_Amount],
			ISNULL([Option_Name],''),
			ISNULL([Salvage_Provider_Question_Ind],0),
			ISNULL([Remarketing_Provider_Ind],0), 
			ISNULL([Settlement_Packet_Express_Ind],0),
			ISNULL([Assign_All_Salvages_To_Specialty_Ind],0),
			ISNULL([Account_Manager_ID],0),
			ISNULL([Account_Manager_Description],'None'),
			ISNULL(TP.SecondOfferProgramParticipantInd,0),
			ISNULL(TP.IsTitleDirectParticipant,0),
			ISNULL(Account_Manager_Code,'NONE'),
			ISNULL(TP.IsLiveRepParticipant,0),
			ISNULL(TP.IsTitleDirectNotificationsParticipant,0),
			ISNULL(TP.IsTitleProblemNotificationsParticipant,0),
			ISNULL(TP.IsTPA,0)
			
			)
	     
	OUTPUT $action INTO #tmp_ProviderRecordCount;
	
/*********************************************************************************************
Checks for updates to the Salvage_Provider_Account_Manager table and makes the corresponding
change in DimProvider
*********************************************************************************************/
UPDATE dbo.DimProvider
SET	   AccountManagerDescription = ISNULL(Account_Manager_Description,'None'),
		AccountManagerCode = ISNULL([Code],'NONE')
FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider_Account_Manager(
   @StartLSN,@EndLSN,'all with merge')  spam
INNER JOIN dbo.DimProvider dp 
ON dp.AccountManagerID = spam.Salvage_Provider_Account_Manager_ID
WHERE [__$operation] = 5

/*********************************************************************************************
Updates Account Manager for Inside Sales.  
This can be removed if ASAP makes the appropriate fix.
*********************************************************************************************/
--UPDATE sp
--SET    AccountManagerID=10, AccountManagerDescription = 'INS'
----SELECT AccountManagerID,ASAPsp.Salvage_Provider_Group_ID,Claim_Number,Assignment_Type_Code,*
--FROM   dbo.DimProvider sp
--       INNER JOIN BI_ASAP_Rep.dbo.Salvage_Provider_Info spi
--         ON sp.ProviderID=spi.Salvage_Provider_ID
--       INNER JOIN BI_ASAP_Rep.dbo.Salvage_Provider ASAPsp
--         ON sp.ProviderID=ASAPsp.Salvage_Provider_ID
--       INNER JOIN BI_ASAP_Rep.dbo.Salvage s
--         ON s.Salvage_ID=spi.Salvage_ID
--WHERE  AccountManagerID<>10 AND ASAPsp.Salvage_Provider_Group_ID = 50
--   --AND ASAPsp.Salvage_Provider_Group_ID<>608
--   AND (Claim_Number IN ('CFC', 'SSP', 'OTR', 'CFC*',
--                         'SSP*', 'OTR*', 'RDD', 'AVP',
--                         'SAF', 'UIN')
--         OR Claim_Number LIKE '%GVP-%')
--   AND Assignment_Type_Code NOT IN ('TOW', 'TTL')   

/*********************************************************************************************
Updates PrepaidAdvanceChargeInd, GlobalMaximumAuthorizedAmount

*********************************************************************************************/
UPDATE DP
SET	  PrepaidAdvanceChargeInd = ISNULL(SP.Prep_Adv_Charge_Ind,0) ,
	  GlobalMaximumAuthorizedAmount = SP.Max_Authorized_Pay_Amount
FROM  dbo.DimProvider DP
	  INNER JOIN #TMP_Provider TP
         ON DP.ProviderID=TP.Salvage_Provider_ID
      INNER JOIN BI_ASAP_Rep.dbo.Salvage_Provider SP
         ON DP.ProviderID = SP.Salvage_Provider_ID
         

       
/*********************************************************************************************
	Set the indicator to 1 when the provider becomes Non-Standard
*********************************************************************************************/
UPDATE DP
SET    ISNONSTANDARDPROVIDER = 1
FROM   dbo.DimProvider AS DP
       INNER JOIN #TMP_NonStandardProvider AS CT WITH (NOLOCK)
       ON DP.ProviderId=CT.ProviderId
WHERE DP.IsNonStandardProvider = 0
/*********************************************************************************	
	Reset the isNonStandardProvider indicator back to 0 if the conditions have changed
		and the provider is no longer Non-Standard
*********************************************************************************/
UPDATE DP
SET    ISNONSTANDARDPROVIDER = 0
--select *
FROM   dbo.DimProvider AS DP
LEFT OUTER JOIN  #TMP_NonStandardProvider  NSP
	ON DP.ProviderID = NSP.ProviderID
WHERE  DP.IsNonStandardProvider = 1
	AND NSP.ProviderID IS NULL

/*********************************************************************************************
	Set the indicator to 1 when the provider becomes TPA
*********************************************************************************************/
UPDATE DP
SET    ISTPA = 1
FROM   dbo.DimProvider AS DP
       INNER JOIN #TMP_TPA AS CT WITH (NOLOCK)
       ON DP.ProviderId=CT.ProviderId
WHERE DP.IsTPA = 0
/*********************************************************************************	
	Reset the isTPA indicator back to 0 if the conditions have changed
		and the provider is no longer TPA
*********************************************************************************/
UPDATE DP
SET    ISTPA = 0
--select *
FROM   dbo.DimProvider AS DP
LEFT OUTER JOIN  #TMP_TPA  NSP
	ON DP.ProviderID = NSP.ProviderID
WHERE  DP.IsTPA = 1
	AND NSP.ProviderID IS NULL

/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #tmp_ProviderRecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		
		IF OBJECT_ID('tempdb..#TMP_Provider') IS NOT NULL DROP TABLE #TMP_Provider
		IF OBJECT_ID('tempdb..#tmp_ProviderRecordCount') IS NOT NULL DROP TABLE #tmp_ProviderRecordCount
		IF OBJECT_ID('tempdb..#TMP_Sndoffer') IS NOT NULL DROP TABLE #TMP_Sndoffer
		
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
		
		IF OBJECT_ID('tempdb..#TMP_Provider') IS NOT NULL DROP TABLE #TMP_Provider
		IF OBJECT_ID('tempdb..#tmp_ProviderRecordCount') IS NOT NULL DROP TABLE #tmp_ProviderRecordCount
		IF OBJECT_ID('tempdb..#TMP_Sndoffer') IS NOT NULL DROP TABLE #TMP_Sndoffer

		RETURN -1
	END CATCH
	END



GO


