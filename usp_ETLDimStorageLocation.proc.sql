/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 08/12/2011
Description	: Populate the EDW DimStorageLocation table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		  Release/Problem Number	
8/17/2011	Matt Stelter	  Changed to use CDC Net Change Function 
8/23/2011	Matt Stelter	  Added Country Name and Mail Country Name
2/8/2013	Matt Stelter	  Changed IsResidence to look for charges
7/22/2013   Pratyusha Koduru  Updated the Storage Location County name field based on a lookup of the Garage Zip Code
==============================================================================
Usage Example:
Exec usp_ETLDimStorageLocation
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimStorageLocation]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'uspDimStorageLocation'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table to consolidate the Current and Static Storage Locations
*********************************************************************************************/
IF OBJECT_ID('tempdb..#SL_Changes') IS NOT NULL DROP TABLE #SL_Changes
CREATE TABLE #SL_Changes(
	[Storage_Location_ID] [int] NULL,
	[Location_Name] [varchar](40) NULL,
	[Address_Line_1] [varchar](40) NULL,
	[Address_Line_2] [varchar](40) NULL,
	[City_Name] [varchar](40) NULL,
	[State_Abbreviation] [char](3) NULL,
	[Country_Abbreviation] [char](2) NULL,
	[Country_Name] [varchar](60) NULL,
	[Zip_Code] [varchar](9) NULL,
	[County_Name] [varchar](30) NULL,
	[Lattitude] [decimal](7, 5) NULL,
	[Longitude] [decimal](8, 5) NULL,
	[Phone_Number_1] [decimal](11, 0) NULL,
	[Phone_Extension_1] [decimal](5, 0) NULL,
	[Phone_Number_2] [decimal](11, 0) NULL,
	[Phone_Extension_2] [decimal](5, 0) NULL,
	[Fax_Number] [decimal](11, 0) NULL,
	[EMail_Address] [varchar](60) NULL,
	[Federal_Tax_ID] [varchar](9) NULL,
	[Contact_User_ID] [int] NULL,
	[Storage_Location_Type_Code] [char](5) NULL,
	[Create_DateTime] [datetime] NULL,
	[Status_Code] [char](3) NULL,
	[Accepts_IAA_Check_Ind] [bit] NULL,
	[Accepts_Tower_Check_Ind] [bit] NULL,
	[Cash_Accepted_Ind] [bit] NULL,
	[Credit_Card_Accepted_Ind] [bit] NULL,
	[Check_Payable_To] [varchar](40) NULL,
	[Residence_Ind] [bit] NULL,
	[Owners_Residence_Ind] [bit] NULL,
	[Cash_Only_Ind] [bit] NULL,
	[Bill_and_Release_Ind] [bit] NULL,
	[Mail_Address_Line_1] [varchar](40) NULL,
	[Mail_Address_Line_2] [varchar](40) NULL,
	[Mail_City_Name] [varchar](40) NULL,
	[Mail_State_Abbreviation] [char](3) NULL,
	[Mail_Zip_Code] [varchar](9) NULL,
	[Call_Before_Arrival_Ind] [bit] NULL,
	[Mail_Country_Abbreviation] [char](2) NULL,
	[Mail_Country_Name] [varchar](50) NULL,
	[Payment_Accepted_At_Location_Ind] [bit] NULL,
	[Parent_Storage_Location_ID] [int] NULL,
	[Storage_Location_Area_Code] [char](3) NULL,
	[Financial_Parent_Storage_Location_ID] [int] NULL,
	[Offsite_Sale_Ind] [bit] NULL,
	[Update_User_ID] [int] NULL,
	[Update_DateTime] [datetime] NULL,
	[Probability] [int] NULL,
	[Storage_Location_Address_ID] [int] NULL,
	[Address_Update_User_ID] [int] NULL,
	[Address_Update_DateTime] [datetime] NULL,
	[Validated_Ind] [int] NULL,
	[Updatable_Ind] [int] NULL,
	[Operation] [int] NULL,
	IsAddressValidated [int] NULL,
	IsAddressValidatedByUser [int] NULL,
	AddressValidatedDatetime [datetime] NULL,
	AddressValidatedUserID [int] NULL,
	IsCurrentValidated [int] NULL,
	CurrentValidatedDateTime [datetime] NULL,
	CurrentValidatedUserID [int] NULL,
	StorageLocationSourceCode [char](1) NULL)


CREATE INDEX IX_SL_Changes_Storage_Location_Address_ID
    ON #SL_Changes ( Storage_Location_Address_ID ) 
	with fillfactor = 100;

CREATE INDEX IX_SL_Changes_Storage_Location_ID
    ON #SL_Changes ( Storage_Location_ID ) 
	with fillfactor = 100;

IF OBJECT_ID('tempdb..#recordCount') IS NOT NULL DROP TABLE #recordCount
CREATE TABLE #recordCount (ChangeType VARCHAR(10)) 

CREATE TABLE #TMP_SP_Data(
	  [Salvage_ID] [int] NOT NULL
	, [Pickup_Location_ID] int )
	
/*********************************************************************************************
Insert the values from the CDC Current Storage Location table into the temp table
*********************************************************************************************/
INSERT INTO #SL_Changes(
	Storage_Location_ID,
	Location_Name,
	Address_Line_2,
	Phone_Number_1,
	Phone_Extension_1,
	Phone_Number_2,
	Phone_Extension_2,
	Fax_Number,
	EMail_Address,
	Federal_Tax_ID,
	Contact_User_ID,
	Storage_Location_Type_Code,
	Create_DateTime,
	Status_Code,
	Accepts_IAA_Check_Ind,
	Accepts_Tower_Check_Ind,
	Cash_Accepted_Ind,
	Credit_Card_Accepted_Ind,
	Check_Payable_To,
	Residence_Ind,
	Owners_Residence_Ind,
	Cash_Only_Ind,
	Bill_and_Release_Ind,
	Mail_Address_Line_1,
	Mail_Address_Line_2,
	Mail_City_Name,
	Mail_State_Abbreviation,
	Mail_Zip_Code,
	Call_Before_Arrival_Ind,
	Mail_Country_Abbreviation,
	Payment_Accepted_At_Location_Ind,
	Parent_Storage_Location_ID,
	Storage_Location_Area_Code,
	Financial_Parent_Storage_Location_ID,
	Offsite_Sale_Ind,
	Update_User_ID,
	Update_DateTime,
	Storage_Location_Address_ID,
	Validated_Ind,
	Updatable_Ind,
	Operation,
	IsCurrentValidated,
	CurrentValidatedDateTime,
	CurrentValidatedUserID,
	StorageLocationSourceCode)
SELECT Storage_Location_ID,
	Location_Name,
	Address_Line_2,
	Phone_Number_1,
	Phone_Extension_1,
	Phone_Number_2,
	Phone_Extension_2,
	Fax_Number,
	EMail_Address,
	Federal_Tax_ID,
	Contact_User_ID,
	Storage_Location_Type_Code,
	Create_DateTime,
	Status_Code,
	Accepts_IAA_Check_Ind,
	Accepts_Tower_Check_Ind,
	Cash_Accepted_Ind,
	Credit_Card_Accepted_Ind,
	Check_Payable_To,
	Residence_Ind,
	Owners_Residence_Ind,
	Cash_Only_Ind,
	Bill_and_Release_Ind,
	Mail_Address_Line_1,
	Mail_Address_Line_2,
	Mail_City_Name,
	Mail_State_Abbreviation,
	Mail_Zip_Code,
	Call_Before_Arrival_Ind,
	Mail_Country_Abbreviation,
	Payment_Accepted_At_Location_Ind,
	Parent_Storage_Location_ID,
	Storage_Location_Area_Code,
	Financial_Parent_Storage_Location_ID,
	Offsite_Sale_Ind,
	Update_User_ID,
	Update_DateTime,
	Storage_Location_Address_ID,
	Validated_Ind,
	Updatable_Ind,
	__$operation,
	Validated_Ind,
	 Validated_DateTime,
	 Validated_User_ID,
	 'C' AS StorageLocationSourceCode
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Storage_Location_Current] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 

--SELECT * FROM #SL_Changes order by 1

/*********************************************************************************************
Insert the values from the CDC Static Storage Location table into the temp table
*********************************************************************************************/
INSERT INTO #SL_Changes(
	Storage_Location_ID,
	Location_Name,
	Address_Line_1,
	Address_Line_2,
	City_Name,
	State_Abbreviation,
	Country_Abbreviation,
	Zip_Code,
	County_Name,
	Lattitude,
	Longitude,
	Phone_Number_1,
	Phone_Extension_1,
	Phone_Number_2,
	Phone_Extension_2,
	Fax_Number,
	EMail_Address,
	Federal_Tax_ID,
	Contact_User_ID,
	Storage_Location_Type_Code,
	Create_DateTime,
	Status_Code,
	Accepts_IAA_Check_Ind,
	Accepts_Tower_Check_Ind,
	Cash_Accepted_Ind,
	Credit_Card_Accepted_Ind,
	Check_Payable_To,
	Residence_Ind,
	Owners_Residence_Ind,
	Cash_Only_Ind,
	Bill_and_Release_Ind,
	Mail_Address_Line_1,
	Mail_Address_Line_2,
	Mail_City_Name,
	Mail_State_Abbreviation,
	Mail_Zip_Code,
	Call_Before_Arrival_Ind,
	Mail_Country_Abbreviation,
	Payment_Accepted_At_Location_Ind,
	Parent_Storage_Location_ID,
	Storage_Location_Area_Code,
	Financial_Parent_Storage_Location_ID,
	Offsite_Sale_Ind,
	Update_User_ID,
	Update_DateTime,
	Probability,
	Storage_Location_Address_ID,
	Operation,
	StorageLocationSourceCode)
SELECT Storage_Location_ID,
	Location_Name,
	Address_Line_1,
	Address_Line_2,
	City_Name,
	State_Abbreviation,
	Country_Abbreviation,
	Zip_Code,
	County_Name,
	Lattitude,
	Longitude,
	Phone_Number_1,
	Phone_Extension_1,
	Phone_Number_2,
	Phone_Extension_2,
	Fax_Number,
	EMail_Address,
	Federal_Tax_ID,
	Contact_User_ID,
	Storage_Location_Type_Code,
	Create_DateTime,
	Status_Code,
	Accepts_IAA_Check_Ind,
	Accepts_Tower_Check_Ind,
	Cash_Accepted_Ind,
	Credit_Card_Accepted_Ind,
	Check_Payable_To,
	Residence_Ind,
	Owners_Residence_Ind,
	Cash_Only_Ind,
	Bill_and_Release_Ind,
	Mail_Address_Line_1,
	Mail_Address_Line_2,
	Mail_City_Name,
	Mail_State_Abbreviation,
	Mail_Zip_Code,
	Call_Before_Arrival_Ind,
	Mail_Country_Abbreviation,
	Payment_Accepted_At_Location_Ind,
	Parent_Storage_Location_ID,
	Storage_Location_Area_Code,
	Financial_Parent_Storage_Location_ID,
	0 AS Offsite_Sale_Ind,
	Update_User_ID,
	Update_DateTime,
	Probability,
	Storage_Location_Address_ID,
	[__$operation],
	 'S' AS StorageLocationSourceCode
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Storage_Location_Static] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 

/*********************************************************************************************
Update all the Storage Location Address information in the temp table
*********************************************************************************************/
UPDATE #SL_Changes SET 
	Address_Line_1=SLA.Address_Line_1,
	City_Name=SLA.City_Name,
	State_Abbreviation=SLA.State_Abbreviation,
	Country_Abbreviation=SLA.Country_Abbreviation,
	Zip_Code=SLA.Zip_Code,
	--County_Name=SLA.County_Name,
	Lattitude=SLA.Lattitude,
	Longitude=SLA.Longitude,
	Probability=SLA.Probability,
	Address_Update_User_ID=SLA.Update_User_ID,
	Address_Update_DateTime=SLA.Update_DateTime,
	IsAddressValidated=SLA.Validated_Ind,
	IsAddressValidatedByUser=Validated_By_User_Ind,
	AddressValidatedDatetime=Validated_Datetime,
	AddressValidatedUserID=Validated_User_ID,
	Country_Name=C.Country_Name,
	Mail_Country_Name=MC.Country_Name,
    County_Name = COALESCE(SLA.County_Name,ISNULL(LKZC.County,'')) 
FROM #SL_Changes SLChanges WITH (NOLOCK)
INNER JOIN [BI_ASAP_REP].dbo.Storage_Location_Address SLA WITH (NOLOCK)
ON SLChanges.Storage_Location_Address_ID = SLA.Storage_Location_Address_ID
LEFT OUTER JOIN [BI_ASAP_REP].dbo.Country C WITH (NOLOCK)
ON SLA.Country_Abbreviation = C.Country_Abbreviation
LEFT OUTER JOIN [BI_ASAP_REP].dbo.Country MC WITH (NOLOCK)
ON SLChanges.Mail_Country_Abbreviation = MC.Country_Abbreviation
LEFT OUTER JOIN EDW.dbo.LKZipCode LKZC
ON SLA.Zip_Code = LKZC.FullZipCode

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimStorageLocation table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION    


MERGE [dbo].[DimStorageLocation] AS DST
USING #SL_Changes AS SRC
         ON SRC.Storage_Location_ID = DST.StorageLocationID
WHEN MATCHED THEN
UPDATE SET 
	[IsAcceptsIAACheck]=ISNULL(SRC.[Accepts_IAA_Check_Ind],0),
	[IsAcceptsTowerCheck]=ISNULL(SRC.[Accepts_Tower_Check_Ind],0),
	[AddressLine1]=SRC.[Address_Line_1],
	[AddressLine2]=SRC.[Address_Line_2],
	[AddressUpdateDateTime]=SRC.[Address_Update_DateTime],
	[AddressUpdateUserID]=SRC.[Address_Update_User_ID],
	[IsBillandRelease]=ISNULL(SRC.[Bill_and_Release_Ind],0),
	[IsCallBeforeArrival]=ISNULL(SRC.[Call_Before_Arrival_Ind],0),
	[IsCashAccepted]=ISNULL(SRC.[Cash_Accepted_Ind],0),
	[IsCashOnly]=ISNULL(SRC.[Cash_Only_Ind],0),
	[CheckPayableTo]=SRC.[Check_Payable_To],
	[CityName]=SRC.[City_Name],
	[ContactUserID]=SRC.[Contact_User_ID],
	[CountryAbbreviation]=SRC.[Country_Abbreviation],
	[CountyName]=SRC.[County_Name],
	[CreateDateTime]=SRC.[Create_DateTime],
	[IsCreditCardAccepted]=ISNULL(SRC.[Credit_Card_Accepted_Ind],0),
	[EmailAddress]=SRC.[EMail_Address],
	[FaxNumber]=SRC.[Fax_Number],
	[FederalTaxID]=SRC.[Federal_Tax_ID],
	[FinancialParentStorageLocationID]=ISNULL(SRC.[Financial_Parent_Storage_Location_ID],1000000),
	[Lattitude]=SRC.[Lattitude],
	[LocationName]=SRC.[Location_Name],
	[Longitude]=SRC.[Longitude],
	[MailAddressLine1]=SRC.[Mail_Address_Line_1],
	[MailAddressLine2]=SRC.[Mail_Address_Line_2],
	[MailCityName]=SRC.[Mail_City_Name],
	[MailCountryAbbreviation]=SRC.[Mail_Country_Abbreviation],
	[MailStateAbbreviation]=SRC.[Mail_State_Abbreviation],
	[MailZipCode]=SRC.[Mail_Zip_Code],
	[IsOffSiteSale]=ISNULL(SRC.[Offsite_Sale_Ind],0),
	[IsOwnersResidence]=ISNULL(SRC.[Owners_Residence_Ind],0),
	[ParentStorageLocationID]=SRC.[Parent_Storage_Location_ID],
	[IsPaymentAcceptedAtLocation]=ISNULL(SRC.[Payment_Accepted_At_Location_Ind],0),
	[PhoneExtension1]=SRC.[Phone_Extension_1],
	[PhoneExtension2]=SRC.[Phone_Extension_2],
	[PhoneNumber1]=SRC.[Phone_Number_1],
	[PhoneNumber2]=SRC.[Phone_Number_2],
	[Probability]=SRC.[Probability],
	[IsResidence]=ISNULL(SRC.[Residence_Ind],0),
	[StateAbbreviation]=SRC.[State_Abbreviation],
	[StorageLocationStatusCode]=SRC.[Status_Code],
	[StorageLocationAreaCode]=SRC.[Storage_Location_Area_Code],
	[StorageLocationID]=SRC.[Storage_Location_ID],
	[StorageLocationTypeCode]=SRC.[Storage_Location_Type_Code],
	[IsUpdateable]=ISNULL(SRC.[Updatable_Ind],0),
	[UpdateDateTime]=SRC.[Update_DateTime],
	[UpdateUserID]=SRC.[Update_User_ID],
	[IsComputedValidated]=CASE WHEN ISNULL(SRC.[IsCurrentValidated],0) = 1 and ISNULL(SRC.[IsAddressValidated],0) = 1 THEN 1 ELSE 0 END,
	[ZipCode]=SRC.[Zip_Code],
	--[ETLLoadID]=0,
	[ETLLoadUpdateID]=@ETLLoadID,
	[IsAddressValidated]=ISNULL(SRC.[IsAddressValidated],0),
	[IsAddressValidatedByUser]=ISNULL(SRC.[IsAddressValidatedByUser],0),
	[AddressValidatedDatetime]=SRC.[AddressValidatedDatetime],
	[AddressValidatedUserID]=SRC.[AddressValidatedUserID],
	[IsCurrentValidated]=ISNULL(SRC.[IsCurrentValidated],0),
	[CurrentValidatedDateTime]=SRC.[CurrentValidatedDateTime],
	[CurrentValidatedUserID]=SRC.[CurrentValidatedUserID],
	[StorageLocationSourceCode]=SRC.[StorageLocationSourceCode],
	[StorageLocationAddressID]=SRC.[Storage_Location_Address_ID],
	[CountryName] = SRC.Country_Name,
	[MailCountryName] = SRC.Mail_Country_Name
WHEN NOT MATCHED THEN
INSERT (IsAcceptsIAACheck,
		IsAcceptsTowerCheck,
		AddressLine1,
		AddressLine2,
		AddressUpdateDateTime,
		AddressUpdateUserID,
		IsBillandRelease,
		IsCallBeforeArrival,
		IsCashAccepted,
		IsCashOnly,
		CheckPayableTo,
		CityName,
		ContactUserID,
		CountryAbbreviation,
		CountyName,
		CreateDateTime,
		IsCreditCardAccepted,
		EmailAddress,
		FaxNumber,
		FederalTaxID,
		FinancialParentStorageLocationID,
		Lattitude,
		LocationName,
		Longitude,
		MailAddressLine1,
		MailAddressLine2,
		MailCityName,
		MailCountryAbbreviation,
		MailStateAbbreviation,
		MailZipCode,
		IsOffSiteSale,
		IsOwnersResidence,
		ParentStorageLocationID,
		IsPaymentAcceptedAtLocation,
		PhoneExtension1,
		PhoneExtension2,
		PhoneNumber1,
		PhoneNumber2,
		Probability,
		IsResidence,
		StateAbbreviation,
		StorageLocationStatusCode,
		StorageLocationAreaCode,
		StorageLocationID,
		StorageLocationTypeCode,
		IsUpdateable,
		UpdateDateTime,
		UpdateUserID,
		IsComputedValidated,
		ZipCode,
		ETLLoadID,
		ETLLoadUpdateID,
		IsAddressValidated,
		IsAddressValidatedByUser,
		AddressValidatedDatetime,
		AddressValidatedUserID,
		IsCurrentValidated,
		CurrentValidatedDateTime,
		CurrentValidatedUserID,
		StorageLocationSourceCode,
		StorageLocationAddressID,
		CountryName, 
		MailCountryName)
      VALUES (ISNULL(SRC.[Accepts_IAA_Check_Ind],0),
		ISNULL(SRC.[Accepts_Tower_Check_Ind],0),
		SRC.[Address_Line_1],
		SRC.[Address_Line_2],
		SRC.[Address_Update_DateTime],
		SRC.[Address_Update_User_ID],
		ISNULL(SRC.[Bill_and_Release_Ind],0),
		ISNULL(SRC.[Call_Before_Arrival_Ind],0),
		ISNULL(SRC.[Cash_Accepted_Ind],0),
		ISNULL(SRC.[Cash_Only_Ind],0),
		SRC.[Check_Payable_To],
		SRC.[City_Name],
		SRC.[Contact_User_ID],
		SRC.[Country_Abbreviation],
		SRC.[County_Name],
		SRC.[Create_DateTime],
		ISNULL(SRC.[Credit_Card_Accepted_Ind],0),
		SRC.[EMail_Address],
		SRC.[Fax_Number],
		SRC.[Federal_Tax_ID],
		ISNULL(SRC.[Financial_Parent_Storage_Location_ID],1000000),
		SRC.[Lattitude],
		SRC.[Location_Name],
		SRC.[Longitude],
		SRC.[Mail_Address_Line_1],
		SRC.[Mail_Address_Line_2],
		SRC.[Mail_City_Name],
		SRC.[Mail_Country_Abbreviation],
		SRC.[Mail_State_Abbreviation],
		SRC.[Mail_Zip_Code],
		SRC.[Offsite_Sale_Ind],
		SRC.[Owners_Residence_Ind],
		SRC.[Parent_Storage_Location_ID],
		ISNULL(SRC.[Payment_Accepted_At_Location_Ind],0),
		SRC.[Phone_Extension_1],
		SRC.[Phone_Extension_2],
		SRC.[Phone_Number_1],
		SRC.[Phone_Number_2],
		SRC.[Probability],
		ISNULL(SRC.[Residence_Ind],0),
		SRC.[State_Abbreviation],
		SRC.[Status_Code],
		SRC.[Storage_Location_Area_Code],
		SRC.[Storage_Location_ID],
		SRC.[Storage_Location_Type_Code],
		ISNULL(SRC.[Updatable_Ind],0),
		SRC.[Update_DateTime],
		SRC.[Update_User_ID],
		CASE WHEN ISNULL(SRC.[IsCurrentValidated],0) = 1 and ISNULL(SRC.[IsAddressValidated],0) = 1 THEN 1 ELSE 0 END,
		SRC.[Zip_Code],
		@ETLLoadID,
		@ETLLoadID,
		ISNULL(SRC.[IsAddressValidated],0),
		ISNULL(SRC.[IsAddressValidatedByUser],0),
		SRC.[AddressValidatedDatetime],
		SRC.[AddressValidatedUserID],
		ISNULL(SRC.[IsCurrentValidated],0),
		SRC.[CurrentValidatedDateTime],
		SRC.[CurrentValidatedUserID],
		SRC.[StorageLocationSourceCode],
		SRC.[Storage_Location_Address_ID],
		SRC.[Country_Name], 
		SRC.[Mail_Country_Name])
   OUTPUT $action INTO #recordCount;

/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #recordCount

/*********************************************************************************************
Update changed storage location addressed not update above
*********************************************************************************************/
UPDATE [dbo].[DimStorageLocation] SET 
	AddressLine1=SLA_CT.Address_Line_1,
	CityName=SLA_CT.City_Name,
	StateAbbreviation=SLA_CT.State_Abbreviation,
	CountryAbbreviation=SLA_CT.Country_Abbreviation,
	ZipCode=SLA_CT.Zip_Code,
	--CountyName=SLA_CT.County_Name,
	Lattitude=SLA_CT.Lattitude,
	Longitude=SLA_CT.Longitude,
	Probability=SLA_CT.Probability,
	AddressUpdateUserID=SLA_CT.Update_User_ID,
	AddressUpdateDateTime=SLA_CT.Update_DateTime,
	IsAddressValidated=ISNULL(SLA_CT.Validated_Ind,0),
	IsAddressValidatedByUser=ISNULL(Validated_By_User_Ind,0),
	AddressValidatedDatetime=Validated_Datetime,
	AddressValidatedUserID=Validated_User_ID,
	[ETLLoadUpdateID]=@ETLLoadID,
	CountryName=C.Country_Name,
   	CountyName= COALESCE(SLA_CT.County_Name,ISNULL(LKZC.County,'')) 
FROM [dbo].[DimStorageLocation] SL WITH (NOLOCK) INNER JOIN 
(SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Storage_Location_Address] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) SLA_CT 
ON SL.StorageLocationAddressID = SLA_CT.Storage_Location_Address_ID
LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Country C WITH (NOLOCK)
ON SLA_CT.Country_Abbreviation = C.Country_Abbreviation
LEFT OUTER JOIN EDW.dbo.LKZipCode LKZC
ON SLA_CT.Zip_Code = LKZC.FullZipCode
WHERE SL.StorageLocationID NOT IN (SELECT Storage_Location_ID FROM #SL_Changes);

--UPDATE [dbo].[DimStorageLocation] SET IsResidence = 1,StorageLocationTypeCode  = 'RESID'
--WHERE (StorageLocationTypeCode = 'RESID' or IsResidence = 1 OR LocationName like '%residence%')
--AND (ISNULL(StorageLocationTypeCode,'') <> 'RESID' or IsResidence <> 1)

UPDATE [dbo].[DimStorageLocation]
SET    IsResidence=CASE
                     WHEN StorageLocationTypeCode='RESID'
                           OR (LocationName LIKE '%residence%'
                                OR LocationName LIKE '%resident%'
                                OR LocationName LIKE '% res %'
                                OR LocationName LIKE '%home%'
                                OR LocationName LIKE '% house %'
                                OR LocationName LIKE '%Resid%') THEN 1
                     ELSE 0
                   END
FROM [dbo].[DimStorageLocation] DSL
INNER JOIN #SL_Changes AS SRC
         ON SRC.Storage_Location_ID = DSL.StorageLocationID                   
WHERE  IsResidence<>CASE
                      WHEN StorageLocationTypeCode='RESID'
                            OR (LocationName LIKE '%residence%'
                                 OR LocationName LIKE '%resident%'
                                 OR LocationName LIKE '% res %'
                                 OR LocationName LIKE '%home%'
                                 OR LocationName LIKE '% house %'
                                 OR LocationName LIKE '%Resid%') THEN 1
                      ELSE 0
                    END 

--GET Pickup location for stocks from Charges/expenses

INSERT INTO #TMP_SP_Data
            (Salvage_ID,
             Pickup_Location_ID)
SELECT [Salvage_ID],
       [Storage_Location_ID]
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_pickup](@StartLSN, @EndLSN, 'all with merge')
WHERE  [__$operation]=5

UPDATE [dbo].[DimStorageLocation]
SET    IsResidence=0
FROM   [dbo].[DimStorageLocation] DSL
       INNER JOIN (SELECT DISTINCT DS.PickupLocationID
                   FROM   DimStock DS WITH (NOLOCK)
                          INNER JOIN #TMP_SP_Data SPD WITH (NOLOCK)
                            ON DS.ASAPSalvageID=SPD.Salvage_ID
                          INNER JOIN dbo.FactFinancialTransaction ft WITH (NOLOCK)
                            ON DS.StockID=ft.StockID
                   WHERE  Isnull(COALESCE(ft.[OverrideAmount], ft.[Amount]), 0)>0
                      AND ft.FinancialTransactionTypeID=289
                      AND ft.IsVoid=0) AS SRC
         ON SRC.PickupLocationID=DSL.StorageLocationID 

--Add the updated addresses to the updated count
SET @UpdateRowCount = @UpdateRowCount + ISNULL(@@ROWCOUNT,0)

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
 
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

		RETURN -1
	END CATCH
	END

GO


