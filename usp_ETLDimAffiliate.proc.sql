
/* 
USE EDW
GO 
==============================================================================
Author		: Nandini Manay
Create date	: 08/11/2011
Description	: Populate the EDW DimAffiliate table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
08/19/2011 Nandini Manay    Added CountryName and MailCountryName logic
10/06/2011 Preetham Duvva   Updated for Nulls.
10/18/2011 Matt Stelter		Added insert for ETLLoadUpdateID
10/28/2011	Juan X. Jacome	Added code to drop temp Tables (DBA Code Review)
==============================================================================
Usage Example:
Exec usp_ETLDimAffiliate
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimAffiliate]
AS     
BEGIN

DECLARE @ETLLoadID        INT,
		@ExtractRowCount  INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
SET NOCOUNT ON;

	BEGIN TRY
            
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimAffiliate'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table to consolidate the Current and Static Storage Locations
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_Affiliate') IS NOT NULL DROP TABLE #TMP_Affiliate
CREATE TABLE #TMP_Affiliate
(
      [Affiliate_ID] [int] NULL,
      [Affiliate_Name] [varchar](40) NULL,
      [Address_Line_1] [varchar](40) NULL,
      [Address_Line_2] [varchar](40) NULL,
      [City_Name] [varchar](40) NULL,
      [State_Abbreviation] [char](3) NULL,
      [Country_Abbreviation] [char](2) NULL,
	  [Country_Name] [varchar](60) NULL,
      [Zip_Code] [varchar](9) NULL,
      [County_Name] [varchar](30) NULL,
      [Mail_Address_Line_1] [varchar](40) NULL,
      [Mail_Address_Line_2] [varchar](40) NULL,
      [Mail_City_Name] [varchar](40) NULL,
      [Mail_County_Name] [varchar](30) NULL,
      [Mail_State_Abbreviation] [char](3) NULL,
      [Mail_Zip_Code] [varchar](9) NULL,
      [Mail_Country_Abbreviation] [char](2) NULL,
	  [Mail_Country_Name] [varchar](60) NULL,
      [Phone_Number_1] [decimal](11, 0) NULL,
      [Phone_Extension_1] [decimal](5, 0) NULL,
      [Phone_Number_2] [decimal](11, 0) NULL,
      [Phone_Extension_2] [decimal](5, 0) NULL,
      [Fax_Number] [decimal](11, 0) NULL,
      [Web_Home_Page_Address] [varchar](60) NULL,
      [Federal_Tax_ID] [varchar](9) NULL,
      [Title_Processor_Type_Code] [char](2) NULL,
      [EMail_Address] [varchar](60) NULL,
      [Note_Text] [varchar](1024) NULL,
      [Update_User_ID] [int] NULL,
      [Update_DateTime] [datetime] NULL,
      [Storage_Location_ID] [int] NULL,
      [Status_Code] [char](3) NULL,
      [National_Use_Ind] [bit] NULL,
      [Specialty_Use_Ind] [bit] NULL,
      [Affiliate_Number] [int] NULL,
      [Process_Settlement_Days] [int] NULL,
      [Detroit_Use_Ind] [bit] NULL,
      [Check_Payable_To] [varchar](80) NULL,
      [Process_Title_Days] [smallint] NULL,
      [Tracking_Report_Sent_DateTime] [datetime] NULL,
      [Tracking_Report_Frequency] [int] NULL,
      [Time_Zone] [varchar](3) NULL
)


INSERT INTO #TMP_Affiliate
SELECT 
      [Affiliate_ID]
      ,[Affiliate_Name]
      ,[Address_Line_1]
      ,[Address_Line_2]
      ,[City_Name]
      ,[State_Abbreviation]
      ,[Country_Abbreviation]
	 ,''
      ,[Zip_Code]
      ,[County_Name]
      ,[Mail_Address_Line_1]
      ,[Mail_Address_Line_2]
      ,[Mail_City_Name]
      ,[Mail_County_Name]
      ,[Mail_State_Abbreviation]
      ,[Mail_Zip_Code]
      ,[Mail_Country_Abbreviation]
	,''
      ,[Phone_Number_1]
      ,[Phone_Extension_1]
      ,[Phone_Number_2]
      ,[Phone_Extension_2]
      ,[Fax_Number]
      ,[Web_Home_Page_Address]
      ,[Federal_Tax_ID]
      ,[Title_Processor_Type_Code]
      ,[EMail_Address]
      ,[Note_Text]
      ,[Update_User_ID]
      ,[Update_DateTime]
      ,[Storage_Location_ID]
      ,[Status_Code]
      ,[National_Use_Ind]
      ,[Specialty_Use_Ind]
      ,[Affiliate_Number]
      ,[Process_Settlement_Days]
      ,[Detroit_Use_Ind]
      ,[Check_Payable_To]
      ,[Process_Title_Days]
      ,[Tracking_Report_Sent_DateTime]
      ,[Tracking_Report_Frequency]
      ,[Time_Zone]
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Affiliate] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 


UPDATE #TMP_Affiliate
SET  [Country_Name] = Country.[Country_Name]
	,[Mail_Country_Name] = Country1.[Country_Name]
	--,[ETLLoadUpdateID]=@ETLLoadID
FROM #TMP_Affiliate AffiliateChanges WITH (NOLOCK)
INNER JOIN [BI_ASAP_REP].dbo.Country Country WITH (NOLOCK)
ON AffiliateChanges.Country_Abbreviation = Country.Country_Abbreviation
INNER JOIN [BI_ASAP_REP].dbo.Country Country1 WITH (NOLOCK)
ON AffiliateChanges.Mail_Country_Abbreviation = Country1.Country_Abbreviation


SELECT @ExtractRowCount = COUNT(*) FROM #TMP_Affiliate

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimUser table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

IF OBJECT_ID('tempdb..#temp_AffiliateRecordCount') IS NOT NULL DROP TABLE #temp_AffiliateRecordCount
CREATE TABLE #temp_AffiliateRecordCount(ChangeType VARCHAR(10)) 

BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.DimAffiliate AS t
	USING #TMP_Affiliate AS s    
		ON t.AffiliateID = s.Affiliate_ID

WHEN MATCHED  THEN
      --Row exists and data is different
	UPDATE SET 
			 t.AffiliateName=ISNULL(s.[Affiliate_Name],'')
			,t.AddressLine1 = ISNULL(s.[Address_Line_1],'')
			,t.AddressLine2=ISNULL(s.[Address_Line_2],'')
			,t.CityName=ISNULL(s.[City_Name],'')
			,t.StateAbbreviation=ISNULL(s.[State_Abbreviation],'')
			,t.ZipCode=ISNULL(s.[Zip_Code],'')
			,t.CountryAbbreviation=ISNULL(s.[Country_Abbreviation],'')
			,t.CountyName=ISNULL(s.[County_Name],'')
			,t.CountryName=ISNULL(s.[Country_Name],'')
			,t.MailAddressLine1=ISNULL(s.[Mail_Address_Line_1],'')
			,t.MailAddressLine2=ISNULL(s.[Mail_Address_Line_2],'')
			,t.MailCityName=ISNULL(s.[Mail_City_Name],'')
			,t.MailStateAbbreviation=ISNULL(s.[Mail_State_Abbreviation],'')
			,t.MailZipCode=ISNULL(s.[Mail_Zip_Code],'')
			,t.MailCountryAbbreviation=ISNULL(s.[Mail_Country_Abbreviation],'')
			,t.MailCountyName=ISNULL(s.[Mail_County_Name],'')
			,t.MailCountryName=ISNULL(s.[Country_Name],'')
			,t.PhoneNumber1=ISNULL(CAST (s.[Phone_Number_1] AS nvarchar(20)),'')
			,t.PhoneExtension1=ISNULL(CAST(s.[Phone_Extension_1] AS nchar(5)),'')
			,t.PhoneNumber2=ISNULL(CAST(s.[Phone_Number_2] AS nvarchar(20)),'')
			,t.PhoneExtension2=ISNULL(CAST(s.[Phone_Extension_2] AS nchar(5)),'')
			,t.FaxNumber=ISNULL(CAST(s.[Fax_Number] AS nvarchar(20)),'')
			,t.WebHomePageAddress=ISNULL(s.[Web_Home_Page_Address],'')
			,t.FederalTaxID=ISNULL(s.[Federal_Tax_ID],'')
			,t.TitleProcessorTypeCode=ISNULL(s.[Title_Processor_Type_Code],'')
			,t.EMailAddress=ISNULL(s.[EMail_Address],'')
			,t.NoteText=ISNULL(s.[Note_Text],'')
			,t.StorageLocationID=ISNULL(s.[Storage_Location_ID],0)
			,t.StatusCode=ISNULL(s.[Status_Code],'')
			,t.IsNationalUse=ISNULL(s.[National_Use_Ind],0)
			,t.IsSpecialtyUse=ISNULL(s.[Specialty_Use_Ind],0)
			,t.AffiliateNumber=ISNULL(s.[Affiliate_Number],0)
			,t.ProcessSettlementDays=ISNULL(s.[Process_Settlement_Days],0)
			,t.IsDetroitUse=ISNULL(s.[Detroit_Use_Ind],0)
			,t.CheckPayableTo=ISNULL(s.[Check_Payable_To],'')
			,t.ProcessTitleDays=ISNULL(s.[Process_Title_Days],0)
			,t.TrackingReportSentDateTime=ISNULL(s.[Tracking_Report_Sent_DateTime],'2999-12-31 00:00:00.000')
			,t.TrackingReportFrequency=ISNULL(s.[Tracking_Report_Frequency],0)
			,t.[ETLLoadUpdateID] = @ETLLoadID

  
	WHEN NOT MATCHED  THEN 
	--Row exists in source but not in target
	INSERT ([AffiliateID]
			,[AffiliateName]
			,[AddressLine1]
			,[AddressLine2]
			,[CityName]
			,[StateAbbreviation]
			,[ZipCode]
			,[CountryAbbreviation]
			,[CountyName]
			,[CountryName]
			,[MailAddressLine1]
			,[MailAddressLine2]	
			,[MailCityName]
			,[MailStateAbbreviation]
			,[MailZipCode]
			,[MailCountryAbbreviation]
			,[MailCountyName]
			,[MailCountryName]
			,[PhoneNumber1]
			,[PhoneExtension1]
			,[PhoneNumber2]
			,[PhoneExtension2]
			,[FaxNumber]
			,[WebHomePageAddress]
			,[FederalTaxID]
			,[TitleProcessorTypeCode]
			,[EMailAddress]
			,[NoteText]
			,[StorageLocationID]
			,[StatusCode]
			,[IsNationalUse]
			,[IsSpecialtyUse]
			,[AffiliateNumber]
			,[ProcessSettlementDays]
			,[IsDetroitUse]
			,[CheckPayableTo]
			,[ProcessTitleDays]
			,[TrackingReportSentDateTime]
			,[TrackingReportFrequency]
			,[ETLLoadID],[ETLLoadUpdateID])
	VALUES (
			 s.[Affiliate_ID]
			,ISNULL(s.[Affiliate_Name],'')
			,ISNULL(s.[Address_Line_1],'')
			,ISNULL(s.[Address_Line_2],'')
			,ISNULL(s.[City_Name],'')
			,ISNULL(s.[State_Abbreviation],'')
			,ISNULL(s.[Zip_Code] ,'')
			,ISNULL(s.[Country_Abbreviation],'')    
			,ISNULL(s.[County_Name],'')
			,ISNULL(s.[Country_Abbreviation],'')
			,ISNULL(s.[Country_Name],'')
			,ISNULL(s.[Mail_Address_Line_2],'')
			,ISNULL(s.[Mail_City_Name],'')
			,ISNULL(s.[Mail_State_Abbreviation],'')
			,ISNULL(s.[Mail_Zip_Code],'')
			,ISNULL(s.[Mail_Country_Abbreviation],'')
			,ISNULL(s.[Mail_County_Name],'')
			,ISNULL(s.[Country_Name],'')
			,ISNULL(CAST (s.[Phone_Number_1] AS nvarchar(20)),'')
			,ISNULL(CAST(s.[Phone_Extension_1] AS nchar(5)),'')
			,ISNULL(CAST(s.[Phone_Number_2] AS nvarchar(20)),'')
			,ISNULL(CAST(s.[Phone_Extension_2] AS nchar(5)),'')
			,ISNULL(CAST(s.[Fax_Number] AS nvarchar(20)),'')
			,ISNULL(s.[Web_Home_Page_Address],'')
			,ISNULL(s.[Federal_Tax_ID],'')
			,ISNULL(s.[Title_Processor_Type_Code],'')
			,ISNULL(s.[EMail_Address],'')
			,ISNULL(s.[Note_Text],'')
			,ISNULL(s.[Storage_Location_ID],0)
			,ISNULL(s.[Status_Code],'')
			,ISNULL(s.[National_Use_Ind],0)
			,ISNULL(s.[Specialty_Use_Ind],0)
			,ISNULL(s.[Affiliate_Number],0)
			,ISNULL(s.[Process_Settlement_Days],0)
			,ISNULL(s.[Detroit_Use_Ind],0)
			,ISNULL(s.[Check_Payable_To],'')
			,ISNULL(s.[Process_Title_Days],0)
			,ISNULL(s.[Tracking_Report_Sent_DateTime],'2999-12-31 00:00:00.000')
			,s.[Tracking_Report_Frequency]
			,@ETLLoadID,@ETLLoadID)
  
OUTPUT $action INTO #temp_AffiliateRecordCount;
COMMIT TRAN

SELECT @InsertRowCount = COUNT(*)  FROM #temp_AffiliateRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #temp_AffiliateRecordCount WHERE ChangeType = 'UPDATE'

--
--IF(Select COUNT(Country_Name) FROM
--(
--SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Country] (
--  @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5
--)a) > 0  
--BEGIN TRANSACTION  
--
--BEGIN
--UPDATE [dbo].[DimAffiliate] SET
--	 CountryName = C_CT.Country_Name
--	,MailCountryName = C_CT1.Country_Name
--	,[ETLLoadUpdateID]=@ETLLoadID
--FROM [dbo].[DimAffiliate] A WITH (NOLOCK) INNER JOIN  
-- (SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Country] (
--   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) C_CT
--ON A.CountryAbbreviation = C_CT.Country_Abbreviation
--INNER JOIN  
-- (SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Country] (
--   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) C_CT1
--ON A.MailCountryAbbreviation = C_CT1.Country_Abbreviation
--END
--COMMIT TRAN

	--EXEC [dbo].[usp_ETLLoadAudit]
	--	@StartEnd = 'E',
	--	@ETLLoadID = @ETLLoadID,
	--	@ExtractRowCount = @ExtractRowCount,
	--	@InsertRowCount = @InsertRowCount,
	--	@UpdateRowCount = @UpdateRowCount

/*********************************************************************************************
To update DimAffiliate based on the Country
*********************************************************************************************/

--EXEC @ETLLoadID = [dbo].[usp_ETLLoadAudit] 
--		@StartEnd = 'S',
--		@PackageName = 'usp_ETLDimAffiliate'

/*********************************************************************************************
Create a temp table and Insert the values from the CDC Country table.
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_Country') IS NOT NULL DROP TABLE #TMP_Country
CREATE TABLE #TMP_Country(Country_Abbreviation CHAR(2),Country_Name VARCHAR(60))
INSERT INTO #TMP_Country(Country_Abbreviation,Country_Name) 	
SELECT Country_Abbreviation,Country_Name
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Country] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	


SELECT @ExtractRowCount = COUNT(*) FROM #TMP_Country	

BEGIN TRANSACTION  

	UPDATE DA 
	SET 
		 CountryName= C.Country_Name
		,MailCountryName= C2.Country_Name
		,ETLLoadUpdateID = @ETLLoadID
	FROM [dbo].[DimAffiliate] AS DA 
		INNER JOIN #TMP_Country AS C
			ON DA.CountryAbbreviation = C.Country_Abbreviation
		INNER JOIN #TMP_Country AS C2
			ON DA.MailCountryAbbreviation = C2.Country_Abbreviation

COMMIT TRAN

SET  @UpdateRowCount = @@ROWCOUNT 	
SET @InsertRowCount = NULL

EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@UpdateRowCount = @UpdateRowCount
		
		IF OBJECT_ID('tempdb..#TMP_Affiliate') IS NOT NULL DROP TABLE #TMP_Affiliate
		IF OBJECT_ID('tempdb..#temp_AffiliateRecordCount') IS NOT NULL DROP TABLE #temp_AffiliateRecordCount
		IF OBJECT_ID('tempdb..#TMP_Country') IS NOT NULL DROP TABLE #TMP_Country

	RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount, 
		@UpdateRowCount = @UpdateRowCount,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		
		IF OBJECT_ID('tempdb..#TMP_Affiliate') IS NOT NULL DROP TABLE #TMP_Affiliate
		IF OBJECT_ID('tempdb..#temp_AffiliateRecordCount') IS NOT NULL DROP TABLE #temp_AffiliateRecordCount
		IF OBJECT_ID('tempdb..#TMP_Country') IS NOT NULL DROP TABLE #TMP_Country

		RETURN -1
	

END CATCH

END

