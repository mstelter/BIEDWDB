/* 
USE EDW
GO 
==============================================================================
Author		: Sidd Murao
Create date	: 08/12/2011
Description	: Populate the EDW DimBranch table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer			
08/18/2011	Sidd Murao		Added Region Info, CountryName and other misc columns
08/29/2011	Sidd Murao		Modified to include default values
10/28/2011	Juan X. Jacome	Update sp to include drop temp tables (DBA Code Review)
4/11/2012	Matt Stelter	Removed Auction Frequncy
9/12/2013	Matt Stelter	Added IsCreditCardAllowed
05/06/2014	Juan X. Jacome	Added IsVirtualBranch 
==============================================================================
Usage Example:
Exec usp_ETLDimBranch
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimBranch]
AS     
BEGIN

DECLARE @ETLLoadID        INT,
        @ETLLoadUpdateID INT,
		@ExtractRowCount  INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
SET NOCOUNT ON;

	BEGIN TRY
            
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimBranch'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp tables to consolidate the Branch and Region Data
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_Branch') IS NOT NULL DROP TABLE #TMP_Branch
CREATE TABLE #TMP_Branch
(
    [Branch_Number] [int] NULL,
	[IAA_Branch_Name] [nvarchar](60) NULL,
	[Address_Line_1] [nvarchar](40) NULL,
	[Address_Line_2] [nvarchar](40) NULL,
	[City_Name] [nvarchar](40) NULL,
	[State_Abbreviation] [nvarchar](3) NULL,
	[Zip_Code] [nvarchar](9) NULL,
	[County_Name] [nvarchar] (30) NULL,
	[Country_Abbreviation] [char](2) NULL,
    [Country_Name] [nvarchar](40) NULL,
	  
	[GMTOffset] [int] NULL,
	[IAACorporateName] [nvarchar](40) NULL,
	[Maximum_Units_Per_Acre] [smallint] NULL,
	[ParticipatesInDaylightSavingTime] [tinyint] NULL,
	[Phone_Number_1] [decimal](11, 0) NULL,
	[Phone_Number_2] [decimal](11, 0) NULL,
	[RegionName] [nvarchar](20) NULL,
	[RegionNumber] [int] NULL,
	[Service_Center_Number] [int] NULL,
	[Service_Center_Ind] [bit] NULL,
	[Status_Code] [nchar](3) NULL,
	[Usable_Acreage] [smallint] NULL,
	[Branch_Type] [nchar](3) NULL,
	--[BranchAuctionFrequency] [varchar](20) NULL,
	[BusinessStartDate] [datetime] NULL, 
	[LegalEntity] [nchar](3) NULL,
	[AuctionMethodCode] [nchar] (3) NULL,
	[AuctionMethodDescription] [nvarchar] (20) NULL,
	[TimeZone] [nchar](3) NULL,
	IsCreditCardAllowed TinyInt NULL,
	IsVirtualBranch TinyInt NULL
	)


INSERT INTO #TMP_Branch
([Branch_Number] ,
	[IAA_Branch_Name],
	[Address_Line_1] ,
	[Address_Line_2] ,
	[City_Name] ,
	[State_Abbreviation],
	[Zip_Code] ,
	[County_Name],
	[Country_Abbreviation] ,
  
	[Maximum_Units_Per_Acre] ,
	[Phone_Number_1] ,
	[Phone_Number_2] ,
	[Service_Center_Number] ,
	[Service_Center_Ind],
	[Status_Code] ,
	[Usable_Acreage] ,
	[Branch_Type] ,
	[BusinessStartDate], 
	[LegalEntity],
	[TimeZone],
	[AuctionMethodCode],
	[RegionNumber])

	SELECT Branch_Number
		  ,IAA_Branch_Name
		  ,Address_Line_1    
	      ,Address_Line_2
	      ,City_Name
	      ,State_Abbreviation,
	      Zip_Code
	      ,County_Name
	      ,Country_Abbreviation
	
		  ,Maximum_Units_Per_Acre
		  ,[Phone_Number_1]
		  ,[Phone_Number_2]
	      ,Service_Center_Number
	      ,isnull(Service_Center_Ind,0)
	      ,Status_Code
	      ,Usable_Acreage
	      ,Branch_Type
		  ,isnull([Business_Start_Date],'2999-12-31 00:00:00.000') 
	      ,[Legal_Entity]
	      ,[Time_Zone]
	      ,[Auction_Method_Code]
	      ,[Region_Number]
	      --from [BI_ASAP_REP].dbo.IAA_Branch
FROM  [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_IAA_Branch] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	
   
   --select * from #tmp_branch 
   --select * from [bi_asap_rep].cdc.dbo_IAA_Branch_CT

UPDATE TB
SET IsCreditCardAllowed = ISNULL([Credit_Card_Allowed_Ind],0),
	IsVirtualBranch = CASE WHEN TB.Branch_Type = 'VIR' THEN 1 ELSE 0 END 
FROM #TMP_Branch TB WITH (NOLOCK)
INNER JOIN BI_ASAP_REP.dbo.IAA_Branch B WITH (NOLOCK)
	ON TB.Branch_Number = B.Branch_Number

UPDATE #TMP_Branch
SET  [Country_Name] = CN.[Country_Name]
    ,[GMTOffset]=  case rtrim(BR.TimeZone)
									when 'AST' then -4
									when 'ET'  then -5
									when 'CT'  then -6
									when 'MT'  then -7
									when 'MST' then -7
									when 'PT'  then -8
									when 'AKT' then -9
									when 'HT' then -10
									else -6
	  							  end
	,[IAACorporateName]='IAACorporateName'
	,[ParticipatesInDaylightSavingTime]=  case BR.State_Abbreviation
		when 'AZ' then 0
		when 'HI' then 0
		else 1 end
	,[RegionName] = IR.Region_Name
	,[RegionNumber]=IR.Region_Number,
    [AuctionMethodDescription]=ABRM.Auction_method_Description
FROM #TMP_Branch  BR  with (nolock)
left outer  JOIN BI_ASAP_REP.dbo.IAA_Region IR with (nolock)
on  (IR.Region_number=BR.RegionNumber)
left outer join [BI_ASAP_REP].[dbo].[Country] CN WITH(NOLOCK) 
on (  BR .country_abbreviation = CN.country_abbreviation)
left outer join [BI_ASAP_REP].[dbo].[Auction_method] ABRM WITH(NOLOCK)  
on (ABRM.Auction_Method_Code=  BR.AuctionMethodCode)

--select * from #tmp_branch


SELECT @ExtractRowCount = COUNT(*) FROM #TMP_Branch

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimBranch table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

IF OBJECT_ID('tempdb..#temp_##TMP_BranchRecordCount') IS NOT NULL DROP TABLE #TMP_BranchRecordCount
CREATE TABLE #TMP_BranchRecordCount(ChangeType VARCHAR(10)) 

BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.DimBranch AS bo
	USING #TMP_Branch AS bb    
		ON bb.Branch_Number = bo.BranchNumber
  
	WHEN NOT MATCHED  THEN 
	--Row exists in source but not in target
	INSERT (   
 [BranchNumber] ,
	[BranchName] ,
	[AddressLine1] ,
	[AddressLine2] ,
	[CityName] ,
	[StateAbbreviation] ,
	[ZipCode] ,
	[CountryAbbreviation] ,
	[CountyName] ,
    [CountryName] ,
	[GMTOffset] ,
	[IAACorporateName] ,
	[MaximumUnitsPerAcre] ,
	[ParticipatesInDaylightSavingTime],
	[PhoneNumber1] ,
	[PhoneNumber2] ,
	[RegionName] ,
	[RegionNumber] ,
	[ServiceCenterNumber] ,
	[IsServiceCenter] ,
	[StatusCode] ,
	[UsableAcreage] ,
	[BranchType],
	--[BranchAuctionFrequency] ,
	[BusinessStartDate], 
	[LegalEntity],
	[AuctionMethodDescription],
	[TimeZone],
    [ETLloadID],
    [ETLLoadUpdateID],
	IsCreditCardAllowed,
	IsVirtualBranch)
	VALUES (
			bb.Branch_Number
		  ,bb.IAA_Branch_Name
		  ,bb.Address_Line_1    
	      ,bb.Address_Line_2
	      ,bb.City_Name
	      ,bb.State_Abbreviation
	      ,bb.Zip_Code
	      ,bb.Country_Abbreviation
		,bb.County_Name
		,bb.Country_Name
	,isnull(bb.GMTOffset,0)
	,bb.IAACorporateName
	,bb.Maximum_Units_Per_Acre
    ,bb.ParticipatesInDaylightSavingTime
	,bb.[Phone_Number_1]
    ,bb.[Phone_Number_2],
	bb.[RegionName] ,
	bb.[RegionNumber] ,
	bb.[Service_Center_Number] ,
	bb.[Service_Center_Ind] ,
	bb.[Status_Code] ,
	bb.[Usable_Acreage] ,
	bb.[Branch_Type],
	--bb.[BranchAuctionFrequency],
    bb.[BusinessStartDate] ,
	bb.[LegalEntity],
	bb.[AuctionMethodDescription],
	bb.[TimeZone],
    @ETLLoadID,
    @ETLLoadID,
	ISNULL(IsCreditCardAllowed,0),
	bb.IsVirtualBranch
    )
  

	WHEN MATCHED  THEN
      --Row exists and data is different
	UPDATE SET 
	bo.[BranchName]=bb.IAA_Branch_Name,
	bo.[AddressLine1] =bb.Address_Line_1,
	bo.[AddressLine2] =bb.Address_Line_2 ,
	bo.[CityName] =bb.City_Name,
	bo.[StateAbbreviation] =bb.State_Abbreviation,
	bo.[ZipCode] =bb.Zip_Code,
	bo.[CountryAbbreviation] =bb.Country_Abbreviation,
	bo.[CountyName]=bb.[County_Name],
    bo.[CountryName] =bb.Country_Name,
	bo.[GMTOffset] =bb.GMTOffset,
	bo.[IAACorporateName] =bb.IAACorporateName,
	bo.[MaximumUnitsPerAcre] =bb.Maximum_Units_Per_Acre,
	bo.[ParticipatesInDaylightSavingTime] =bb.ParticipatesInDaylightSavingTime,
	bo.[PhoneNumber1] =bb.[Phone_Number_1],
	bo.[PhoneNumber2] =bb.[Phone_Number_2],
	bo.[RegionName] = bb.[RegionName],
	bo.[RegionNumber] =bb.[RegionNumber] ,
	bo.[ServiceCenterNumber] =bb.[Service_Center_Number],
	bo.[IsServiceCenter] =bb.[Service_Center_Ind],
	bo.[StatusCode] =bb.[Status_Code],
	bo.[UsableAcreage] =bb.[Usable_Acreage],
	bo.[BranchType] =bb.[Branch_Type],
	--bo.[BranchAuctionFrequency] =bb.[BranchAuctionFrequency],
	bo.[BusinessStartDate]=bb.[BusinessStartDate] ,
	bo.[LegalEntity]=bb.[LegalEntity],
	bo.[TimeZone]=bb.[TimeZone],
	bo.[AuctionMethodDescription]=bb.[AuctionMethodDescription],
	bo.[ETLLoadUpdateID]=@ETLLoadID,
	bo.IsCreditCardAllowed = ISNULL(bb.IsCreditCardAllowed,0),
	bo.IsVirtualBranch = bb.IsVirtualBranch

OUTPUT $action INTO #TMP_BranchRecordCount;
--select * from dimbranch
SELECT @InsertRowCount = COUNT(*)  FROM #TMP_BranchRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #TMP_BranchRecordCount WHERE ChangeType = 'UPDATE'


	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount
COMMIT TRAN
	
	IF OBJECT_ID('tempdb..#TMP_Branch') IS NOT NULL DROP TABLE #TMP_Branch
	IF OBJECT_ID('tempdb..#TMP_BranchRecordCount') IS NOT NULL DROP TABLE #TMP_BranchRecordCount
		
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
		
		IF OBJECT_ID('tempdb..#TMP_Branch') IS NOT NULL DROP TABLE #TMP_Branch
		IF OBJECT_ID('tempdb..#TMP_BranchRecordCount') IS NOT NULL DROP TABLE #TMP_BranchRecordCount

		RETURN -1
	

END CATCH

END
