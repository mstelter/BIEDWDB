/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 8/23/2016
Description	: Populate the EDW.dbo.BuyerSearchData table from AC Search Tables	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
2/15/2017	MStelter			Change to use temp tables to speed processing
==============================================================================
Usage Example:
Exec usp_BuyerSearchData 
SELECT TOP 1000 * FROM EDW.dbo.BuyerSearchData
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_BuyerSearchData]
AS     
BEGIN

DECLARE @StartDate DateTime, @EndDate Datetime

SET NOCOUNT ON;
BEGIN TRY

--TRUNCATE TABLE EDW.dbo.BuyerSearchData

IF OBJECT_ID('tempdb..#ASL') IS NOT NULL DROP TABLE #ASL
IF OBJECT_ID('tempdb..#MODELS') IS NOT NULL DROP TABLE #MODELS
IF OBJECT_ID('tempdb..#MakeS') IS NOT NULL DROP TABLE #MakeS
IF OBJECT_ID('tempdb..#MODELS2') IS NOT NULL DROP TABLE #MODELS2
IF OBJECT_ID('tempdb..#MakeS2') IS NOT NULL DROP TABLE #MakeS2
IF OBJECT_ID('tempdb..#BuyerSearchData') IS NOT NULL DROP TABLE #BuyerSearchData

CREATE TABLE #BuyerSearchData(
	[SearchPlatform] [varchar](50) NOT NULL,
	[Branch] [int] NULL,
	[BranchName] [varchar](100) NULL,
	[Region] [varchar](100) NULL,
	[State] [varchar](100) NULL,
	[BuyerType] [varchar](100) NULL,
	[FastSearchText] [varchar](100) NULL,
	[VehicleType] [varchar](100) NULL,
	[VehicleSubType] [varchar](100) NULL,
	[AutomobileType] [varchar](100) NULL,
	[Scope] [varchar](100) NULL,
	[Transmission] [varchar](100) NULL,
	[FuelType] [varchar](100) NULL,
	[Cylinders] [varchar](100) NULL,
	[SaleDocument] [varchar](100) NULL,
	[YearFilter] [varchar](100) NULL,
	[PageNo] [varchar](100) NULL,
	[Shortcuts] [varchar](100) NULL,
	[IBFRange] [varchar](100) NULL,
	[LossType] [varchar](100) NULL,
	[AuctionDayFilter1] [varchar](100) NULL,
	[AuctionDayFilter2] [varchar](100) NULL,
	[AuctionWeekRange] [varchar](100) NULL,
	[AuctionDay] [varchar](100) NULL,
	[Make] [varchar](1000) NULL,
	[Model] [varchar](1000) NULL,
	[OdometerRange] [varchar](100) NULL,
	[VRDProvider] [varchar](100) NULL,
	[BodyStyleName] [varchar](100) NULL,
	[ExteriorColor] [varchar](100) NULL,
	[InteriorColor] [varchar](100) NULL,
	[DriveLineType] [varchar](100) NULL,
	[CountryOfOrigin] [varchar](100) NULL,
	[PromaryDamage] [varchar](100) NULL,
	[StartCode] [varchar](100) NULL,
	[SecondaryDamage] [varchar](100) NULL,
	[AirBag] [varchar](100) NULL,
	[Keys] [varchar](100) NULL,
	[ACVRange] [varchar](100) NULL,
	[WhoCanBuy] [varchar](100) NULL,
	[Branches] [varchar](1000) NULL,
	[TitleType] [varchar](100) NULL,
	[AuctionDate] [varchar](20) NULL,
	[IsPreSale] [int] NOT NULL,
	[ActionLogType] [int] NULL,
	[IPAddress] [varchar](20) NULL,
	[UserID] [int] NULL,
	[SalvageID] [int] NULL,
	[SearchDate] [datetime] NULL,
	[SearchString] [varchar](max) NULL)

--SET @StartDate = GETDATE()-5
--SET @EndDate = GETDATE()

SELECT @StartDate =MinDatetime,@EndDate =MaxDatetime  FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

--SET @StartDate = '8/17/2016'
--SET @EndDate = '8/24/2016'

SELECT CAST(SearchParams AS VarChar(MAX)) as SPText,SearchParams,
ASL.LogID,	ActionLogType,	CdDate,	Source,	UserID,	0 as LogDetailID,	0 as SalvageID,	'' AS AuctionDate
INTO #ASL
FROM [EDW].[dbo].AdvancedSearchLog ASL WITH (NOLOCK)
WHERE ASL.CdDate BETWEEN @StartDate AND @EndDate

CREATE CLUSTERED INDEX ix1 ON #ASL (LogID)

DELETE LU 
FROM   (SELECT *, 
               Row_number() 
                 OVER ( 
                   partition BY SPText,LogID,ActionLogType,CdDate,Source,UserID
                   ORDER BY LogID DESC) [Row] 
        FROM   #ASL) LU 
WHERE  [row] > 1 

--MODELS
SELECT MODELS.LogID,ACSM.ModelName 
INTO #MODELS 
FROM (SELECT LogID, N.C.value('@M', 'varchar(max)') MODELS
				FROM #ASL
				CROSS APPLY SearchParams.nodes('//Mds/Md') N(C)) MODELS
LEFT OUTER JOIN Staging.dbo.ACSalvageModel ACSM
	ON MODELS.Models = ACSM.SalvageModelID

SELECT LogID, Model = STUFF((SELECT N', ' + RTRIM(ModelName) 
  FROM #MODELS AS p2
   WHERE p2.LogID = p.LogID 
   ORDER BY LogID
   FOR XML PATH(N'')), 1, 2, N'')
Into #Models2
FROM #MODELS AS p
GROUP BY LogID
ORDER BY LogID

--Makes
SELECT Makes.LogID,ACSM.MakeName 
INTO #MAKES 
FROM (SELECT LogID, N.C.value('@N', 'varchar(max)') Makes
				FROM [EDW].[dbo].AdvancedSearchLog ASL
				CROSS APPLY SearchParams.nodes('//Mks/Mk') N(C)
				WHERE ASL.CdDate between @StartDate AND @EndDate) Makes
LEFT OUTER JOIN Staging.dbo.ACSalvageMake ACSM
	ON Makes.Makes = ACSM.SalvageMakeID

SELECT LogID, Make = STUFF((SELECT N', ' + RTRIM(MakeName) 
  FROM #MAKES AS p2
   WHERE p2.LogID = p.LogID 
   ORDER BY LogID
   FOR XML PATH(N'')), 1, 2, N'')
INTO #MAKES2
FROM #MAKES AS p
GROUP BY LogID
ORDER BY LogID

SELECT * INTO #SL FROM [EDW].[dbo].[SearchLog] SL WITH (NOLOCK)
WHERE SL.CdDate between @StartDate AND @EndDate

INSERT INTO #BuyerSearchData
           ([SearchPlatform],[Branch],[BranchName],[Region],[State],[BuyerType],[FastSearchText],[VehicleType],[VehicleSubType],[AutomobileType]
           ,[Scope],[Transmission],[FuelType],[Cylinders],[SaleDocument],[YearFilter],[PageNo],[Shortcuts],[IBFRange],[LossType],[AuctionDayFilter1]
           ,[AuctionDayFilter2],[AuctionWeekRange],[AuctionDay],[Make],[Model],[OdometerRange],[VRDProvider],[BodyStyleName],[ExteriorColor]
           ,[InteriorColor],[DriveLineType],[CountryOfOrigin],[PromaryDamage],[StartCode],[SecondaryDamage],[AirBag],[Keys],[ACVRange],[WhoCanBuy]
           ,[Branches],[TitleType],[AuctionDate],[IsPreSale],[ActionLogType],[IPAddress],[UserID],[SalvageID],[SearchDate],[SearchString])

SELECT 
'SearchLog' AS SearchPlatform,
IIF(SearchLogType = 1,RIGHT(LEFT(SearchParams,19),3),999) AS Branch,
DB.BranchName,
DB.RegionName AS Region,
DB.StateAbbreviation AS State,
'' AS BuyerType,
'' AS FastSearchText,
'' AS VehicleType,
'' AS VehicleSubType,
'' AS AutomobileType,
'' AS Scope,
'' AS Transmission,
'' AS FuelType,
'' AS Cylinders,
'' AS SaleDocument,
'' AS YearFilter,
0 AS PageNo,
'' AS Shortcuts,
'' AS IBFRange,
'' AS LossType,
'' AS AuctionDayFilter1,
'' AS AuctionDayFilter2,
'' ASAuctionWeekRange,
'' AS AuctionDay,
'' AS Make,
'' AS Model,
'' AS OdometerRange,
'' AS VRDProvider,
'' AS BodyStyleName,
'' AS ExteriorColor,
'' AS InteriorColor,
'' AS DriveLineType,
'' AS CountryOfOrigin,
'' AS PrimaryDamage,
'' AS StartCode,
'' AS SecondaryDamage,
'' AS AirBag,
'' AS Keys,
'' AS ACVRange,'' AS WhoCanBuy,'' AS Branches,
'' AS TitleType,
'' AS AuctionDate,
IIF(SearchLogType = 1,1,0) AS IsPreSale,
ActionLogType,
Source AS IPAddress,
UserID, 
0 AS SalvageID,
CDDate AS SearchDate,
'' AS SearchString
FROM #SL SL WITH (NOLOCK)
LEFT OUTER JOIN EDW.dbo.DimBranch DB WITH (NOLOCK)
	ON DB.BranchNumber = IIF(SearchLogType = 1,RIGHT(LEFT(SearchParams,19),3),999)
WHERE SL.CdDate between @StartDate AND @EndDate


INSERT INTO #BuyerSearchData
           ([SearchPlatform],[Branch],[BranchName],[Region],[State],[BuyerType],[FastSearchText],[VehicleType],[VehicleSubType],[AutomobileType]
           ,[Scope],[Transmission],[FuelType],[Cylinders],[SaleDocument],[YearFilter],[PageNo],[Shortcuts],[IBFRange],[LossType],[AuctionDayFilter1]
           ,[AuctionDayFilter2],[AuctionWeekRange],[AuctionDay],[Make],[Model],[OdometerRange],[VRDProvider],[BodyStyleName],[ExteriorColor]
           ,[InteriorColor],[DriveLineType],[CountryOfOrigin],[PromaryDamage],[StartCode],[SecondaryDamage],[AirBag],[Keys],[ACVRange],[WhoCanBuy]
           ,[Branches],[TitleType],[AuctionDate],[IsPreSale],[ActionLogType],[IPAddress],[UserID],[SalvageID],[SearchDate],[SearchString])
SELECT DISTINCT
'FastSearchLog' AS SearchPlatform,
999 AS Branch,
IIF(CHARINDEX('Branch :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Branch :: ',SearchParams)+9)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Branch :: ',SearchParams)+9))-(CHARINDEX('Branch :: ',SearchParams)+11)),'') AS BranchName,
IIF(CHARINDEX('Region :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Region :: ',SearchParams)+9)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Region :: ',SearchParams)+9))-(CHARINDEX('Region :: ',SearchParams)+11)),'') AS Region,
IIF(CHARINDEX('State :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('State :: ',SearchParams)+8)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('State :: ',SearchParams)+8))-(CHARINDEX('State :: ',SearchParams)+10)),'') AS State,
IIF(CHARINDEX('BuyerType :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('BuyerType :: ',SearchParams)+12)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('BuyerType :: ',SearchParams)+12))-(CHARINDEX('BuyerType :: ',SearchParams)+14)),'') AS BuyerType,
IIF(LEFT(SearchParams,13) = '| Query ::  |','',LEFT(RIGHT(SearchParams,LEN(SearchParams)-11),CHARINDEX ( '|' ,SearchParams,12)-13)) AS FastSearchText,
IIF(CHARINDEX('VehicleType :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('VehicleType :: ',SearchParams)+14)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('VehicleType :: ',SearchParams)+14))-(CHARINDEX('VehicleType :: ',SearchParams)+16)),'') AS VehicleType,
IIF(CHARINDEX('VehicleSubType :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('VehicleSubType :: ',SearchParams)+17)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('VehicleSubType :: ',SearchParams)+17))-(CHARINDEX('VehicleSubType :: ',SearchParams)+19)),'') AS VehicleSubType,
IIF(CHARINDEX('AutomobileType :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('AutomobileType :: ',SearchParams)+17)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('AutomobileType :: ',SearchParams)+17))-(CHARINDEX('AutomobileType :: ',SearchParams)+19)),'') AS AutomobileType,
IIF(CHARINDEX('Scope :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Scope :: ',SearchParams)+8)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Scope :: ',SearchParams)+8))-(CHARINDEX('Scope :: ',SearchParams)+10)),'') AS Scope,
IIF(CHARINDEX('Transmission :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Transmission :: ',SearchParams)+15)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Transmission :: ',SearchParams)+15))-(CHARINDEX('Transmission :: ',SearchParams)+17)),'') AS Transmission,
IIF(CHARINDEX('FuelType :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('FuelType :: ',SearchParams)+11)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('FuelType :: ',SearchParams)+11))-(CHARINDEX('FuelType :: ',SearchParams)+13)),'') AS FuelType,
IIF(CHARINDEX('Cylinders :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Cylinders :: ',SearchParams)+12)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Cylinders :: ',SearchParams)+12))-(CHARINDEX('Cylinders :: ',SearchParams)+14)),'') AS Cylinders,
IIF(CHARINDEX('SaleDocument :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('SaleDocument :: ',SearchParams)+15)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('SaleDocument :: ',SearchParams)+15))-(CHARINDEX('SaleDocument :: ',SearchParams)+17)),'') AS SaleDocument,
IIF(CHARINDEX('YearFilter :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('YearFilter :: ',SearchParams)+13)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('YearFilter :: ',SearchParams)+13))-(CHARINDEX('YearFilter :: ',SearchParams)+15)),'') AS YearFilter,
IIF(CHARINDEX('PageNo :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('PageNo :: ',SearchParams)+9)),1),'') AS PageNo,
IIF(CHARINDEX('Shortcuts :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Shortcuts :: ',SearchParams)+12)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Shortcuts :: ',SearchParams)+12))-(CHARINDEX('Shortcuts :: ',SearchParams)+14)),'') AS Shortcuts,
IIF(CHARINDEX('IBFRange :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('IBFRange :: ',SearchParams)+11)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('IBFRange :: ',SearchParams)+11))-(CHARINDEX('IBFRange :: ',SearchParams)+13)),'') AS IBFRange,
IIF(CHARINDEX('LossType :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('LossType :: ',SearchParams)+11)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('LossType :: ',SearchParams)+11))-(CHARINDEX('LossType :: ',SearchParams)+13)),'') AS LossType,
IIF(CHARINDEX('AuctionDayFilter1 :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('AuctionDayFilter1 :: ',SearchParams)+19)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('AuctionDayFilter1 :: ',SearchParams)+19))-(CHARINDEX('AuctionDayFilter1 :: ',SearchParams)+21)),'') AS AuctionDayFilter1,
IIF(CHARINDEX('AuctionDayFilter2 :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('AuctionDayFilter2 :: ',SearchParams)+19)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('AuctionDayFilter2 :: ',SearchParams)+19))-(CHARINDEX('AuctionDayFilter2 :: ',SearchParams)+21)),'') AS AuctionDayFilter2,
IIF(CHARINDEX('AuctionWeekRange :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('AuctionWeekRange :: ',SearchParams)+18)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('AuctionWeekRange :: ',SearchParams)+18))-(CHARINDEX('AuctionWeekRange :: ',SearchParams)+20)),'') AS AuctionWeekRange,
IIF(CHARINDEX('AuctionDay :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('AuctionDay :: ',SearchParams)+13)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('AuctionDay :: ',SearchParams)+13))-(CHARINDEX('AuctionDay :: ',SearchParams)+15)),'') AS AuctionDay,
IIF(CHARINDEX('Make :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Make :: ',SearchParams)+7)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Make :: ',SearchParams)+7))-(CHARINDEX('Make :: ',SearchParams)+9)),'') AS Make,
IIF(CHARINDEX('Model :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('Model :: ',SearchParams)+8)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('Model :: ',SearchParams)+8))-(CHARINDEX('Model :: ',SearchParams)+10)),'') AS Model,
IIF(CHARINDEX('OdometerRange :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('OdometerRange :: ',SearchParams)+16)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('OdometerRange :: ',SearchParams)+16))-(CHARINDEX('OdometerRange :: ',SearchParams)+18)),'') AS OdometerRange,
IIF(CHARINDEX('VRDProvider :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('VRDProvider :: ',SearchParams)+12)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('VRDProvider :: ',SearchParams)+12))-(CHARINDEX('VRDProvider :: ',SearchParams)+16)),'') AS VRDProvider,
IIF(CHARINDEX('BODYSTYLENAME :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('BODYSTYLENAME :: ',SearchParams)+16)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('BODYSTYLENAME :: ',SearchParams)+16))-(CHARINDEX('BODYSTYLENAME :: ',SearchParams)+18)),'') AS BodyStyleName,
IIF(CHARINDEX('EXTERIORCOLOR :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('EXTERIORCOLOR :: ',SearchParams)+16)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('EXTERIORCOLOR :: ',SearchParams)+16))-(CHARINDEX('EXTERIORCOLOR :: ',SearchParams)+18)),'') AS ExteriorColor,
IIF(CHARINDEX('INTERIORCOLOR :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('INTERIORCOLOR :: ',SearchParams)+16)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('INTERIORCOLOR :: ',SearchParams)+16))-(CHARINDEX('INTERIORCOLOR :: ',SearchParams)+18)),'') AS InteriorColor,
IIF(CHARINDEX('DRIVELINETYPE :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('DRIVELINETYPE :: ',SearchParams)+16)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('DRIVELINETYPE :: ',SearchParams)+16))-(CHARINDEX('DRIVELINETYPE :: ',SearchParams)+18)),'') AS DriveLineType,
IIF(CHARINDEX('COUNTRYOFORIGIN :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('COUNTRYOFORIGIN :: ',SearchParams)+18)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('COUNTRYOFORIGIN :: ',SearchParams)+18))-(CHARINDEX('COUNTRYOFORIGIN :: ',SearchParams)+20)),'') AS CountryOfOrigin,
IIF(CHARINDEX('PRIMARYDAMAGE :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('PRIMARYDAMAGE :: ',SearchParams)+16)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('PRIMARYDAMAGE :: ',SearchParams)+16))-(CHARINDEX('PRIMARYDAMAGE :: ',SearchParams)+18)),'') AS PromaryDamage,
IIF(CHARINDEX('STARTCODE :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('STARTCODE :: ',SearchParams)+12)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('STARTCODE :: ',SearchParams)+12))-(CHARINDEX('STARTCODE :: ',SearchParams)+14)),'') AS StartCode,
IIF(CHARINDEX('SECONDARYDAMAGE :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('SECONDARYDAMAGE :: ',SearchParams)+16)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('SECONDARYDAMAGE :: ',SearchParams)+16))-(CHARINDEX('SECONDARYDAMAGE :: ',SearchParams)+18)),'') AS SecondaryDamage,
IIF(CHARINDEX('AIRBAG :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('AIRBAG :: ',SearchParams)+9)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('AIRBAG :: ',SearchParams)+9))-(CHARINDEX('AIRBAG :: ',SearchParams)+11)),'') AS AirBag,
IIF(CHARINDEX('KEY :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('KEY :: ',SearchParams)+6)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('KEY :: ',SearchParams)+6))-(CHARINDEX('KEY :: ',SearchParams)+8)),'') AS Keys,
IIF(CHARINDEX('ACVRANGE :: ',SearchParams) > 0,LEFT(RIGHT(SearchParams,LEN(SearchParams)-(CHARINDEX('ACVRANGE :: ',SearchParams)+11)),(CHARINDEX ( '|' ,SearchParams,CHARINDEX('ACVRANGE :: ',SearchParams)+11))-(CHARINDEX('ACVRANGE :: ',SearchParams)+13)),'') AS ACVRange,
'' AS WhoCanBuy,'' AS Branches,
'' AS TitleType,'' AS AuctionDate,
0 AS IsPreSale,
ActionLogType,
Source AS IPAddress,
UserID, 
0 AS SalvageID,
CONVERT(VARCHAR(19),CDDate) AS SearchDate,
SearchParams AS SearchString
FROM [EDW].[dbo].FastSearchLog SL WITH (NOLOCK)
WHERE SL.CdDate between @StartDate AND @EndDate
AND SearchParams <> 'Start Over'


INSERT INTO #BuyerSearchData
           ([SearchPlatform],[Branch],[BranchName],[Region],[State],[BuyerType],[FastSearchText],[VehicleType],[VehicleSubType],[AutomobileType]
           ,[Scope],[Transmission],[FuelType],[Cylinders],[SaleDocument],[YearFilter],[PageNo],[Shortcuts],[IBFRange],[LossType],[AuctionDayFilter1]
           ,[AuctionDayFilter2],[AuctionWeekRange],[AuctionDay],[Make],[Model],[OdometerRange],[VRDProvider],[BodyStyleName],[ExteriorColor]
           ,[InteriorColor],[DriveLineType],[CountryOfOrigin],[PromaryDamage],[StartCode],[SecondaryDamage],[AirBag],[Keys],[ACVRange],[WhoCanBuy]
           ,[Branches],[TitleType],[AuctionDate],[IsPreSale],[ActionLogType],[IPAddress],[UserID],[SalvageID],[SearchDate],[SearchString])
SELECT
'AdvancedSearchLog' AS SearchPlatform,
999 AS Branch,
DB.BranchName,
DB.RegionName AS Region,
DB.StateAbbreviation AS State,
'' AS BuyerType,
'' AS FastSearchText,
VehicleType = (SELECT DISTINCT N.C.value('VT[1]', 'int') VehicleType
				FROM #ASL WITH (NOLOCK)
				CROSS APPLY SearchParams.nodes('//VTs') N(C) WHERE LogID=ASL.LogID),
VehicleSubType = (SELECT DISTINCT N.C.value('VS[1]', 'varchar(max)')  + 
							   IIF(N.C.value('VS[2]', 'varchar(max)') IS NULL,'',', ') +
								  ISNULL(N.C.value('VS[2]', 'varchar(max)'),'')  +
							   IIF(N.C.value('VS[3]', 'varchar(max)') IS NULL,'',', ') +
								  ISNULL(N.C.value('VS[3]', 'varchar(max)'),'') +
							   IIF(N.C.value('VS[4]', 'varchar(max)') IS NULL,'',', ') +
								  ISNULL(N.C.value('VS[4]', 'varchar(max)'),'') +
							   IIF(N.C.value('VS[5]', 'varchar(max)') IS NULL,'',', ') +
								  ISNULL(N.C.value('VS[5]', 'varchar(max)'),'') +
							   IIF(N.C.value('VS[6]', 'varchar(max)') IS NULL,'',', ') +
								  ISNULL(N.C.value('VS[6]', 'varchar(max)'),'') +
							   IIF(N.C.value('VS[7]', 'varchar(max)') IS NULL,'',', ') +
								  ISNULL(N.C.value('VS[7]', 'varchar(max)'),'')  VehicleSubType
							FROM #ASL WITH (NOLOCK)
							CROSS APPLY SearchParams.nodes('//VSs') N(C)
							WHERE LogID =ASL.LogID),
'' AS AutomobileType,
'' AS Scope,
'' AS Transmission,
'' AS FuelType,
Cylinders = (SELECT DISTINCT REPLACE(LEFT(Right(CAST(SearchParams AS VarChar(8000)),(LEN(CAST(SearchParams AS VarChar(8000)))-CHARINDEX('<Cyls>',CAST(SearchParams AS VarChar(8000))))-9),(CHARINDEX('</Cyls>',CAST(SearchParams AS VarChar(8000)))-(CHARINDEX('<Cyls>',CAST(SearchParams AS VarChar(8000)))+6+9))),'</Cy><Cy>',', ') AS Cylinders
				FROM #ASL
				WHERE CAST(SearchParams AS VarChar(8000)) NOT LIKE '%<Cyls All="true"%' and CAST(SearchParams AS VarChar(8000)) LIKE '%</Cy><Cy>%' AND LogID=ASL.LogID),
'' AS SaleDocument,
'' AS YearFilter,
0 AS PageNo,
'' AS Shortcuts,
'' AS IBFRange,
'' AS LossType,
AuctionDate AS AuctionDayFilter1,
AuctionDate AS AuctionDayFilter2,
'' ASAuctionWeekRange,
AuctionDate AS AuctionDay,
Make = (SELECT Make FROM #MAKES2 AS p2 WHERE p2.LogID = ASL.LogID),
Model = LEFT((SELECT Model FROM #Models2 AS p2 WHERE p2.LogID = ASL.LogID),1000),
OdometerRange = (SELECT DISTINCT N.C.value('@Min', 'varchar(max)') + ' - ' + N.C.value('@Max', 'varchar(max)') OdometerRange
				FROM #ASL
				CROSS APPLY SearchParams.nodes('//Odm') N(C) 
				WHERE LogID =ASL.LogID),
'' AS VRDProvider,
'' AS BodyStyleName,
ExteriorColor = (SELECT DISTINCT REPLACE(LEFT(Right(CAST(SearchParams AS VarChar(8000)),(LEN(CAST(SearchParams AS VarChar(8000)))-CHARINDEX('<Clrs>',CAST(SearchParams AS VarChar(8000))))-9),(CHARINDEX('</Clrs>',CAST(SearchParams AS VarChar(8000)))-(CHARINDEX('<Clrs>',CAST(SearchParams AS VarChar(8000)))+6+9))),'</Cl><Cl>',', ') AS ExteriorColor
					FROM #ASL 
					WHERE CAST(SearchParams AS VarChar(8000)) NOT LIKE '%<Clrs All="true"%' and CAST(SearchParams AS VarChar(8000)) LIKE '%</Cl><Cl>%' AND LogID =ASL.LogID),
'' AS InteriorColor,
'' AS DriveLineType,
'' AS CountryOfOrigin,
'' AS PrimaryDamage,
'' AS StartCode,
'' AS SecondaryDamage,
AirBag = (SELECT DISTINCT CASE N.C.value('@Abg', 'varchar(max)') 
			WHEN 0 THEN 'No Restrictions'
			WHEN 1 THEN 'Intact Airbags Only'
			ELSE 'Deployed Only(1 or more)'
			END AS AirBag
				FROM #ASL WITH (NOLOCK)
				CROSS APPLY SearchParams.nodes('//Cns') N(C)
				WHERE LogID=ASL.LogID),
'' AS Keys,
'' AS ACVRange,

WhoCanBuy = (SELECT DISTINCT CASE N.C.value('@Opt', 'varchar(max)') 
							WHEN 0 THEN 'All'
							WHEN 1 THEN 'Public'
							ELSE 'Licensed Business'
							END AS WhoCanBuy
				FROM #ASL WITH (NOLOCK)
				CROSS APPLY SearchParams.nodes('//WCB') N(C)
				WHERE LogID=ASL.LogID ),
Branches = (SELECT DISTINCT REPLACE(LEFT(Right(CAST(SearchParams AS VarChar(8000)),(LEN(CAST(SearchParams AS VarChar(8000)))-CHARINDEX('<Brs>',CAST(SearchParams AS VarChar(8000))))-8),(CHARINDEX('</Brs>',CAST(SearchParams AS VarChar(8000)))-(CHARINDEX('<Brs>',CAST(SearchParams AS VarChar(8000)))+6+8))),'</Br><Br>',', ') AS Branches
			FROM #ASL
			WHERE CAST(SearchParams AS VarChar(8000)) NOT LIKE '%<Brs All="true"%' and CAST(SearchParams AS VarChar(8000)) LIKE '%</Br><Br>%' AND LogID=ASL.LogID),
TitleType = (SELECT DISTINCT REPLACE(LEFT(Right(CAST(SearchParams AS VarChar(8000)),(LEN(CAST(SearchParams AS VarChar(8000)))-CHARINDEX('<TTs>',CAST(SearchParams AS VarChar(8000))))-8),(CHARINDEX('</TTs>',CAST(SearchParams AS VarChar(8000)))-(CHARINDEX('<TTs>',CAST(SearchParams AS VarChar(8000)))+6+8))),'</TT><TT>',', ')
			FROM #ASL
			WHERE CAST(SearchParams AS VarChar(8000)) NOT LIKE '%<TTs All="true"%' and CAST(SearchParams AS VarChar(8000)) LIKE '%</TT><TT>%' AND LogID=ASL.LogID),
AuctionDate,
'' AS IsPreSale,
ActionLogType,
Source AS IPAddress,
UserID, 
SalvageID,
CDDate AS SearchDate,
SPText AS SearchString
FROM #ASL ASL WITH (NOLOCK)
LEFT OUTER JOIN EDW.dbo.DimBranch DB WITH (NOLOCK)
	ON DB.BranchNumber = 999

DELETE FROM TBSD
FROM #BuyerSearchData TBSD WITH (NOLOCK)
inner join EDW.dbo.BuyerSearchData BSD WITH (NOLOCK)
ON TBSD.UserID = BSD.UserID
	AND TBSD.SearchDate = BSD.SearchDate
	AND TBSD.SearchString = BSD.SearchString

BEGIN TRANSACTION

INSERT INTO EDW.[dbo].[BuyerSearchData]
           ([SearchPlatform]
           ,[Branch]
           ,[BranchName]
           ,[Region]
           ,[State]
           ,[BuyerType]
           ,[FastSearchText]
           ,[VehicleType]
           ,[VehicleSubType]
           ,[AutomobileType]
           ,[Scope]
           ,[Transmission]
           ,[FuelType]
           ,[Cylinders]
           ,[SaleDocument]
           ,[YearFilter]
           ,[PageNo]
           ,[Shortcuts]
           ,[IBFRange]
           ,[LossType]
           ,[AuctionDayFilter1]
           ,[AuctionDayFilter2]
           ,[AuctionWeekRange]
           ,[AuctionDay]
           ,[Make]
           ,[Model]
           ,[OdometerRange]
           ,[VRDProvider]
           ,[BodyStyleName]
           ,[ExteriorColor]
           ,[InteriorColor]
           ,[DriveLineType]
           ,[CountryOfOrigin]
           ,[PromaryDamage]
           ,[StartCode]
           ,[SecondaryDamage]
           ,[AirBag]
           ,[Keys]
           ,[ACVRange]
           ,[WhoCanBuy]
           ,[Branches]
           ,[TitleType]
           ,[AuctionDate]
           ,[IsPreSale]
           ,[ActionLogType]
           ,[IPAddress]
           ,[UserID]
           ,[SalvageID]
           ,[SearchDate]
           ,[SearchString])
SELECT DISTINCT [SearchPlatform]
      ,[Branch]
      ,[BranchName]
      ,[Region]
      ,[State]
      ,[BuyerType]
      ,[FastSearchText]
      ,[VehicleType]
      ,[VehicleSubType]
      ,[AutomobileType]
      ,[Scope]
      ,[Transmission]
      ,[FuelType]
      ,[Cylinders]
      ,[SaleDocument]
      ,[YearFilter]
      ,[PageNo]
      ,[Shortcuts]
      ,[IBFRange]
      ,[LossType]
      ,[AuctionDayFilter1]
      ,[AuctionDayFilter2]
      ,[AuctionWeekRange]
      ,[AuctionDay]
      ,[Make]
      ,[Model]
      ,[OdometerRange]
      ,[VRDProvider]
      ,[BodyStyleName]
      ,[ExteriorColor]
      ,[InteriorColor]
      ,[DriveLineType]
      ,[CountryOfOrigin]
      ,[PromaryDamage]
      ,[StartCode]
      ,[SecondaryDamage]
      ,[AirBag]
      ,[Keys]
      ,[ACVRange]
      ,[WhoCanBuy]
      ,[Branches]
      ,[TitleType]
      ,[AuctionDate]
      ,[IsPreSale]
      ,[ActionLogType]
      ,[IPAddress]
      ,[UserID]
      ,[SalvageID]
      ,[SearchDate]
      ,[SearchString]
  FROM #BuyerSearchData

		COMMIT TRAN
			IF OBJECT_ID('tempdb..#ASL') IS NOT NULL DROP TABLE #ASL
			IF OBJECT_ID('tempdb..#MODELS') IS NOT NULL DROP TABLE #MODELS
			IF OBJECT_ID('tempdb..#MakeS') IS NOT NULL DROP TABLE #MakeS
			IF OBJECT_ID('tempdb..#MODELS2') IS NOT NULL DROP TABLE #MODELS2
			IF OBJECT_ID('tempdb..#MakeS2') IS NOT NULL DROP TABLE #MakeS2
			IF OBJECT_ID('tempdb..#BuyerSearchData') IS NOT NULL DROP TABLE #BuyerSearchData

		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		
			IF OBJECT_ID('tempdb..#ASL') IS NOT NULL DROP TABLE #ASL
			IF OBJECT_ID('tempdb..#MODELS') IS NOT NULL DROP TABLE #MODELS
			IF OBJECT_ID('tempdb..#MakeS') IS NOT NULL DROP TABLE #MakeS
			IF OBJECT_ID('tempdb..#MODELS2') IS NOT NULL DROP TABLE #MODELS2
			IF OBJECT_ID('tempdb..#MakeS2') IS NOT NULL DROP TABLE #MakeS2
			IF OBJECT_ID('tempdb..#BuyerSearchData') IS NOT NULL DROP TABLE #BuyerSearchData

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END