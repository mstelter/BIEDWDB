/* 
USE EDW
GO 
==============================================================================
Author            : Juan X Jacome
Create date : 06/11/2012
Description : Populate the EDW usp_ETLDimBuyerEmployee table every day
==============================================================================
REVISION History
7/2/2012	Juan X. Jacome	    Added logic for BuyerCurrentStatusEffectiveDateTime using CDC tables
8/22/2012   Somesh Velupalli    Added logic to Make Employees Inactive When Buyer Becomes Inactive
08/27/2012  Venkata Vempali     Updated logic for CurrentStatusEffectiveDateTime
08/30/2012  Venkata Vempali     Updated logic for CurrentStatusEffectiveDateTime
09/06/2012  Somesh V            Added logic for keeping status as New for any buyer Inserting for first Time
09/11/2012  Venkata Vempali     Fixed CurrentStatusEffectiveDateTime for updated BuyerEmployee
2/26/2013   Matt Stelter	    Updated BuyerEmployeeStatusDescription
12/29/2015   Matt Stelter	    Update Initial Setup Date
2/10/2016   Matt Stelter		Removed logic to Make Employees Inactive When Buyer Becomes Inactive
4/22/2016  Yogitha Alwarsetty   Added Buyer Employee Title
6/15/2016	Matt Stelter		Change Buyer Employee Email Logic
==============================================================================
Usage Example:
Exec usp_ETLDimBuyerEmployee
==============================================================================
*/
CREATE  PROCEDURE [dbo].[usp_ETLDimBuyerEmployee]

AS     
BEGIN


DECLARE @ETLLoadID INT,
		@ExtractRowCount INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
		
SET NOCOUNT ON;
BEGIN TRY

----Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimBuyerEmployee'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK)WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp tables
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_DimBuyerEmployee') IS NOT NULL DROP TABLE #TMP_DimBuyerEmployee
CREATE TABLE #TMP_DimBuyerEmployee
(	[BuyerEmployeeID] [int] NOT NULL,
	[BuyerID] [int] NULL,
	[EmployeeName] [nvarchar](61) NULL,
	[AddressLine1] [nvarchar](40) NULL,
	[AddressLine2] [nvarchar](40) NULL,
	[CityName] [nvarchar](40) NULL,
	[StateAbbreviation] [nchar](3) NULL,
	[ZipCode] [nvarchar](20) NULL,
	[PhoneNumber] [nvarchar](11) NULL,
	[BirthDate] [datetime] NULL,
	[EmailAddress] [nvarchar](60) NULL,
	[RegistrationEffectiveDayID] [int] NULL,
	[RegistrationEffectiveDateTime] [datetime] NULL,
	[RegistrationExpirationDayID] [int] NULL,
	[RegistrationExpirationDateTime] [datetime] NULL,
	[RegistrationRenewalDayID] [int] NULL,
	[RegistrationRenewalDateTime] [datetime] NULL,
	[CurrentStatusCode] [nchar](3) NULL,
	[CurrentStatusDescription] [nvarchar](40) NULL,
	[CurrentStatusEffectiveDayID] [int] NULL,
	[CurrentStatusEffectiveDateTime] [datetime] NULL,
	[InitialSetupDayID] [int] NULL,
	[InitialSetupDateTime] [datetime] NULL,
	[IsArchived] [tinyint] NOT NULL,
	[BidderDisplayName] [nvarchar](100) NULL,
	[BuyerEmployeeTitle] [Varchar](40) 
)

IF OBJECT_ID('tempdb..#TMP_DimBuyerEmployee_Count') IS NOT NULL DROP TABLE #TMP_DimBuyerEmployee_Count
CREATE TABLE #TMP_DimBuyerEmployee_Count(ChangeType VARCHAR(10)) 


IF OBJECT_ID('tempdb..#TMP_BuyerEmployee_CurrentEffectiveStatusChange') IS NOT NULL DROP TABLE #TMP_BuyerEmployee_CurrentEffectiveStatusChange
CREATE TABLE #TMP_BuyerEmployee_CurrentEffectiveStatusChange
(	[BuyerEmployeeID] [int] NOT NULL,
	[CurrentStatusEffectiveDateTime] [datetime] NULL
)
      
IF OBJECT_ID('tempdb..#TMP_BuyerEmployee_User_info') IS NOT NULL DROP TABLE #TMP_BuyerEmployee_User_info      
CREATE TABLE #TMP_BuyerEmployee_User_info
(	[BuyerEmployeeID]         INT,
    [EmployeeName] [nvarchar](61) NULL,
	[AddressLine1] [nvarchar](40) NULL,
	[AddressLine2] [nvarchar](40) NULL,
	[CityName] [nvarchar](40) NULL,
	[StateAbbreviation] [nchar](3) NULL,
	[ZipCode] [nvarchar](20) NULL,
	[PhoneNumber] [nvarchar](11) NULL,
	[BirthDate] [datetime] NULL,
	[EmailAddress] [nvarchar](60) NULL
)      

IF OBJECT_ID('tempdb..#Temp_Buyer_Status_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Status_Data
CREATE TABLE #Temp_Buyer_Status_Data  
 (	 [Buyer_ID]	[int]
    ,[Status_code]	[char] (3)
    ,[Buyer_Current_Status_Effective_Datetime] [datetime])


/*********************************************************************************************
Insert into #temp tables
*********************************************************************************************/
INSERT INTO #TMP_DimBuyerEmployee (
	 BuyerEmployeeID
      ,BuyerID
      ,EmployeeName
      ,AddressLine1
      ,AddressLine2
      ,CityName
      ,StateAbbreviation
      ,ZipCode
      ,PhoneNumber
      ,BirthDate
      ,EmailAddress
      ,RegistrationEffectiveDayID
      ,RegistrationEffectiveDateTime
      ,RegistrationExpirationDayID
      ,RegistrationExpirationDateTime
      ,RegistrationRenewalDayID
      ,RegistrationRenewalDateTime
      ,CurrentStatusCode
      ,CurrentStatusDescription
      ,CurrentStatusEffectiveDayID
      ,CurrentStatusEffectiveDateTime
      ,InitialSetupDayID
      ,InitialSetupDateTime
      ,IsArchived
      ,BidderDisplayName
	  ,BuyerEmployeeTitle 
)
select BE.Buyer_Employee_ID AS BuyerEmployeeID
		,BE.Buyer_ID AS BuyerID
		,CASE
           WHEN UI.First_Name IS NULL THEN ''
           ELSE UI.First_Name+' '
           END + Isnull(UI.Last_Name, '') AS EmployeeName
        ,ISNULL(UI.Address_Line_1,'') AS AddressLine1
        ,ISNULL(UI.Address_Line_2,'') AS AddressLine2
        ,ISNULL(UI.City_Name, '') AS CityName
        ,ISNULL(UI.State_Abbreviation, '') AS StateAbbreviation
        ,ISNULL(UI.Zip_Code, '') AS ZipCode
        ,ISNULL(UI.Phone_Number_1, '') AS PhoneNumber
        ,ISNULL(UI.Birth_Date, '') AS BirthDate
        ,ISNULL(UI.EMail_Address_1, '') AS EmailAddress
        ,ISNULL(CONVERT(INT, CONVERT(CHAR(8),(BE.Effective_Date), 112)), 29991231)AS RegistrationEffectiveDayID
        ,ISNULL(BE.Effective_Date, '2999-12-31') AS RegistrationEffectiveDateTime
        ,ISNULL(CONVERT(INT, CONVERT(CHAR(8),(BE.Expiration_Date), 112)), 29991231) AS RegistrationExpirationDayID
        ,ISNULL(BE.Expiration_Date, '2999-12-31') AS RegistrationExpirationDateTime
        ,ISNULL(CONVERT(INT, CONVERT(CHAR(8),(BE.Expiration_Date-60), 112)), 29991231)AS RegistrationRenewalDayID
        ,ISNULL(BE.Expiration_Date-60, '2999-12-31') AS RegistrationRenewalDateTime
        ,ISNULL(BE.Status_Code, '') AS CurrentStatusCode
        --,CASE WHEN Status_code IN ('ACT', 'PRO') THEN 'ACTIVE'
        --     WHEN Status_code IN ('New') THEN 'NEW'
        --     WHEN Status_code IN ('DUP') THEN 'DUPLICATE'
        --     ELSE 'INACTIVE' END AS CurrentStatusDescription
		,CASE 
			WHEN Status_code IN ('NEW') THEN 'NEW'
			WHEN Status_code IN ('ACT') THEN 'ACTIVE'
			WHEN Status_code IN ('PRO') THEN 'PROSPECTIVE'
			WHEN Status_code IN ('ARH') THEN 'ARCHIVED'
			WHEN Status_code IN ('BAN') THEN 'BANNED'
			WHEN Status_code IN ('INA') THEN 'INACTIVE'
			WHEN Status_code IN ('OUT') THEN 'OUT OF BUSINESS'
			WHEN Status_code IN ('DUP') THEN 'DUPLICATE' 
			ELSE 'INACTIVE' END AS CurrentStatusDescription
        ,29991231  AS CurrentStatusEffectiveDayID
        ,'2999-12-31' AS CurrentStatusEffectiveDateTime
        ,ISNULL(CONVERT(INT, CONVERT(CHAR(8),(BE.Effective_Date), 112)), 29991231) AS InitialSetupDayID --Only use INSERT
        ,ISNULL(BE.Effective_Date, '2999-12-31') AS InitialSetupDateTime -- only use INSERT
        ,ISNULL(BE.Archive_Ind, 0) AS IsArchived
        ,CONVERT(VARCHAR(20), BE.Buyer_ID)+'_'+CONVERT(VARCHAR(20), BE.Buyer_Employee_ID) AS BidderDisplayName
		,ISNULL(JT.Job_Title_Description,'') AS BuyerEmployeeTitle
  FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_buyer_employee] (@StartLSN, @EndLSN, 'all with merge') AS BE 
  left outer join [BI_ASAP_REP].dbo.User_Association UA WITH (NOLOCK)
  on BE.Buyer_Employee_ID = UA.Buyer_Employee_ID
  left outer join [BI_ASAP_REP].dbo.User_Info UI WITH (NOLOCK)
  on UA.[User_ID] = UI.[User_ID]
    left outer Join [BI_ASAP_REP].dbo.Job_Title JT WITH (NOLOCK)
	on JT.Job_Title_Code=BE.Job_Title_Code
  WHERE  [__$operation]=5


  UPDATE T SET 
  CurrentStatusCode = 'New'
  ,CurrentStatusDescription = 'NEW'
  FROM #TMP_DimBuyerEmployee T
  INNER JOIN BI_ASAP_Rep.dbo.buyer_employee BE
   ON T.BuyerEmployeeID = BE.Buyer_Employee_ID
   WHERE ISNULL(BE.Expiration_Date,'2999-12-31') = '2999-12-31'
     AND ISNULL(BE.Effective_Date,'2999-12-31') = '2999-12-31'
   
  
  

  UPDATE BE SET
  --SELECT BE.BuyerEmployeeID,
		 CurrentStatusEffectiveDateTime = BEA.Update_DateTime
		,CurrentStatusEffectiveDayID = CONVERT(INT, CONVERT(CHAR(8),(BEA.Update_DateTime), 112)) 
   
  FROM #TMP_DimBuyerEmployee BE 
		INNER JOIN BI_ASAP_Audit_Rep.dbo.Buyer_Employee_Audit AS BEA WITH(NOLOCK)
			ON BEA.Buyer_Employee_ID = BE.BuyerEmployeeID
		INNER JOIN (SELECT Buyer_Employee_ID, MAX(Update_DateTime) AS MaxUpdateTime
						FROM BI_ASAP_Audit_Rep.dbo.Buyer_Employee_Audit WITH(NOLOCK) 
						GROUP BY Buyer_Employee_ID) Max_BE
			ON BEA.Buyer_Employee_ID = Max_BE.Buyer_Employee_ID AND BEA.Update_DateTime = Max_BE.MaxUpdateTime

   INSERT INTO #Temp_Buyer_Status_Data(Buyer_ID ,Status_code ,Buyer_Current_Status_Effective_Datetime )
	SELECT Buyer_ID ,Status_Code ,DB.BuyerCurrentStatusEffectiveDatetime
	FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_buyer] (@StartLSN, @EndLSN, 'all with merge') AS B
		INNER JOIN dbo.DimBuyer DB WITH(NOLOCK)
			ON B.Buyer_ID = DB.BuyerID
	WHERE  [__$operation]=5

--Update Initial Setup Date
  UPDATE DBE
  SET  DBE.InitialSetupDateTime = ISNULL(BEA.Update_DateTime, '2999-12-31')
      ,DBE.InitialSetupDayID =  ISNULL(CONVERT(INT, CONVERT(CHAR(8),(BEA.Update_DateTime), 112)), 29991231)
--SELECT DBE.InitialSetupDateTime,ISNULL(BEA.Update_DateTime, '2999-12-31')
  FROM #TMP_DimBuyerEmployee DBE With(NoLock)
  INNER JOIN (SELECT BE.Buyer_Employee_ID,MIN(BE.Update_DateTime) as Update_DateTime
    FROM BI_ASAP_Audit_Rep.dbo.Buyer_Employee_Audit BE WITH(NoLock)
  --WHERE BE.Status_Code = 'ACT'
  GROUP BY BE.Buyer_Employee_ID) BEA
    ON DBE.BuyerEmployeeID = BEA.Buyer_Employee_Id
WHERE DBE.InitialSetupDateTime = '2999-12-31 00:00:00.000'

SELECT @ExtractRowCount=COUNT(*) FROM #TMP_DimBuyerEmployee WITH (NOLOCK)

/*********************************************************************************************
* Merge
*********************************************************************************************/


  
  BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.DimBuyerEmployee AS DBE
	USING #TMP_DimBuyerEmployee AS TBE    
		ON DBE.BuyerEmployeeID =  TBE.BuyerEmployeeID
		
WHEN MATCHED THEN
    --Row exists and data is different
	UPDATE SET 	
	   DBE.BuyerID = TBE.BuyerID
      ,DBE.EmployeeName = TBE.EmployeeName
      ,DBE.AddressLine1 = TBE.AddressLine1
      ,DBE.AddressLine2 = TBE.AddressLine2
      ,DBE.CityName = TBE.CityName
      ,DBE.StateAbbreviation = TBE.StateAbbreviation
      ,DBE.ZipCode = TBE.ZipCode
      ,DBE.PhoneNumber = TBE.PhoneNumber
      ,DBE.BirthDate = TBE.BirthDate
      ,DBE.EmailAddress = TBE.EmailAddress
      ,DBE.RegistrationEffectiveDayID = TBE.RegistrationEffectiveDayID
      ,DBE.RegistrationEffectiveDateTime = TBE.RegistrationEffectiveDateTime
      ,DBE.RegistrationExpirationDayID = TBE.RegistrationExpirationDayID
      ,DBE.RegistrationExpirationDateTime = TBE.RegistrationExpirationDateTime
      ,DBE.RegistrationRenewalDayID = TBE.RegistrationRenewalDayID
      ,DBE.RegistrationRenewalDateTime = TBE.RegistrationRenewalDateTime
      ,DBE.CurrentStatusCode = TBE.CurrentStatusCode
      ,DBE.CurrentStatusDescription = TBE.CurrentStatusDescription
      ,DBE.CurrentStatusEffectiveDayID = TBE.CurrentStatusEffectiveDayID 
      ,DBE.CurrentStatusEffectiveDateTime = TBE.CurrentStatusEffectiveDateTime
      ,DBE.IsArchived = TBE.IsArchived
      ,DBE.BidderDisplayName = TBE.BidderDisplayName
	  ,DBE.BuyerEmployeeTitle=TBE.BuyerEmployeeTitle
      ,DBE.ETLLoadUpdateID = @ETLLoadID
		
WHEN NOT MATCHED THEN
   INSERT(	BuyerEmployeeID
      ,BuyerID
      ,EmployeeName
      ,AddressLine1
      ,AddressLine2
      ,CityName
      ,StateAbbreviation
      ,ZipCode
      ,PhoneNumber
      ,BirthDate
      ,EmailAddress
      ,RegistrationEffectiveDayID
      ,RegistrationEffectiveDateTime
      ,RegistrationExpirationDayID
      ,RegistrationExpirationDateTime
      ,RegistrationRenewalDayID
      ,RegistrationRenewalDateTime
      ,CurrentStatusCode
      ,CurrentStatusDescription
      ,CurrentStatusEffectiveDayID
      ,CurrentStatusEffectiveDateTime
      ,InitialSetupDayID
      ,InitialSetupDateTime
      ,IsArchived
      ,BidderDisplayName
	  ,BuyerEmployeeTitle
      ,ETLLoadID
      ,ETLLoadUpdateID
		)	 
	VALUES (BuyerEmployeeID
      ,BuyerID
      ,EmployeeName
      ,AddressLine1
      ,AddressLine2
      ,CityName
      ,StateAbbreviation
      ,ZipCode
      ,PhoneNumber
      ,BirthDate
      ,EmailAddress
      ,RegistrationEffectiveDayID
      ,RegistrationEffectiveDateTime
      ,RegistrationExpirationDayID
      ,RegistrationExpirationDateTime
      ,RegistrationRenewalDayID
      ,RegistrationRenewalDateTime
      ,CurrentStatusCode
      ,CurrentStatusDescription
      ,CurrentStatusEffectiveDayID
      ,CurrentStatusEffectiveDateTime
      ,InitialSetupDayID
      ,InitialSetupDateTime
      ,IsArchived
      ,BidderDisplayName
	  ,BuyerEmployeeTitle
      ,@ETLLoadID
      ,@ETLLoadID
			)
	     
	OUTPUT $action INTO #TMP_DimBuyerEmployee_Count;

	  
/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_DimBuyerEmployee_Count WITH (NOLOCK)

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

/*********************************************************************************
Update Buyer Employee CurrentStatusEffectiveDateTime when only status code changes
*********************************************************************************/

--INSERT INTO #TMP_BuyerEmployee_CurrentEffectiveStatusChange 
--(	BuyerEmployeeID,
--	CurrentStatusEffectiveDateTime
--)

--SELECT	BE2.Buyer_Employee_ID AS BuyerEmployeeID ,
--		max(BE2.Update_DateTime) AS CurrentStatusEffectiveDateTime
--FROM [BI_ASAP_Rep].cdc.dbo_buyer_employee_CT AS BEI
--INNER JOIN [BI_ASAP_Rep].cdc.dbo_buyer_employee_CT As BE2
--ON BEI.__$seqval = BE2.__$seqval
--AND BEI.__$operation = 3
--AND BE2.__$operation = 4
--AND ISNULL(BEI.Status_Code, -1) <> ISNULL(BE2.Status_Code,-1)
--WHERE BE2.__$start_lsn >= @StartLSN and BE2.__$start_lsn <= @EndLSN
--GROUP BY BE2.Buyer_Employee_ID


--UPDATE DBE
--SET	CurrentStatusEffectiveDayID = ISNULL(Cast(CONVERT(CHAR(8), TBEC.CurrentStatusEffectiveDateTime, 112) AS INT), 29991231),
--    CurrentStatusEffectiveDateTime = TBEC.CurrentStatusEffectiveDateTime
--FROM DimBuyerEmployee DBE 
--INNER JOIN #TMP_BuyerEmployee_CurrentEffectiveStatusChange TBEC
--ON DBE.BuyerEmployeeID = TBEC.BuyerEmployeeID


/*******************************************************************
Update Buyer Employee User Info -- when only User Info data change
********************************************************************/
INSERT INTO  #TMP_BuyerEmployee_User_info
(	BuyerEmployeeID,
    EmployeeName,
	AddressLine1,
	AddressLine2,
	CityName,
	StateAbbreviation,
	ZipCode,
	PhoneNumber,
	BirthDate,
	EmailAddress
) 
SELECT	DBE.BuyerEmployeeID
		,CASE
			WHEN UI.First_Name IS NULL THEN ''
			ELSE UI.First_Name+' '
			END + Isnull(UI.Last_Name, '') AS EmployeeName
        ,ISNULL(UI.Address_Line_1,'') AS AddressLine1
        ,ISNULL(UI.Address_Line_2,'') AS AddressLine2
        ,ISNULL(UI.City_Name, '') AS CityName
        ,ISNULL(UI.State_Abbreviation, '') AS StateAbbreviation
        ,ISNULL(UI.Zip_Code, '') AS ZipCode
        ,ISNULL(UI.Phone_Number_1, '') AS PhoneNumber
        ,ISNULL(UI.Birth_Date, '') AS BirthDate
        ,ISNULL(UI.EMail_Address_1, '') AS EmailAddress	
  FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_User_Info] (@StartLSN, @EndLSN, 'all with merge') AS UI 
  INNER JOIN [BI_ASAP_REP].dbo.User_Association UA WITH (NOLOCK)
   ON UI.[User_ID] = UA.[User_ID]
  INNER JOIN DimBuyerEmployee DBE WITH (NOLOCK)
   ON UA.Buyer_Employee_ID = DBE.BuyerEmployeeID
  WHERE  [__$operation]=5
  
  
  UPDATE DBE
  SET	EmployeeName = TBUI.EmployeeName,
		AddressLine1 = TBUI.AddressLine1,
		AddressLine2 = TBUI.AddressLine2,
		CityName = TBUI.CityName,
		StateAbbreviation = TBUI.StateAbbreviation,
		ZipCode = TBUI.ZipCode,
		PhoneNumber = TBUI.PhoneNumber,
		BirthDate = TBUI.BirthDate,
		EmailAddress = TBUI.EmailAddress
  FROM DimBuyerEmployee DBE 
  INNER JOIN #TMP_BuyerEmployee_User_info TBUI
  ON DBE.BuyerEmployeeID = TBUI.BuyerEmployeeID

--------UPDATE BuyerEE Email--------

UPDATE DBE SET EmailAddress = IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),''))
FROM [BI_ASAP_Rep].dbo.buyer_employee AS BE 
  left outer join [BI_ASAP_REP].dbo.User_Association UA WITH (NOLOCK)
  on BE.Buyer_Employee_ID = UA.Buyer_Employee_ID
  left outer join [BI_ASAP_REP].dbo.User_Info UI WITH (NOLOCK)
  on UA.[User_ID] = UI.[User_ID] --where BE.Buyer_ID = 25111
  inner join edw.dbo.DimBuyerEmployee DBE
  on be.Buyer_Employee_ID = dbe.BuyerEmployeeID
  where dbe.EmailAddress <> IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),''))
  
--/*******************************************************************
--Update Buyer Employee -- when Buyer becomes Inactive 
--********************************************************************/

 -- UPDATE DBE 
 --   SET 
 --  -- DBE.CurrentStatusCode = ISNULL(TBSD.Status_Code, 'INA')
 --  --,DBE.CurrentStatusDescription =  CASE WHEN TBSD.Status_code IN ('ACT', 'PRO') THEN 'ACTIVE'
 --  -- 	                                    WHEN TBSD.Status_code IN ('New') THEN 'NEW'
	--	 --                                   WHEN TBSD.Status_code IN ('DUP') THEN 'DUPLICATE'
	--		--                                ELSE 'INACTIVE' END 
 --   DBE.CurrentStatusCode = CASE WHEN TBSD.Status_code IN ('INA') THEN 'INA'
	--								WHEN TBSD.Status_code IN ('DUP') THEN 'DUP'
	--								WHEN TBSD.Status_code IN ('OUT') THEN 'OUT'
	--								WHEN TBSD.Status_code IN ('BAN') THEN 'BAN'
	--								WHEN TBSD.Status_code IN ('ARH') THEN 'ARH'
	--								ELSE 'INA' END
 --   ,DBE.CurrentStatusDescription = CASE WHEN TBSD.Status_code IN ('INA') THEN 'INACTIVE'
	--								WHEN TBSD.Status_code IN ('OUT') THEN 'OUT OF BUSINESS'
	--								WHEN TBSD.Status_code IN ('DUP') THEN 'DUPLICATE'
	--								WHEN TBSD.Status_code IN ('BAN') THEN 'BANNED'
	--								WHEN TBSD.Status_code IN ('ARH') THEN 'ARCHIVED'
	--								ELSE 'INACTIVE' END	
 --  ,DBE.CurrentStatusEffectiveDateTime = ISNULL(TBSD.Buyer_Current_Status_Effective_Datetime, '12/31/2999')
 --  ,DBE.CurrentStatusEffectiveDayID = ISNULL(CAST(CONVERT(CHAR(8), TBSD.Buyer_Current_Status_Effective_Datetime, 112) AS INT), 29991231)
 --   FROM 
	--EDW.dbo.DimBuyerEmployee DBE 
	--INNER JOIN #Temp_Buyer_Status_Data TBSD 
	--ON DBE.BuyerID = TBSD.Buyer_ID
	--WHERE TBSD.Status_code IN ('ARH', 'BAN', 'INA', 'OUT','DUP') --Is InActive
	----AND DBE.CurrentStatusCode IN ('ACT','PRO') --IsActive
	--AND TBSD.Status_code <> DBE.CurrentStatusCode

 --   UPDATE DBE 
 --   SET 
 --   DBE.CurrentStatusCode = --ISNULL(TBSD.Status_Code, 'INA')
	--									CASE WHEN BE.Status_code IN ('ACT') THEN 'ACT'
 --   											WHEN BE.Status_code IN ('PRO') THEN 'PRO'
 --   											--WHEN BE.Status_code IN ('New') THEN 'NEW'
	--											ELSE 'INA' END
 --  ,DBE.CurrentStatusDescription =  CASE WHEN BE.Status_code IN ('ACT') THEN 'ACTIVE'
 --   	                                    WHEN BE.Status_code IN ('PRO') THEN 'PROSPECTIVE'
 --   	                                    --WHEN BE.Status_code IN ('New') THEN 'NEW'
	--		                                ELSE 'INACTIVE' END 
 --  ,DBE.CurrentStatusEffectiveDateTime = ISNULL(TBSD.Buyer_Current_Status_Effective_Datetime, '12/31/2999')
 --  ,DBE.CurrentStatusEffectiveDayID = ISNULL(CAST(CONVERT(CHAR(8), TBSD.Buyer_Current_Status_Effective_Datetime, 112) AS INT), 29991231)
	--FROM 
	--EDW.dbo.DimBuyerEmployee DBE 
	--INNER JOIN #Temp_Buyer_Status_Data TBSD 
	--ON DBE.BuyerID = TBSD.Buyer_ID
	--INNER JOIN BI_ASAP_Rep.dbo.buyer_employee BE 
	--ON DBE.BuyerEmployeeID = BE.Buyer_Employee_ID
	--WHERE BE.Status_Code IN ('ACT','PRO')
	--	AND DBE.CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT','DUP')
	--	AND TBSD.Status_code NOT IN ('ARH', 'BAN', 'INA', 'OUT','DUP')
  
  	COMMIT TRAN
		IF OBJECT_ID('tempdb..#TMP_DimBuyerEmployee') IS NOT NULL DROP TABLE #TMP_DimBuyerEmployee
		IF OBJECT_ID('tempdb..#TMP_DimBuyerEmployee_Count') IS NOT NULL DROP TABLE #TMP_DimBuyerEmployee_Count
		IF OBJECT_ID('tempdb..#TMP_BuyerEmployee_CurrentEffectiveStatusChange') IS NOT NULL DROP TABLE #TMP_BuyerEmployee_CurrentEffectiveStatusChange
		IF OBJECT_ID('tempdb..#TMP_BuyerEmployee_User_info') IS NOT NULL DROP TABLE #TMP_BuyerEmployee_User_info 
		IF OBJECT_ID('tempdb..#Temp_Buyer_Status_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Status_Data 
		
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
		
		IF OBJECT_ID('tempdb..#TMP_DimBuyerEmployee') IS NOT NULL DROP TABLE #TMP_DimBuyerEmployee
		IF OBJECT_ID('tempdb..#TMP_DimBuyerEmployee_Count') IS NOT NULL DROP TABLE #TMP_DimBuyerEmployee_Count
		IF OBJECT_ID('tempdb..#TMP_BuyerEmployee_CurrentEffectiveStatusChange') IS NOT NULL DROP TABLE #TMP_BuyerEmployee_CurrentEffectiveStatusChange
		IF OBJECT_ID('tempdb..#TMP_BuyerEmployee_User_info') IS NOT NULL DROP TABLE #TMP_BuyerEmployee_User_info 
		IF OBJECT_ID('tempdb..#Temp_Buyer_Status_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Status_Data
		--IF OBJECT_ID('tempdb..') IS NOT NULL DROP TABLE
		
		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END
