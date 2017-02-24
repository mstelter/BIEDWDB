
/* 
USE EDW
GO 
==============================================================================
Author		: Venkata Vempali
Create date	: 08/14/2012
Description	: Populate the DimBuyerEmployeeDetail table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
08/22/2012     VV           Added additional conditions, so that Proc which ran 
							mutiple times (without cdc change in ETLRun table) won't insert duplicate records.
08/23/2012     VV           Added logic:- Update the previous termination date
08/29/2012     VV           Updated Prevous termination date logic.
08/29/2012     VV           Updated condition, so that Proc can be ran multiple times 
							without inserting duplicate records.
09/06/2012  Somesh V        Added logic to insert new Buyer with status code of NEW(ASAP Shows Inactive)	
02/06/2012     VV           Added Distinct to pull only Distinct records. R5.11.02 
2/26/2013   Matt Stelter	    Updated BuyerEmployeeStatusDescription 	
2/10/2016   Matt Stelter	Removed Updating/Inserting based on Buyer INACTIVE					
==============================================================================
Usage Example:
Exec usp_ETLDimBuyerEmployeeDetail
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimBuyerEmployeeDetail]
AS     
BEGIN

DECLARE @ETLLoadID        INT,
		@ExtractRowCount  INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@MinLSN BINARY(10),
		@MaxLSN BINARY(10)
				
SET NOCOUNT ON;

BEGIN TRY
            
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimBuyerEmployeeDetail'




/*********************************************************************************************
Create the temp table to consolidate the Current and Static Storage Locations
*********************************************************************************************/



    
	IF OBJECT_ID('tempdb..#Temp_Buyer_Employee_Audit_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Employee_Audit_Data
	CREATE TABLE #Temp_Buyer_Employee_Audit_Data(  
		 Buyer_ID	[int]
		,Buyer_Employee_ID	[int]
		,Effective_Date	[datetime]
		,Expiration_Date	[datetime]
		,Update_datetime	[datetime]
		,Status_code	[char] (3)
		,Audit_Action_Code	[char] (1))
	
	 --Update Existing Buyer Employee Details  
	 IF OBJECT_ID('tempdb..#Temp_Buyer_Employee_Audit_Data_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Employee_Audit_Data_Update
	 CREATE TABLE #Temp_Buyer_Employee_Audit_Data_Update(
		 Buyer_ID	[int]
		,Buyer_Employee_ID	[int]
		,Effective_Date	[datetime]
		,Expiration_Date	[datetime]
		,Update_datetime	[datetime]
		,Status_code	[char] (3))
	
	
	--IF OBJECT_ID('tempdb..#Temp_Buyer_Status_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Status_Data
	--CREATE TABLE #Temp_Buyer_Status_Data(  
	--	 Buyer_ID	[int]
	--	,Status_code	[char] (3)
	--	,Buyer_Current_Status_Effective_Datetime [datetime])

	--IF OBJECT_ID('tempdb..#Temp_Buyer_Inactive_Insert_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Inactive_Insert_Update
	--CREATE TABLE #Temp_Buyer_Inactive_Insert_Update(
	--	 Buyer_ID [int]
	--	,Buyer_Employee_ID [int]
	--	,Detail_Status [varchar](10)
	--	,Detail_Status_Code	[char] (3)
	--	,Detail_Status_Effective_Datetime [datetime]
	--	,Detail_Status_Effective_DayID  [int] 
	--	,Detail_Status_Expiration_Datetime 	[datetime]
	--	,Detail_Status_Expiration_DayID  [int])
	
	--IF OBJECT_ID('tempdb..#Temp_Buyer_Active_Insert_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Active_Insert_Update
	--CREATE TABLE #Temp_Buyer_Active_Insert_Update(
	--	 Buyer_ID [int]
	--	,Buyer_Employee_ID [int]
	--	,Detail_Status [varchar](10)
	--	,Detail_Status_Code	[char] (3)
	--	,Detail_Status_Effective_Datetime [datetime]
	--	,Detail_Status_Effective_DayID  [int] 
	--	,Detail_Status_Expiration_Datetime 	[datetime]
	--	,Detail_Status_Expiration_DayID  [int])
	


--CDC Changes from [BI_ASAP_Audit_Rep].[dbo].Buyer_Employee_Audit

	--Get the start and end LSNs
	SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob_BI_ASAP_AUDIT_Rep'	
	
	
	INSERT INTO #Temp_Buyer_Employee_Audit_Data(
		Buyer_ID ,Buyer_Employee_ID ,Effective_Date ,Expiration_Date ,Update_datetime ,Status_code ,Audit_Action_Code)
	SELECT DISTINCT Buyer_ID ,Buyer_Employee_ID ,Effective_Date ,Expiration_Date ,Update_datetime ,Status_code ,Audit_Action_Code
	FROM [BI_ASAP_Audit_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Buyer_Employee_Audit](
	@MinLSN,@MaxLSN,'all with merge') 
	WHERE  [__$operation]=5
	--[BI_ASAP_Audit_Rep].[dbo].Buyer_Employee_Audit


	INSERT INTO #Temp_Buyer_Employee_Audit_Data_Update(
		Buyer_ID ,Buyer_Employee_ID ,Effective_Date ,Expiration_Date ,Update_datetime ,Status_code)
	SELECT DISTINCT BEA.Buyer_ID
		 ,BEA.Buyer_Employee_ID
		 ,BEA.Effective_Date
		 ,BEA.Expiration_Date
		 ,BEA.Update_datetime
		 ,BEA.Status_code
	FROM BI_ASAP_Audit_Rep.dbo.Buyer_Employee_Audit BEA WITH (NOLOCK)
	 INNER JOIN (
				 SELECT Buyer_Employee_ID ,MAX(Update_datetime) AS Max_Update_Datetime
				 FROM #Temp_Buyer_Employee_Audit_Data
				 WHERE  Audit_Action_Code='U'
				 GROUP  BY Buyer_Employee_ID ) T_MUD
	   ON BEA.Buyer_Employee_ID = T_MUD.Buyer_Employee_ID
		  AND BEA.Update_datetime = T_MUD.Max_Update_Datetime


--Buyer Employee Detail - Inactivate if Buyer is inactive and Active when the Buyer comes active again----

--CDC Changes from [BI_ASAP_Rep].[dbo].Buyer 
	
	--Get the Min and Max LSN's
	--SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'



	--INSERT INTO #Temp_Buyer_Status_Data(Buyer_ID ,Status_code ,Buyer_Current_Status_Effective_Datetime )
	--SELECT Buyer_ID ,Status_Code ,DB.BuyerCurrentStatusEffectiveDatetime
	--FROM [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_buyer] (@MinLSN, @MaxLSN, 'all with merge') B
	--	INNER JOIN dbo.DimBuyer DB WITH(NOLOCK)
	--		ON B.Buyer_ID = DB.BuyerID
	--WHERE  [__$operation]=5
	
	
	
	----Updating/Inserting based on Buyer INACTIVE.
	--INSERT INTO #Temp_Buyer_Inactive_Insert_Update(Buyer_ID ,Buyer_Employee_ID ,Detail_Status ,Detail_Status_Code
	--		, Detail_Status_Effective_Datetime ,Detail_Status_Effective_DayID 
	--		, Detail_Status_Expiration_Datetime ,Detail_Status_Expiration_DayID)
	--SELECT DISTINCT TBSD.Buyer_ID 
	--		,DBED.BuyerEmployeeID 
	--		,CASE WHEN TBSD.Status_code IN ('ACT') THEN 'ACTIVE'
	--				WHEN TBSD.Status_code IN ('PRO') THEN 'PROSPECTIVE'
	--			  WHEN TBSD.Status_code IN ('New') THEN 'NEW'
	--			  WHEN TBSD.Status_code IN ('DUP') THEN 'DUPLICATE'
	--				ELSE 'INACTIVE' END AS Buyer_Employee_Detail_Status
	--		,ISNULL(TBSD.Status_Code, 'INA') AS Buyer_Employee_Detail_Status_Code
	--		,ISNULL(TBSD.Buyer_Current_Status_Effective_Datetime, '12/31/2999') AS Buyer_Employee_Detail_Status_Effective_Datetime
	--		,ISNULL(CAST(CONVERT(CHAR(8), TBSD.Buyer_Current_Status_Effective_Datetime, 112) AS INT), 29991231) AS Buyer_Employee_Detail_Status_Effective_Day_ID
	--		,'12/31/2999' AS Buyer_Employee_Detail_Status_Termination_Datetime
	--		,29991231 AS Buyer_Employee_Detail_Status_Termination_Day_ID
	----SELECT TBSD.* ,DBED.BuyerEmployeeID ,DBED.BuyerEmployeeDetailStatusCode ,DBED.BuyerEmployeeDetailStatusEffectiveDatetime
	----,DBED.BuyerEmployeeDetailStatusTerminationDatetime
	--FROM #Temp_Buyer_Status_Data TBSD WITH(NOLOCK) 
	--	INNER JOIN dbo.DimBuyerEmployeeDetail DBED WITH(NOLOCK)
	--		ON TBSD.Buyer_ID = DBED.BuyerId
	--WHERE TBSD.Status_code IN ('ARH', 'BAN', 'INA', 'OUT')	--INACTIVE Status
	--		--AND DBED.BuyerEmployeeDetailStatusCode IN ('ACT','PRO') --Active Status
	--		AND TBSD.Status_code <> DBED.BuyerEmployeeDetailStatusCode
	--		AND DBED.BuyerEmployeeDetailStatusTerminationDayID = '29991231'
	--Order by 1,2

	----Updating/Inserting based on Buyer ACTIVE.
	--INSERT INTO #Temp_Buyer_Active_Insert_Update(Buyer_ID ,Buyer_Employee_ID ,Detail_Status ,Detail_Status_Code
	--		, Detail_Status_Effective_Datetime ,Detail_Status_Effective_DayID 
	--		, Detail_Status_Expiration_Datetime ,Detail_Status_Expiration_DayID)
	--SELECT DISTINCT TBSD.Buyer_ID 
	--		,DBED.BuyerEmployeeID 
	--		,CASE WHEN TBSD.Status_code IN ('ACT') THEN 'ACTIVE'
	--				WHEN TBSD.Status_code IN ('PRO') THEN 'PROSPECTIVE'
	--			  WHEN TBSD.Status_code IN ('New') THEN 'NEW'
	--			  WHEN TBSD.Status_code IN ('DUP') THEN 'DUPLICATE'
	--				ELSE 'INACTIVE' END AS Buyer_Employee_Detail_Status
	--		,ISNULL(TBSD.Status_Code, 'INA') AS Buyer_Employee_Detail_Status_Code
	--		,ISNULL(TBSD.Buyer_Current_Status_Effective_Datetime, '12/31/2999') AS Buyer_Employee_Detail_Status_Effective_Datetime
	--		,ISNULL(CAST(CONVERT(CHAR(8), TBSD.Buyer_Current_Status_Effective_Datetime, 112) AS INT), 29991231) AS Buyer_Employee_Detail_Status_Effective_Day_ID
	--		,'12/31/2999' AS Buyer_Employee_Detail_Status_Termination_Datetime
	--		,29991231 AS Buyer_Employee_Detail_Status_Termination_Day_ID
	----SELECT TBSD.* ,DBED.BuyerEmployeeID ,DBED.BuyerEmployeeDetailStatusCode ,DBED.BuyerEmployeeDetailStatusEffectiveDatetime
	----,DBED.BuyerEmployeeDetailStatusTerminationDatetime
	--FROM #Temp_Buyer_Status_Data TBSD WITH(NOLOCK) 
	--	INNER JOIN dbo.DimBuyerEmployeeDetail DBED WITH(NOLOCK)
	--		ON TBSD.Buyer_ID = DBED.BuyerId
	--WHERE TBSD.Status_code IN ('ACT','PRO') --Active Status
	--		AND DBED.BuyerEmployeeDetailStatusCode IN ('ARH', 'BAN', 'INA', 'OUT')	--INACTIVE Status
	--		AND DBED.BuyerEmployeeDetailStatusTerminationDayID = '29991231'
	--Order by 1,2




/*********************************************************************************************
INSERT/UPDATE the data from the temp table into the DimBuyerEmployeeDetail table
No Deletes  
*********************************************************************************************/

BEGIN TRANSACTION 

		--Insert New Buyer Employee Details records
		INSERT INTO dbo.DimBuyerEmployeeDetail(
			BuyerId ,BuyerEmployeeID ,BuyerEmployeeDetailStatus ,BuyerEmployeeDetailStatusCode 
			, BuyerEmployeeDetailStatusEffectiveDatetime ,BuyerEmployeeDetailStatusEffectiveDayID
			, BuyerEmployeeDetailStatusTerminationDatetime ,BuyerEmployeeDetailStatusTerminationDayID
			, ETLLoadID ,ETLLoadUpdateID)
		SELECT TBEAD.Buyer_ID
			,TBEAD.Buyer_Employee_ID
			--,CASE WHEN TBEAD.Status_code IN ('ACT', 'PRO') THEN 'ACTIVE'
			--	  WHEN TBEAD.Status_code IN ('New') THEN 'NEW'
			--	  WHEN TBEAD.Status_code IN ('DUP') THEN 'DUPLICATE'
			--		ELSE 'INACTIVE' END AS Buyer_Employee_Detail_Status
			,'NEW' AS Buyer_Employee_Detail_Status
			,'New' AS Buyer_Employee_Detail_Status_Code
			--,ISNULL(TBEAD.Status_Code, 'INA') AS Buyer_Employee_Detail_Status_Code
			,ISNULL(TBEAD.Update_datetime, '12/31/2999') AS Buyer_Employee_Detail_Status_Effective_Datetime
			,ISNULL(CAST(CONVERT(CHAR(8), TBEAD.Update_datetime, 112) AS INT), 29991231) AS Buyer_Employee_Detail_Status_Effective_Day_ID
			,'12/31/2999' AS Buyer_Employee_Detail_Status_Termination_Datetime
			,29991231 AS Buyer_Employee_Detail_Status_Termination_Day_ID
			,@ETLLoadID
			,@ETLLoadID AS ETLLoadUpdateID
		FROM  #Temp_Buyer_Employee_Audit_Data TBEAD WITH (NOLOCK)
			 LEFT OUTER JOIN dbo.DimBuyerEmployeeDetail DBED WITH (NOLOCK)
			   ON TBEAD.Buyer_Employee_ID = DBED.BuyerEmployeeID 
		WHERE TBEAD.Audit_Action_Code = 'I' AND DBED.BuyerEmployeeID IS NULL
	  
	  
	    --Update the previous termination date
     
		--SELECT DBED.BuyerEmployeeID,DBED.BuyerEmployeeDetailStatusCode ,TBEADU.Status_code
		--  ,BuyerEmployeeDetailStatusTerminationDatetime,BuyerEmployeeDetailStatusEffectiveDatetime ,TBEADU.Update_datetime
		
		UPDATE DBED SET
			 BuyerEmployeeDetailStatusTerminationDatetime  = TBEADU.Update_datetime
			,BuyerEmployeeDetailStatusTerminationDayID = ISNULL(CAST(CONVERT(CHAR(8), TBEADU.Update_datetime, 112) AS INT), 29991231)
			,ETLLoadUpdateID = @ETLLoadID
		FROM #Temp_Buyer_Employee_Audit_Data_Update TBEADU WITH (NOLOCK)
			INNER JOIN dbo.DimBuyerEmployeeDetail DBED WITH (NOLOCK)
			   ON TBEADU.Buyer_Employee_ID = DBED.BuyerEmployeeID
		WHERE TBEADU.Status_code <> DBED.BuyerEmployeeDetailStatusCode 
				AND DBED.BuyerEmployeeDetailStatusEffectiveDatetime < TBEADU.Update_datetime
				AND DBED.BuyerEmployeeDetailStatusTerminationDayID = '29991231'
		
	  
	  
		--Insert the new record 
		INSERT INTO dbo.DimBuyerEmployeeDetail(
			BuyerId ,BuyerEmployeeID ,BuyerEmployeeDetailStatus ,BuyerEmployeeDetailStatusCode 
			, BuyerEmployeeDetailStatusEffectiveDatetime ,BuyerEmployeeDetailStatusEffectiveDayID
			, BuyerEmployeeDetailStatusTerminationDatetime ,BuyerEmployeeDetailStatusTerminationDayID
			, ETLLoadID ,ETLLoadUpdateID)

		SELECT TBEADU.Buyer_ID
			 ,TBEADU.Buyer_Employee_ID
			 --,CASE WHEN TBEADU.Status_code IN ('ACT', 'PRO') THEN 'ACTIVE'
				--   WHEN TBEADU.Status_code IN ('New') THEN 'NEW'
				--   WHEN TBEADU.Status_code IN ('DUP') THEN 'DUPLICATE'
				--	ELSE 'INACTIVE' END AS Buyer_Employee_Detail_Status
				,CASE 
					WHEN TBEADU.Status_code IN ('NEW') THEN 'NEW'
					WHEN TBEADU.Status_code IN ('ACT') THEN 'ACTIVE'
					WHEN TBEADU.Status_code IN ('PRO') THEN 'PROSPECTIVE'
					WHEN TBEADU.Status_code IN ('ARH') THEN 'ARCHIVED'
					WHEN TBEADU.Status_code IN ('BAN') THEN 'BANNED'
					WHEN TBEADU.Status_code IN ('INA') THEN 'INACTIVE'
					WHEN TBEADU.Status_code IN ('OUT') THEN 'OUT OF BUSINESS'
					WHEN TBEADU.Status_code IN ('DUP') THEN 'DUPLICATE' 
					ELSE 'INACTIVE' END AS Buyer_Employee_Detail_Status
			 ,ISNULL(TBEADU.Status_Code, 'INA') AS Buyer_Employee_Detail_Status_Code
			 ,ISNULL(TBEADU.Update_datetime, '12/31/2999') AS Buyer_Employee_Detail_Status_Effective_Datetime
			 ,ISNULL(CAST(CONVERT(CHAR(8), TBEADU.Update_datetime, 112) AS INT), 29991231) AS Buyer_Employee_Detail_Status_Effective_Day_ID
			 ,'12/31/2999' AS Buyer_Employee_Detail_Status_Termination_Datetime
			 ,29991231 AS Buyer_Employee_Detail_Status_Termination_Day_ID
			 ,@ETLLoadID
			 ,@ETLLoadID AS ETLLoadUpdateID
		FROM #Temp_Buyer_Employee_Audit_Data_Update TBEADU WITH (NOLOCK)
		WHERE NOT EXISTS 
			(SELECT 1 FROM dbo.DimBuyerEmployeeDetail DBED WITH (NOLOCK)
					WHERE TBEADU.Buyer_ID = DBED.BuyerID
							AND	TBEADU.Buyer_Employee_ID = DBED.BuyerEmployeeID 
							AND TBEADU.Status_Code = DBED.BuyerEmployeeDetailStatusCode
							--AND TBEADU.Update_datetime = DBED.BuyerEmployeeDetailStatusEffectiveDatetime
							AND ISNULL(CAST(CONVERT(CHAR(8), TBEADU.Update_datetime, 112) AS INT), 29991231)= DBED.BuyerEmployeeDetailStatusEffectiveDayID
							--AND DBED.BuyerEmployeeDetailStatusTerminationDayID = 29991231
			 )

		  


--Buyer Employee Detail - Inactivate if Buyer is inactive and Active when the Buyer comes active again----
		
		--Updating/Inserting based on Buyer INACTIVE.

		--Update the Previous status Termination date with current status Effective Date.
		--UPDATE DBED SET	
		--	 BuyerEmployeeDetailStatusTerminationDatetime = T.Detail_Status_Effective_Datetime
		--	,BuyerEmployeeDetailStatusTerminationDayID = T.Detail_Status_Effective_DayID
		--	,ETLLoadUpdateID = @ETLLoadID
		----SELECT DBED.BuyerEmployeeID ,BuyerEmployeeDetailStatusTerminationDatetime ,T.Detail_Status_Effective_Datetime	
		--FROM dbo.DimBuyerEmployeeDetail DBED 
		--	INNER JOIN #Temp_Buyer_Inactive_Insert_Update T WITH(NOLOCK)
		--		ON DBED.BuyerEmployeeID = T.Buyer_Employee_ID --AND DBED.BuyerId = T.Buyer_ID
		--WHERE DBED.BuyerEmployeeDetailStatusCode IN ('ACT','PRO')  
		--		AND DBED.BuyerEmployeeDetailStatusEffectiveDatetime < T.Detail_Status_Effective_Datetime
		--		AND DBED.BuyerEmployeeDetailStatusTerminationDayID = '29991231'		


		----Insert the New records.
		--INSERT INTO dbo.DimBuyerEmployeeDetail(BuyerId ,BuyerEmployeeID ,BuyerEmployeeDetailStatus ,BuyerEmployeeDetailStatusCode
		--	,BuyerEmployeeDetailStatusEffectiveDatetime ,BuyerEmployeeDetailStatusEffectiveDayID
		--	,BuyerEmployeeDetailStatusTerminationDatetime ,BuyerEmployeeDetailStatusTerminationDayID
		--	,ETLLoadID ,ETLLoadUpdateID)
		--SELECT Buyer_ID
		--	   ,Buyer_Employee_ID
		--	   ,Detail_Status
		--	   ,Detail_Status_Code
		--	   ,Detail_Status_Effective_Datetime
		--	   ,Detail_Status_Effective_DayID
		--	   ,Detail_Status_Expiration_Datetime
		--	   ,Detail_Status_Expiration_DayID
		--	   ,@ETLLoadID
		--	   ,@ETLLoadID AS ETLLoadUpdateID
		--FROM #Temp_Buyer_Inactive_Insert_Update T
		--WHERE NOT EXISTS 
		--	(SELECT 1 FROM dbo.DimBuyerEmployeeDetail DBED WITH (NOLOCK)
		--			WHERE T.Buyer_ID = DBED.BuyerID
		--					AND T.Buyer_Employee_ID = DBED.BuyerEmployeeID 
		--					AND T.Detail_Status_Code = DBED.BuyerEmployeeDetailStatusCode
		--					--AND T.Detail_Status_Effective_Datetime = DBED.BuyerEmployeeDetailStatusEffectiveDatetime
		--					AND T.Detail_Status_Effective_DayID = DBED.BuyerEmployeeDetailStatusEffectiveDayID
		--					--AND DBED.BuyerEmployeeDetailStatusTerminationDayID = 29991231
		--	 )

	
		
		----Updating/Inserting based on Buyer ACTIVE.
		
		----Update the Previous status Termination date with current status Effective Date.
		--UPDATE DBED SET	
		--	 BuyerEmployeeDetailStatusTerminationDatetime = T.Detail_Status_Effective_Datetime
		--	,BuyerEmployeeDetailStatusTerminationDayID = T.Detail_Status_Effective_DayID
		--	,ETLLoadUpdateID = @ETLLoadID
		----SELECT DBED.BuyerEmployeeID ,BuyerEmployeeDetailStatusTerminationDatetime ,T.Detail_Status_Effective_Datetime	
		--FROM dbo.DimBuyerEmployeeDetail DBED 
		--	INNER JOIN #Temp_Buyer_Active_Insert_Update T WITH(NOLOCK)
		--		ON DBED.BuyerEmployeeID = T.Buyer_Employee_ID --AND DBED.BuyerId = T.Buyer_ID
		--WHERE DBED.BuyerEmployeeDetailStatusCode IN ('ACT','PRO') 
		--		AND DBED.BuyerEmployeeDetailStatusEffectiveDatetime < T.Detail_Status_Effective_Datetime 
		--		AND DBED.BuyerEmployeeDetailStatusTerminationDayID = '29991231'		


		----Insert the New records.
		--INSERT INTO dbo.DimBuyerEmployeeDetail(BuyerId ,BuyerEmployeeID ,BuyerEmployeeDetailStatus ,BuyerEmployeeDetailStatusCode
		--	,BuyerEmployeeDetailStatusEffectiveDatetime ,BuyerEmployeeDetailStatusEffectiveDayID
		--	,BuyerEmployeeDetailStatusTerminationDatetime ,BuyerEmployeeDetailStatusTerminationDayID
		--	,ETLLoadID ,ETLLoadUpdateID)
		--SELECT Buyer_ID
		--	   ,Buyer_Employee_ID
		--	   ,Detail_Status
		--	   ,Detail_Status_Code
		--	   ,Detail_Status_Effective_Datetime
		--	   ,Detail_Status_Effective_DayID
		--	   ,Detail_Status_Expiration_Datetime
		--	   ,Detail_Status_Expiration_DayID
		--	   ,@ETLLoadID
		--	   ,@ETLLoadID AS ETLLoadUpdateID
		--FROM #Temp_Buyer_Active_Insert_Update T 
		--WHERE NOT EXISTS 
		--	(SELECT 1 FROM dbo.DimBuyerEmployeeDetail DBED WITH (NOLOCK)
		--			WHERE T.Buyer_ID = DBED.BuyerID
		--					AND T.Buyer_Employee_ID = DBED.BuyerEmployeeID 
		--					AND T.Detail_Status_Code = DBED.BuyerEmployeeDetailStatusCode
		--					--AND T.Detail_Status_Effective_Datetime = DBED.BuyerEmployeeDetailStatusEffectiveDatetime
		--					AND T.Detail_Status_Effective_DayID = DBED.BuyerEmployeeDetailStatusEffectiveDayID
		--					--AND DBED.BuyerEmployeeDetailStatusTerminationDayID = 29991231
		--	 )
		
COMMIT TRAN

SET @InsertRowCount = NULL
SET @UpdateRowCount = NULL 	

EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount 
		
		IF OBJECT_ID('tempdb..#Temp_Buyer_Employee_Audit_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Employee_Audit_Data
		IF OBJECT_ID('tempdb..#Temp_Buyer_Employee_Audit_Data_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Employee_Audit_Data_Update
		--IF OBJECT_ID('tempdb..#Temp_Buyer_Status_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Status_Data
		--IF OBJECT_ID('tempdb..#Temp_Buyer_Inactive_Insert_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Inactive_Insert_Update
		--IF OBJECT_ID('tempdb..#Temp_Buyer_Inactive_Insert_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Inactive_Insert_Update


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
		
		IF OBJECT_ID('tempdb..#Temp_Buyer_Employee_Audit_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Employee_Audit_Data
		IF OBJECT_ID('tempdb..#Temp_Buyer_Employee_Audit_Data_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Employee_Audit_Data_Update
		--IF OBJECT_ID('tempdb..#Temp_Buyer_Status_Data') IS NOT NULL DROP TABLE #Temp_Buyer_Status_Data
		--IF OBJECT_ID('tempdb..#Temp_Buyer_Inactive_Insert_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Inactive_Insert_Update
		--IF OBJECT_ID('tempdb..#Temp_Buyer_Inactive_Insert_Update') IS NOT NULL DROP TABLE #Temp_Buyer_Inactive_Insert_Update



		RETURN -1
	

END CATCH

END


GO


