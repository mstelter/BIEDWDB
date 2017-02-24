/* 
USE EDW
GO 
==============================================================================
Author		: Venkata Vempali
Create date	: 08/07/2012
Description	: Populate the CSATAutoMinimumBidUpdateHistoryNew table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	

==============================================================================
Usage Example:
Exec usp_ETLCSATAutoMinimumBidUpdateHistoryNew
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLCSATAutoMinimumBidUpdateHistoryNew]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLCSATAutoMinimumBidUpdateHistoryNew'

--Get the start and end LSNs
SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob_BI_CSA_REP'


/*********************************************************************************************
Create the temp table to consolidate the Current and Static Storage Locations
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_CSAT_MinimumBid') IS NOT NULL DROP TABLE #TMP_CSAT_MinimumBid
CREATE TABLE #TMP_CSAT_MinimumBid(
	[csat_Auto_MinimumBid_Update_History_ID] [int] NOT NULL,
	[Salvage_ID] [int] NOT NULL,
	[ASV_Damage_Group] [varchar](50) NULL,
	[Run_Drive] [bit] NULL,
	[ASAP_Minimum_Bid_Amount] [decimal](18, 0) NULL,
	[Percent_of_ASV] [decimal](5, 2) NULL,
	[Updated_Minimim_Bid] [decimal](18, 0) NULL,
	[cddate] [datetime] NOT NULL,
	[cduser] [varchar](50) NULL)

INSERT INTO #TMP_CSAT_MinimumBid

SELECT csat_Auto_MinimumBid_Update_ID ,Salvage_ID ,ASV_Damage_Group ,Run_Drive ,ASAP_Minimum_Bid_Amount 
	, Percent_of_ASV ,Updated_Minimim_Bid ,cddate ,cduser 
FROM [BI_CSA_REP].[cdc].[fn_cdc_get_net_changes_dbo_csat_Auto_MinimumBid_Update](
   @MinLSN,@MaxLSN,'all') 
WHERE [__$operation] = 2

UNION ALL

SELECT csat_Auto_MinimumBid_Update_History_ID ,Salvage_ID ,ASV_Damage_Group ,Run_Drive ,ASAP_Minimum_Bid_Amount
	, Percent_of_ASV ,Updated_Minimim_Bid, cddate, cduser
FROM [BI_CSA_REP].[cdc].[fn_cdc_get_net_changes_dbo_csat_Auto_MinimumBid_Update_History](
   @MinLSN,@MaxLSN,'all') 
WHERE [__$operation] = 2

UNION ALL

SELECT csat_Auto_MinimumBid_Update_History_ID ,Salvage_ID ,ASV_Damage_Group ,Run_Drive ,ASAP_Minimum_Bid_Amount
	, Percent_of_ASV ,Updated_Minimim_Bid ,cddate, cduser 
FROM [BI_CSA_REP].[cdc].[fn_cdc_get_net_changes_dbo_csat_Auto_MinimumBid_Update_History_New](
   @MinLSN,@MaxLSN,'all')  
WHERE [__$operation] = 2

UNION ALL
                                                  
SELECT csat_Auto_MinimumBid_Update_ID ,Salvage_ID ,ASV_Damage_Group ,Run_Drive ,ASAP_Minimum_Bid_Amount
	, Percent_of_ASV ,Updated_Minimim_Bid, cddate, cduser
FROM [BI_CSA_REP].[cdc].[fn_cdc_get_net_changes_dbo_csat_Auto_MinimumBid_Update_New](
   @MinLSN,@MaxLSN,'all')
WHERE [__$operation] = 2



SELECT @ExtractRowCount = COUNT(*) FROM #TMP_CSAT_MinimumBid

/*********************************************************************************************
INSERT the data from the temp table into the CSATAutoMinimumBidUpdateHistoryNew table
This does Insert only(New Records).  No Deletes & Updates.
*********************************************************************************************/

BEGIN TRANSACTION 

	INSERT INTO [dbo].[CSATAutoMinimumBidUpdateHistoryNew](
		CSATAutoMinimumBidUpdateHistoryID ,ASAPSalvageID ,CSATASVDamageGroup ,CSATRunDrive ,ASAPMinimumBidAmount 
		, CSATPercentofASV ,CSATUpdatedMinimimBid ,CDdate ,CDuser ,ETLLoadID ,ETLLoadUpdateID)
	SELECT TCSAT.csat_Auto_MinimumBid_Update_History_ID ,TCSAT.Salvage_ID ,TCSAT.ASV_Damage_Group ,TCSAT.Run_Drive 
		,TCSAT.ASAP_Minimum_Bid_Amount ,TCSAT.Percent_of_ASV ,TCSAT.Updated_Minimim_Bid ,TCSAT.cddate ,TCSAT.cduser 
		,@ETLLoadID ,@ETLLoadID
	FROM #TMP_CSAT_MinimumBid TCSAT
		LEFT OUTER JOIN dbo.CSATAutoMinimumBidUpdateHistoryNew CSAT WITH(NOLOCK)
			ON TCSAT.csat_Auto_MinimumBid_Update_History_ID = CSAT.CSATAutoMinimumBidUpdateHistoryID
					AND TCSAT.Salvage_ID = CSAT.ASAPSalvageID AND TCSAT.cddate = CSAT.CDDate
	WHERE CSAT.CSATAutoMinimumBidUpdateHistoryID IS NULL     


COMMIT TRAN

SET @InsertRowCount = @@ROWCOUNT
SET @UpdateRowCount = NULL 	

EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount 
		
		IF OBJECT_ID('tempdb..#TMP_CSAT_MinimumBid') IS NOT NULL DROP TABLE #TMP_CSAT_MinimumBid

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
		
		IF OBJECT_ID('tempdb..#TMP_CSAT_MinimumBid') IS NOT NULL DROP TABLE #TMP_CSAT_MinimumBid


		RETURN -1
	

END CATCH

END


