
/*
 USE EDW
GO 
==============================================================================
 Author:		Somesh Velupalli
 Create date:   07/26/2011
 Description:	Populate the DimBuyerDetail Table from CDC
 ===============================================================================
 Revision History
 ChangeDate	    Developer		   Release/Problem Number
 08/30/2012     Somesh V           Added Logic to run proc multiple times and not insert data
 2/20/2013   Matt Stelter	Updated BuyerStatusDescription
 
 
 
 ===============================================================================
 
 */
CREATE PROCEDURE [dbo].[usp_ETLDimBuyerDetail] 
	
AS
BEGIN

DECLARE @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		,@MinAllowableDate Datetime
		,@MaxAllowableDate Datetime
		
SET NOCOUNT ON;

BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimBuyerDetail'
		
--Get the Min and Max LSN's
SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK)WHERE ETLJobName = 'DailyETLJob_BI_ASAP_AUDIT_Rep'

--DECLARE @MinLSN Binary(10)
--		,@MaxLSN Binary (10)

--SELECT @MinLSN = BI_ASAP_Audit_Rep.sys.fn_cdc_get_min_lsn('dbo_Buyer_Audit')
--SELECT @MaxLsn = BI_ASAP_Audit_Rep.sys.fn_cdc_get_max_lsn()


CREATE TABLE #TMP_BuyerDetailStatusUpdate
(Buyer_ID INT NOT NULL 
,Status_Code [CHAR](3) NULL
,Update_Datetime DATETIME )
INSERT INTO #TMP_BuyerDetailStatusUpdate
(Buyer_ID,
 Status_Code,
 Update_Datetime)
SELECT BA.Buyer_ID,
       BA.Status_Code,
       BA.Update_DateTime --INTO #BuyerStatusUpdate 
FROM   BI_ASAP_Audit_Rep.dbo.Buyer_Audit BA WITH (NOLOCK)
       INNER JOIN (SELECT Buyer_ID,Max(Update_DateTime) AS MaxUpdateDateTime
                   FROM  [BI_ASAP_Audit_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Buyer_Audit] (@MinLSN, @MaxLSN, 'all with merge') 
                   WHERE  Audit_Action_Code='U' AND [__$operation] = 5  GROUP  BY Buyer_ID) MAXBA
               ON BA.Buyer_ID=MAXBA.Buyer_ID
               AND BA.Update_DateTime=MAXBA.MaxUpdateDateTime
        


--Insert all the new reocrds that have been inserted into ASAP Audit table
 
BEGIN TRAN  
 
INSERT INTO EDW.dbo.DimBuyerDetail 
(
  BuyerID
 ,BuyerDetailStatus
 ,BuyerDetailStatusCode
 ,BuyerDetailStatusEffectiveDatetime
 ,BuyerDetailStatusEffectiveDayID
 ,BuyerDetailStatusTerminationDatetime
 ,BuyerDetailStatusTerminationDayID
 ,ETLLoadID
 ,ETLLoadUpdateID
 )
 SELECT BA.Buyer_ID
       --,CASE
       -- WHEN BA.Status_Code IN ('ACT', 'PRO') THEN 'ACTIVE'
       -- WHEN BA.Status_Code IN ('ARH', 'BAN', 'INA', 'OUT') THEN 'INACTIVE'
       -- WHEN BA.Status_Code IN ('DUP') THEN 'DUPLICATE'
       -- ELSE 'INACTIVE'  END AS Buyer_Detail_Status
		,CASE 
			WHEN BA.Status_Code IN ('NEW') THEN 'NEW'
			WHEN BA.Status_Code IN ('ACT') THEN 'ACTIVE'
			WHEN BA.Status_Code IN ('PRO') THEN 'PROSPECTIVE'
			WHEN BA.Status_Code IN ('ARH') THEN 'ARCHIVED'
			WHEN BA.Status_Code IN ('BAN') THEN 'BANNED'
			WHEN BA.Status_Code IN ('INA') THEN 'INACTIVE'
			WHEN BA.Status_Code IN ('OUT') THEN 'OUT OF BUSINESS'
			WHEN BA.Status_Code IN ('DUP') THEN 'DUPLICATE' 
			ELSE 'INACTIVE' END AS Buyer_Detail_Status
       ,BA.Status_Code AS BuyerDetailStatusCode
       ,ISNULL(BA.Initial_Registration_Date, '12/31/2999') AS BuyerDetailStatusEffectiveDatetime
       ,CAST(CONVERT(CHAR(8), ISNULL(BA.Initial_Registration_Date, '12/31/2999'), 112) AS INT) AS BuyerDetailStatusEffectiveDayID
       ,'12/31/2999' AS BuyerDetailStatusTerminationDatetime
       ,29991231 AS BuyerDetailStatusTerminationDayID
       ,@ETLLoadID_sp
       ,@ETLLoadID_sp
      FROM  [BI_ASAP_Audit_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Buyer_Audit] (@MinLSN, @MaxLSN, 'all with merge') BA
                     INNER JOIN dbo.DimBuyer B WITH (NOLOCK)
                          ON BA.Buyer_ID=B.BuyerID 
                     LEFT OUTER JOIN dbo.DimBuyerDetail DBD WITH (NOLOCK)
                          ON B.BuyerID = DBD.BuyerID                      
      WHERE  [__$operation]=5 AND BA.Audit_Action_Code='I'  AND BA.Status_Code='NEW' 
             AND DBD.BuyerID IS NULL              



SELECT @ExtractRowCount_sp = COUNT(*) FROM  [BI_ASAP_Audit_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Buyer_Audit] (@MinLSN, @MaxLSN, 'all with merge') BA
                                 INNER JOIN EDW.dbo.DimBuyer B WITH (NOLOCK)
                                 ON BA.Buyer_ID=B.BuyerID
                                 WHERE  [__$operation]=5 AND   BA.Audit_Action_Code='I'  AND BA.Status_Code='NEW' 
                                 

SELECT @InsertRowCount_sp = COUNT(*) FROM  [BI_ASAP_Audit_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Buyer_Audit] (@MinLSN, @MaxLSN, 'all with merge') BA
                                 INNER JOIN EDW.dbo.DimBuyer B WITH (NOLOCK)
                                 ON BA.Buyer_ID=B.BuyerID
                                 WHERE  [__$operation]=5 AND   BA.Audit_Action_Code='I'  AND BA.Status_Code='NEW'                                  



--If Status has changed then mark the termination date

   
     
UPDATE BD SET  
-- SELECT BUyerID,BUyerDetailID,bd.BuyerDetailStatusCode,--B.BuyerStatusCode,
  BuyerDetailStatusTerminationDatetime=TBDSU.Update_DateTime,
  BuyerDetailStatusTerminationDayID = Cast(CONVERT(CHAR(8), TBDSU.Update_DateTime, 112) AS INT)
  --,ETLLoadupdateid = @ETLLoadID_sp
  FROM 
  dbo.DimBuyerDetail BD 
  INNER JOIN #TMP_BuyerDetailStatusUpdate TBDSU
   ON BD.BuyerID = TBDSU.Buyer_ID
  WHERE BD.BuyerDetailStatusCode <> TBDSU.Status_Code
     AND BuyerDetailStatusTerminationDayID = 29991231
      

  
SET @UpdateRowCount_sp = @@ROWCOUNT


---Insert the updated records

INSERT INTO EDW.[dbo].[DimBuyerDetail]
                  ([BuyerID],
                   [BuyerDetailStatus],
                   [BuyerDetailStatusCode],
                   [BuyerDetailStatusEffectiveDatetime],
                   [BuyerDetailStatusEffectiveDayID],
                   [BuyerDetailStatusTerminationDatetime],
                   [BuyerDetailStatusTerminationDayID],
                    ETLLoadID,
                    ETLLoadUpdateID)
      SELECT  DISTINCT TBSDU.Buyer_ID,
              CASE WHEN TBSDU.Status_Code IN ('ACT', 'PRO') THEN 'ACTIVE'
                   WHEN TBSDU.Status_Code IN ('ARH', 'BAN', 'INA', 'OUT') THEN 'INACTIVE'
                   WHEN TBSDU.Status_Code IN ('DUP') THEN 'DUPLICATE'  ELSE 'INACTIVE'    END AS Buyer_Detail_Status,
             TBSDU.Status_Code AS Buyer_Detail_Status_Code,
             TBSDU.Update_DateTime AS Buyer_Detail_Status_Effective_Datetime,
             Cast(CONVERT(CHAR(8), TBSDU.Update_DateTime, 112) AS INT) AS Buyer_Detail_Status_Effective_Day_ID,
             '12/31/2999' AS Buyer_Detail_Status_Termination_Datetime,
             29991231 AS Buyer_Detail_Status_Termination_Day_ID
             ,@ETLLoadID_sp
             ,@ETLLoadID_sp
		  FROM  #TMP_BuyerDetailStatusUpdate TBSDU 
		 WHERE NOT EXISTS 
		        (SELECT 1 FROM dbo.DimBuyerDetail DBD WITH (NOLOCK)
		            WHERE TBSDU.Buyer_ID = DBD.BuyerID
		              AND TBSDU.Status_Code = DBD.BuyerDetailStatusCode
		              AND Cast(CONVERT(CHAR(8), TBSDU.Update_DateTime, 112) AS INT) = DBD.BuyerDetailStatusEffectiveDayID
		              AND DBD.BuyerDetailStatusTerminationDayID = 29991231		        
		        
		        )
		 
      
    
    COMMIT TRAN 
  
 
 
EXEC [dbo].[usp_ETLLoadAudit]
	   @StartEnd = 'E',
	   @ETLLoadID = @ETLLoadID_sp,
	   @ExtractRowCount = @ExtractRowCount_sp,
	   @InsertRowCount = @InsertRowCount_sp,
	   @UpdateRowCount = @UpdateRowCount_sp

IF OBJECT_ID('tempdb..#BuyerDetailStatusUpdate') IS NOT NULL DROP TABLE #BuyerDetailStatusUpdate
IF OBJECT_ID('tempdb..#BuyersToUpdate') IS NOT NULL DROP TABLE #BuyersToUpdate
IF OBJECT_ID('tempdb..#RecordsToInsert') IS NOT NULL DROP TABLE #RecordsToInsert



   RETURN 0
END TRY

BEGIN CATCH

  SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN

		EXEC [dbo].[usp_ETLLoadAudit]
				@StartEnd = 'E',
				@ETLLoadID = @ETLLoadID_sp,
				@ExtractRowCount = @ExtractRowCount_sp,
				@InsertRowCount = @InsertRowCount_sp, 
				@UpdateRowCount = @UpdateRowCount_sp,
				@ErrorCode = @@ERROR
				
				DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
				SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
				RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
				
				IF OBJECT_ID('tempdb..#BuyerDetailStatusUpdate') IS NOT NULL DROP TABLE #BuyerDetailStatusUpdate
				IF OBJECT_ID('tempdb..#BuyersToUpdate') IS NOT NULL DROP TABLE #BuyersToUpdate
				IF OBJECT_ID('tempdb..#RecordsToInsert') IS NOT NULL DROP TABLE #RecordsToInsert
				
				RETURN -1
	 
 END CATCH

END
GO


