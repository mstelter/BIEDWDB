/*
 USE EDW
GO 
=========================================================================
 Author:	  Somesh Velupalli 
 Create date: 08/25/2012
 Description: Populate the FactBuyerRegistration Table Everyday
 =========================================================================
 REVISION History
 Date        Developer        Comments
 09/06/2012  Somesh V         Added logic for NewBidders(Pulls From BuyerEmplyeeDetail)
 09/10/2012  Somesh V         Added Inner join with DimbuyeremployeeDetail to show records in DimbuyerEmployeeDetail
 09/13/2012  mstelter         Changed Lost and > 181 days to between 181 and 210 days
 10/11/2012	 MSTELTER	      Added Renewed Bidders
 12/05/2013  Juan X. Jacome	  Added logic to parsing of BuyerEmployeeID from Reference Text
 08/11/2016  Mohammad Waheed  Implemented NewAndPaidBidders, User Story 137409:Add New Registration Fee Paid to Fact Buyer Registration 
  ==========================================================================
 Usage Example
 EXEC [usp_ETLFactBuyerRegistration]
 ==========================================================================
 */

CREATE PROCEDURE [dbo].[usp_ETLFactBuyerRegistration]

AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY 
    DECLARE  @IntErrorCode INT
            ,@IntRecordCount INT
            ,@ETLLoadID int
            ,@CurrentDateTime DateTime
            ,@CurrentDayId int
            ,@MonthID INT 
            ,@InsertRowCount_sp int 
            ,@UpdateRowCount int
            ,@ExtractRowCount int

SET @CurrentDateTime =  
(SELECT  CASE WHEN DATEPART(HH,MaxDateTime) Between 0 and 14 
      THEN DATEADD(HOUR,23,CONVERT(DATETIME,CONVERT(VARCHAR(8), MaxDatetime - 1,112),114)) 
      ELSE MaxDateTime END 
 FROM dbo.ETLRun WITH (NOLOCK)WHERE ETLJobName = 'DailyETLJob')     
      
SET @CurrentDayId = CONVERT(VARCHAR(8), @CurrentDateTime,112)

SET @MonthID = CONVERT(VARCHAR(6), DATEADD(MONTH, 0, @CurrentDateTime), 112)

--Run the Audit begin                 
EXEC @ETLLoadID = [dbo].[usp_ETLLoadAudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactBuyerRegistration'

--IF OBJECT_ID('tempdb..#TMP_FBRTRecordCount') IS NOT NULL DROP TABLE #TMP_FBRTRecordCount
CREATE TABLE #TMP_FBRTRecordCount
(ChangeType VARCHAR(30))

--Tempe Table to Calculate all the Measures
--IF OBJECT_ID('tempdb..#TMP_RegistrationMeasures') IS NOT NULL DROP TABLE #TMP_RegistrationMeasures
CREATE TABLE #TMP_RegistrationMeasures
(BuyerID INT NOT NULL,
 DayID INT NOT NULL,
 MonthID INT ,
 ActiveBidders INT ,
 NewBidders INT ,
 LostBidders INT,
 RenewedBidders INT,
 BiddersExpired0to30Days INT,
 BiddersExpired31to60Days INT,
 BiddersExpired61to90Days INT,
 BiddersExpired91to120Days INT,
 BiddersExpired121to180Days INT,
 BiddersExpiredover181Days   INT,
 NewAndPaidBidders INT)


CREATE TABLE #TMP_NewBidders
(BuyerID INT NOT NULL,
 DayID INT NOT NULL,
 MonthID INT NOT NULL,
 NewBidders INT NOT NULL) 


 CREATE TABLE #TMP_NewAndPaidBidders
(BuyerID INT NOT NULL,
 DayID INT NOT NULL,
 MonthID INT NOT NULL,
 NewAndPaidBidders INT NOT NULL) 
 

INSERT INTO #TMP_RegistrationMeasures
(BuyerID,
 DayID,
 MonthID,
 ActiveBidders,
 --LostBidders,
 --RenewedBidders,
 BiddersExpired0to30Days,
 BiddersExpired31to60Days,
 BiddersExpired61to90Days,
 BiddersExpired91to120Days,
 BiddersExpired121to180Days,
 BiddersExpiredover181Days)
 SELECT 
 DBE.BuyerID
 ,@CurrentDayId
 ,@MonthID
 ,SUM(CASE WHEN CurrentStatusCode IN ('ACT','PRO') OR (CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) < 181 ) THEN 1 ELSE 0 END) AS ActiveBidders
 --,SUM(CASE WHEN DATEPART(MM,RegistrationRenewalDateTime) = DATEPART(MM,@CurrentDateTime) AND  DATEPART(YEAR,RegistrationRenewalDateTime) = DATEPART(YEAR,@CurrentDateTime)  THEN 1 ELSE 0 END) AS RenewedBidders
 ,SUM(CASE WHEN CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) BETWEEN  0 AND 30 THEN 1 ELSE 0 END) AS CountOfBiddersExpired0to30Days
 ,SUM(CASE WHEN CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS CountOfBiddersExpired31to60Days
 ,SUM(CASE WHEN CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) BETWEEN 61 AND 90 THEN 1 ELSE 0 END) AS CountOfBiddersExpired61to90Days
 ,SUM(CASE WHEN CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) BETWEEN 91 AND 120 THEN 1 ELSE 0 END) AS CountOfBiddersExpired91to120Days
 ,SUM(CASE WHEN CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) BETWEEN 121 AND 180 THEN 1 ELSE 0 END) AS CountOfBiddersExpired121to180Days
 --,SUM(CASE WHEN CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) > 181 THEN 1 ELSE 0 END) AS CountOfBiddersExpiredOver181Days
 ,SUM(CASE WHEN CurrentStatusCode IN ('ARH', 'BAN', 'INA', 'OUT') AND DATEDIFF(DD,CurrentStatusEffectiveDateTime,@CurrentDateTime) BETWEEN 181 AND 210 THEN 1 ELSE 0 END) AS CountOfBiddersExpiredOver181Days
 FROM 
 dbo.DimBuyerEmployee DBE WITH (NOLOCK)
 INNER JOIN (SELECT DISTINCT BuyerEmployeeID FROM dbo.DimBuyerEmployeeDetail WITH (NOLOCK)) DBED --(Join to show records in Dimbuyer Employee Detail only)
 ON DBE.BuyerEmployeeID = DBED.BuyerEmployeeID
 GROUP BY 
 DBE.BuyerID
 
 INSERT INTO #TMP_NewBidders
 (BuyerID,DayID,MonthID,NewBidders)
  SELECT 
 BuyerId,
 @CurrentDayId,
 @MonthID,
 SUM(CASE WHEN BuyerEmployeeDetailStatus IN ('NEW') AND DATEPART(MM,BuyerEmployeeDetailStatusEffectiveDatetime)  = DATEPART(MM,@CurrentDateTime) AND  DATEPART(YYYY,BuyerEmployeeDetailStatusEffectiveDatetime)  = DATEPART(YYYY,@CurrentDateTime)  THEN 1 ELSE 0 END) AS NewBidders
 FROM 
 dbo.DimBuyerEmployeeDetail
 WHERE BuyerEmployeeDetailStatusCode = 'New'
 GROUP BY BuyerId

 Insert into #TMP_NewAndPaidBidders
 (BuyerID,DayID,MonthID,NewAndPaidBidders)
 select bed.BuyerId,
 @CurrentDayId,
 @MonthID,
 SUM(CASE WHEN  (DATEPART(MM,InitialSetupDateTime)  = DATEPART(MM,@CurrentDateTime) AND  DATEPART(YYYY,InitialSetupDateTime)  = DATEPART(YYYY,@CurrentDateTime))
			And	 (DATEPART(MM,CurrentStatusEffectiveDateTime)  = DATEPART(MM,@CurrentDateTime) AND  DATEPART(YYYY,CurrentStatusEffectiveDateTime)  = DATEPART(YYYY,@CurrentDateTime))
		   THEN 1 ELSE 0 END) AS NewAndRegistrationFeePaid
 FROM  dbo.DimBuyerEmployee bed With (nolock)
 where CASE WHEN  (DATEPART(MM,InitialSetupDateTime)  = DATEPART(MM,@CurrentDateTime) AND  DATEPART(YYYY,InitialSetupDateTime)  = DATEPART(YYYY,@CurrentDateTime))
			And	 (DATEPART(MM,CurrentStatusEffectiveDateTime)  = DATEPART(MM,@CurrentDateTime) AND  DATEPART(YYYY,CurrentStatusEffectiveDateTime)  = DATEPART(YYYY,@CurrentDateTime))  THEN 1 ELSE 0  end>0
			and CurrentStatusDescription <> 'NEW'
 group by bed.BuyerId

 
 UPDATE T SET 
 T.NewBidders = TN.NewBidders 
 FROM 
 #TMP_RegistrationMeasures T 
 INNER JOIN #TMP_NewBidders TN 
 ON T.BuyerID = TN.BuyerID


  UPDATE T SET 
 T.NewAndPaidBidders = TN.NewAndPaidBidders 
 FROM 
 #TMP_RegistrationMeasures T 
 INNER JOIN #TMP_NewAndPaidBidders TN 
 ON T.BuyerID = TN.BuyerID
 
 /****************************************************************************/
 /********************Renewed Bidders*****************************************/
  /****************************************************************************/
--UPDATE FBR
--SET    RenewedBidders=BuyerEmployeeRenewals
--FROM   dbo.FactBuyerRegistration FBR
--INNER JOIN (SELECT BuyerID, @CurrentDayId AS DayID, @MonthID AS MonthID, Count(*) AS BuyerEmployeeRenewals
--			FROM   (SELECT DISTINCT BuyerId, BuyerEmployeeID
--					FROM   (SELECT CONVERT(INT, LEFT(Reference_Text, Charindex(':', Reference_Text) - 1)) AS Buyer_Employee_ID, Charge_DateTime, Buyer_ID --*
--							FROM   bi_asap_rep.[dbo].[buyer_charge] FFT WITH (NOLOCK)
--							WHERE  Charge_Type_ID IN (29,30)
--								AND Status_Code NOT IN ('vod')
--								AND Datepart(MM, Charge_DateTime)=Datepart(MM, @CurrentDateTime)
--								AND Datepart(YYYY, Charge_DateTime)=Datepart(YYYY, @CurrentDateTime)
--								AND Charindex(':', Reference_Text)>0) BER 
--					LEFT OUTER JOIN [dbo].[DimBuyerEmployeeDetail] BED WITH (NOLOCK)
--					ON BED.[BuyerEmployeeID]=BER.Buyer_Employee_ID
--					WHERE  BuyerEmployeeDetailStatusEffectiveDatetime BETWEEN Dateadd(YY, -1, BER.Charge_DateTime) AND Charge_DateTime
--						AND BED.BuyerEmployeeDetailStatus='Active') BuyerEmployeeRenewed
--					GROUP  BY BuyerId) AS BiddersRenewed
--ON FBR.BuyerID=BiddersRenewed.BuyerId AND FBR.MonthID=BiddersRenewed.MonthID
 
UPDATE FBR
SET    RenewedBidders=BuyerEmployeeRenewals
FROM   #TMP_RegistrationMeasures FBR
INNER JOIN (SELECT BuyerID, @CurrentDayId AS DayID, @MonthID AS MonthID, Count(*) AS BuyerEmployeeRenewals
			FROM   (
			
			SELECT DISTINCT BE.BuyerId, BE.BuyerEmployeeID
					FROM   (
					
					SELECT TRY_CONVERT(INT, LEFT(Reference_Text, Charindex(':', Reference_Text) - 1)) AS Buyer_Employee_ID, Charge_DateTime, Buyer_ID --*
							FROM   bi_asap_rep.[dbo].[buyer_charge] FFT WITH (NOLOCK)
							WHERE  Charge_Type_ID IN (29,30)
								AND Status_Code NOT IN ('vod')
								AND Datepart(MM, Charge_DateTime)=Datepart(MM, @CurrentDateTime)
								AND Datepart(YYYY, Charge_DateTime)=Datepart(YYYY, @CurrentDateTime)
								AND Charindex(':', Reference_Text)>0
								
								) BER 
					INNER JOIN [dbo].[DimBuyerEmployeeDetail] BED WITH (NOLOCK)
					ON BED.[BuyerEmployeeID]=BER.Buyer_Employee_ID
					INNER JOIN [dbo].[DimBuyerEmployee] BE WITH (NOLOCK)
					ON BE.[BuyerEmployeeID] = BER.Buyer_Employee_ID
					WHERE  BuyerEmployeeDetailStatusEffectiveDatetime BETWEEN Dateadd(YY, -1, BER.Charge_DateTime) AND Charge_DateTime
						AND BED.BuyerEmployeeDetailStatus <> 'New' 
						--AND BE.CurrentStatusCode = 'ACT'
						
						
						) BuyerEmployeeRenewed
					GROUP  BY BuyerId) AS BiddersRenewed
ON FBR.BuyerID=BiddersRenewed.BuyerId AND FBR.MonthID=BiddersRenewed.MonthID

----Merge and update

BEGIN TRANSACTION


MERGE dbo.FactBuyerRegistration FBR
USING #TMP_RegistrationMeasures  TMPR
ON  FBR.BuyerID = TMPR.BuyerID
AND FBR.MOnthID = TMPR.MonthID
WHEN MATCHED THEN 
UPDATE SET 
BUYERID  =  TMPR.BuyerID 
,DayID = TMPR.DayID 
,MonthID = TMPR.MonthID
,ActiveBidders = ISNULL(TMPR.ActiveBidders,0)
,NewBidders = ISNULL(TMPR.NewBidders,0)
,LostBidders = ISNULL(TMPR.BiddersExpiredover181Days,0)
,RenewedBidders = ISNULL(TMPR.RenewedBidders,0)
,CountOfBiddersExpired0to30Days = ISNULL(TMPR.BiddersExpired0to30Days,0)
,CountOfBiddersExpired31to60Days = ISNULL(TMPR.BiddersExpired31to60Days,0)
,CountOfBiddersExpired61to90Days = ISNULL(TMPR.BiddersExpired61to90Days,0)
,CountOfBiddersExpired91to120Days = ISNULL(TMPR.BiddersExpired91to120Days,0)
,CountOfBiddersExpired121to180Days = ISNULL(TMPR.BiddersExpired121to180Days,0)
,CountOfBiddersExpiredover181Days = ISNULL(TMPR.BiddersExpiredover181Days,0)
,ETLLoadUpdateID = @ETLLoadID
,NewAndPaidBidders = ISNULL(TMPR.NewAndPaidBidders,0)

WHEN NOT MATCHED THEN 
INSERT 
(BuyerID, 
 DayID,
 MonthID,
 ActiveBidders, 
 NewBidders,
 LostBidders, 
 RenewedBidders, 
 CountofBiddersExpired0to30Days, 
 CountofBiddersExpired31to60Days, 
 CountofBiddersExpired61to90Days,
 CountofBiddersExpired91to120Days,
 CountofBiddersExpired121to180Days,
 CountofBiddersExpiredover181Days,ETLLoadID,ETLLoadUPdateID,NewAndPaidBidders)
 VALUES ( BuyerID,
 DayID,
 MonthID,
 ISNULL(ActiveBidders,0)
 ,ISNULL(NewBidders,0)
 ,ISNULL(TMPR.BiddersExpiredover181Days,0)
 ,ISNULL(RenewedBidders,0)
 ,ISNULL(BiddersExpired0to30Days,0)
 ,ISNULL(BiddersExpired31to60Days,0)
 ,ISNULL(BiddersExpired61to90Days,0)
 ,ISNULL(BiddersExpired91to120Days,0)
 ,ISNULL(BiddersExpired121to180Days,0)
 ,ISNULL(BiddersExpiredover181Days,0)
 ,@ETLLoadID
 ,@ETLLoadID
 ,ISNULL(TMPR.NewAndPaidBidders,0)
 )
OUTPUT $action
INTO #TMP_FBRTRecordCount;

 COMMIT TRAN 
 
        SELECT @InsertRowCount_sp = Count(*) FROM   #TMP_FBRTRecordCount WHERE  ChangeType='INSERT'
          
        EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',
									    @ETLLoadID = @ETLLoadID,
									    @ExtractRowCount =@ExtractRowCount  ,
									    @InsertRowCount =@InsertRowCount_sp,
									    @UpdateRowCount = @UpdateRowCount
							          
		IF OBJECT_ID('tempdb..#TMP_FBRTRecordCount') IS NOT NULL DROP TABLE #TMP_FBRTRecordCount
        IF OBJECT_ID('tempdb..#TMP_RegistrationMeasures') IS NOT NULL DROP TABLE #TMP_RegistrationMeasures
		IF OBJECT_ID('tempdb..#TMP_NewAndPaidBidders') IS NOT NULL DROP TABLE #TMP_NewAndPaidBidders	  
		       
        RETURN 0 
        
END TRY 
BEGIN CATCH 
 
       IF @@TRANCOUNT>0
       ROLLBACK TRAN         
       EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',
                                     @ETLLoadID = @ETLLoadID,
                                     @ExtractRowCount = @ExtractRowCount,
                                     @InsertRowCount = @InsertRowCount_sp,
                                     @UpdateRowCount = @UpdateRowCount,
                                     @ErrorCode = @@ERROR     
              

          DECLARE @ErrorMessage  NVARCHAR(4000),
                  @ErrorSeverity INT,
                  @ErrorState    INT;

          SELECT @ErrorMessage=Error_message(),
                 @ErrorSeverity=Error_severity(),
                 @ErrorState=Error_state();

          RAISERROR (@ErrorMessage,
                     @ErrorSeverity,
                     @ErrorState);

		IF OBJECT_ID('tempdb..#TMP_FBRTRecordCount') IS NOT NULL DROP TABLE #TMP_FBRTRecordCount
        IF OBJECT_ID('tempdb..#TMP_RegistrationMeasures') IS NOT NULL DROP TABLE #TMP_RegistrationMeasures
		IF OBJECT_ID('tempdb..#TMP_NewAndPaidBidders') IS NOT NULL DROP TABLE #TMP_NewAndPaidBidders

      Return -1

END CATCH

END;        
GO
