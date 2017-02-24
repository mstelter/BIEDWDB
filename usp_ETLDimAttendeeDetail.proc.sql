/*
USE EDW
GO 
==============================================================================
Author		: Matt Stelter	 
Create date	: 09/17/2011
Description	: Populate the EDW DimAttendeeDetail table 
==============================================================================
REVISION History
ChangeDate	     Developer	    	Release/Problem Number
10/1/2012        Somesh V           Removed reference to AuctionLog Database
12/19/2012		Matt S				Changed code to eliminate duplicates & fix for Location
9/30/2012		Matt S				Changed reference to PBI-DB3..User_Info
6/8/2016		Matt S				Added IPAddress (Location), Flavor, LaunchType
==============================================================================
Usage Example:
Exec [usp_ETLDimAttendeeDetail]
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimAttendeeDetail]
AS
  DECLARE @ExtractRowCount_sp  INT,
          @ETLLoadID_sp        INT,
          @InsertRowCount_sp   INT,
          @UpdateRowCount_sp   INT,
          @CurrentDatetime     DATETIME,
          @DuplicateLogout     INT,
          @DuplicateLogout_prv INT

	SET NOCOUNT ON;

	BEGIN TRY
      --Run the Audit Begin
      EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimAttendeeDetail'

      SET @CurrentDatetime = (SELECT ER.MaxDatetime FROM   dbo.ETLRun ER WITH (NOLOCK) WHERE  ETLJobName='DailyETLJob')

      /*******************************************************************************************/
      CREATE TABLE #ad
        (BranchNumber INT,
         Login_Day_Id INT)

      CREATE TABLE #LLAuctions
        (Source_Filename VARCHAR(400),
         BranchNumber    INT,
         DayID           INT)

      CREATE TABLE #AttendeeDetail
        (AttendeeDetailID INT,
         AutoReconnect    BIT,
         BuyerEmployeeID  INT,
         BranchNumber     INT,
         BuyerID          INT,
         LoginDatetime    DATETIME,
         LoginDayId       INT,
         LoginDuration    INT,
         LoginName        VARCHAR(50),
         LoginStatusType  CHAR(1),
         LogoutDatetime   DATETIME,
         LogoutDayId      INT,
         ProperLogout     BIT,
         AuctionID        INT,
         IPAddress        NVARCHAR(160),
         IPNumber         BIGINT,
         LocID            INT,
         DisplayLocation  NVARCHAR(100),
         LoginLogID       INT,
         DeviceType       NVARCHAR(50),
         AuctionLane      VARCHAR(1),
		 Location			NVARCHAR(50),
		 Flavor				NVARCHAR(50),
         LaunchType			NVARCHAR(50))

      CREATE TABLE #AttendeeDetailDUPS
        (cnt             INT NULL,
         BuyerEmployeeID INT NULL,
         BranchNumber    INT NULL,
         LoginDatetime   DATETIME NULL,
         LogoutDatetime  DATETIME NULL,
         MaxADDMID       INT NULL)

      CREATE TABLE #temp_RecordCount
        (ChangeType VARCHAR(10))

UPDATE EDW.dbo.BidLog SET BranchNumber = 116 where BranchNumber = 115 and TimeStamp > GETDATE()-3
UPDATE EDW.dbo.SoldLog SET BranchNumber = 116 where BranchNumber = 115 and TimeStamp > GETDATE()-3
UPDATE EDW.dbo.RapidBidLog SET BranchNumber = 116 where BranchNumber = 115 and TimeStamp > GETDATE()-3
UPDATE EDW.dbo.LoginLog SET BranchNumber = 116 where BranchNumber = 115 and LoginTime > GETDATE()-3
UPDATE EDW.dbo.BidLog SET BranchNumber = 761 where BranchNumber = 717 and TimeStamp > GETDATE()-3
UPDATE EDW.dbo.SoldLog SET BranchNumber = 761 where BranchNumber = 717 and TimeStamp > GETDATE()-3
UPDATE EDW.dbo.RapidBidLog SET BranchNumber = 761 where BranchNumber = 717 and TimeStamp > GETDATE()-3
UPDATE EDW.dbo.LoginLog SET BranchNumber = 761 where BranchNumber = 717 and LoginTime > GETDATE()-3

      /*****Get Non Processed LoginLog Auctions*****/
      INSERT INTO #ad
                  (BranchNumber,Login_Day_Id)
      (SELECT DISTINCT BranchNumber,
                       LoginDayId
       FROM   dbo.DimAttendeeDetail WITH (NOLOCK))

      INSERT INTO #LLAuctions
                  (Source_Filename,BranchNumber,DayID)
      SELECT DISTINCT ll.SourceFilename,
                      ll.BranchNumber AS BranchNumber,
                      CONVERT(VARCHAR(8), LoginTime, 112) AS DayID
      FROM   dbo.LoginLog ll WITH (NOLOCK)
             INNER JOIN dbo.DimBranch br WITH (NOLOCK)
               ON ll.BranchNumber=br.BranchNumber
             LEFT OUTER JOIN #ad AS ad WITH (NOLOCK)
               ON br.BranchNumber=ad.BranchNumber
                  AND CONVERT(VARCHAR(8), ll.LoginTime, 112)=ad.Login_Day_Id
      WHERE  ad.BranchNumber IS NULL
         AND ll.LoginTime>Dateadd(d, -10, @CurrentDatetime)

      -------------------------------------------------------------------
      ------------------------Get Data-----------------------------------
      -------------------------------------------------------------------
      INSERT INTO #AttendeeDetail
                  (AutoReconnect,BuyerEmployeeID,BranchNumber,BuyerID,LoginDatetime,LoginDayId,LoginName,LoginStatusType,LogoutDatetime,LogoutDayId,
                   ProperLogout,
                   LoginLogID,DeviceType,AuctionLane,[Location],[Flavor],[LaunchType])
      SELECT DISTINCT CASE
                        WHEN ll.AutoReconnect='Y' THEN 1
                        ELSE 0
                      END AS Auto_Reconnect,
                      BE.BuyerEmployeeID AS BuyerEmployeeID,
                      BR.BranchNumber AS BranchNumber,
                      BE.BuyerID AS BuyerID,
                      Dateadd(hh, -1, ll.LoginTime) AS Logindatetime,
                      CONVERT(NVARCHAR(8), Dateadd(hh, -1, ll.LoginTime), 112) AS LoginDayID
                      --,CASE WHEN BE.BuyerEmployeeID IS NULL 
                      --	THEN ll.name 
                      --	ELSE u.login END					
                      ,
                      ll.name AS Login_Name,
                      Upper(LEFT(ll.Screen, 1)) AS LoginStatusType,
                      Dateadd(hh, -1, ll.LogoutTime) AS LogoutDatetime,
                      CONVERT(NVARCHAR(8), Dateadd(hh, -1, ll.LogoutTime), 112) AS LogoutDayID,
                      CASE
                        WHEN Dateadd(hh, -1, ll.LogoutTime)<'2/4/1908' --AMS sets missing logout datetime to 2/3/1908
                      THEN 0
                        ELSE 1
                      END AS ProperLogout,
                      ll.LoginLogId AS LoginLogID,
                      ll.DeviceType AS DeviceType,
                      ll.AuctionLane AS AuctionLane,ll.[Location],ll.[Flavor],ll.[LaunchType]
      FROM   #LLAuctions lla WITH (NOLOCK)
             INNER JOIN dbo.LoginLog ll WITH (NOLOCK)
               ON lla.Source_Filename=ll.SourceFilename
             --	LEFT OUTER JOIN 
             --BI_ASAP_Rep.dbo.user_Info u WITH (NOLOCK)
             --	ON substring (ll.name, CHARINDEX( '_', ll.name)+1, len(ll.name)) = CONVERT(varchar(50),u.Employee_Id)
             LEFT OUTER JOIN dbo.DimBuyerEmployee BE WITH (NOLOCK)
               ON Substring (ll.name, Charindex('_', ll.name)+1, Len(ll.name))=CONVERT(VARCHAR(50), BE.BuyerEmployeeId)
             LEFT OUTER JOIN dbo.DimBranch BR WITH (NOLOCK)
               ON BR.BranchNumber=ll.BranchNumber
             LEFT OUTER JOIN dbo.DimAttendeeDetail ad WITH (NOLOCK)
               ON Isnull(BE.BuyerEmployeeID, 0)=Isnull(ad.BuyerEmployeeID, 0)
                  AND Isnull(BR.BranchNumber, 0)=Isnull(ad.BranchNumber, 0)
                  AND Isnull(ll.LoginTime, ' ')=Isnull(ad.Logindatetime, ' ')
                  AND Isnull(ll.LogoutTime, ' ')=Isnull(ad.Logoutdatetime, ' ')
                  AND Isnull(ll.AuctionLane, ' ')=Isnull(ad.AuctionLane, ' ')
      WHERE  ad.AttendeeDetailID IS NULL

      -------------------------------------------------------------------
      ------------------------Update IP Address--------------------------
      ------------------------REMOVED FOR EDW!---------------------------
      -------------------------------------------------------------------
      ------------------------Update IP_Number---------------------------
      ------------------------REMOVED FOR EDW!---------------------------
      -------------------------------------------------------------------
      --------------------Update Logout_DateTime-------------------------
      -------------------------------------------------------------------
      UPDATE #AttendeeDetail
      SET    LogoutDateTime=AuctionEndDateTime
      FROM   (SELECT auctsched.AuctionID,
                     AuctEnd.AuctionEndDateTime
              FROM   dbo.DimAuction auctsched WITH (NOLOCK)
                     INNER JOIN dbo.DimBranch br  WITH (NOLOCK)
                       ON auctsched.BranchNumber=br.BranchNumber
                     INNER JOIN (SELECT Max(TimeStamp) AS AuctionEndDateTime,
                                        BranchNumber,
                                        Cast(CONVERT(VARCHAR(20), TimeStamp, 112) AS INT) AS AuctionDayID
                                 FROM   dbo.BidLog WITH (NOLOCK)
                                 GROUP  BY BranchNumber,
                                           Cast(CONVERT(VARCHAR(20), TimeStamp, 112) AS INT)) AS AuctEnd
                       ON Cast(CONVERT(VARCHAR(8), auctsched.AuctionDateTime, 112) AS INT)=AuctEnd.AuctionDayID
                          AND br.BranchNumber=AuctEnd.BranchNumber) AS AuctionEnd
             INNER JOIN #AttendeeDetail ad WITH (NOLOCK)
               ON AuctionEnd.AuctionID=ad.AuctionID
      WHERE  LogoutDateTime IS NULL
          OR LogoutDateTime=0
          OR LogoutDateTime='1908-02-03 00:00:00.000'

      -------------------------------------------------------------------
      ----------If there was no auction then update to 5:00 pm-----------
      -------------------------------------------------------------------
      UPDATE #AttendeeDetail
      SET    LogoutDateTime=Cast(CONVERT(VARCHAR(20), LoginDateTime, 101)+' 17:00:00.000' AS DATETIME)
      WHERE  LogoutDateTime IS NULL
          OR LogoutDateTime=0
          OR LogoutDateTime='1908-02-03 00:00:00.000'

      -------------------------------------------------------------------
      ---For all logout times that fall between other login and logout---
      ----------times reset the logout time to the login time------------
      -------------------------------------------------------------------
      SELECT @DuplicateLogout=Isnull(Sum(cnt), 0)
      FROM   (SELECT Count(*) AS cnt,
                     BuyerEmployeeID,
                     BranchNumber,
                     LogoutDatetime
              FROM   #AttendeeDetail AS ad WITH (NOLOCK)
              GROUP  BY BuyerEmployeeID,
                        BranchNumber,
                        LogoutDatetime
              HAVING (Count(*)>1)
                     AND (NOT (BuyerEmployeeID IS NULL))) AS GetCount

      WHILE @DuplicateLogout>0 BEGIN
            UPDATE #AttendeeDetail
            SET    LogoutDatetime=Dateadd(ms, -300, NextLogin.LoginDT)
            FROM   #AttendeeDetail ad WITH (NOLOCK)
                   INNER JOIN (SELECT AD1.AttendeeDetailID,
                                      Min(AD2.LoginDatetime) AS LoginDT
                               FROM   (SELECT AttendeeDetailID,
                                              BranchNumber,
                                              AuctionID,
                                              BuyerEmployeeID,
                                              LoginDatetime,
                                              LogoutDatetime
                                       FROM   #AttendeeDetail WITH (NOLOCK)) AS AD1
                                      INNER JOIN (SELECT AttendeeDetailID,
                                                         BranchNumber,
                                                         AuctionID,
                                                         BuyerEmployeeID,
                                                         LoginDatetime,
                                                         LogoutDatetime
                                                  FROM   #AttendeeDetail WITH (NOLOCK)) AS AD2
                                        ON AD1.BranchNumber=AD2.BranchNumber
                                           AND AD1.BuyerEmployeeID=AD2.BuyerEmployeeID
                               WHERE  AD1.LoginDatetime<=AD2.LoginDatetime
                                  AND AD1.LogoutDatetime>=AD2.LoginDatetime
                                  AND AD1.LogoutDatetime<=AD2.LogoutDatetime
                                  AND AD1.AttendeeDetailID<>AD2.AttendeeDetailID
                               GROUP  BY AD1.AttendeeDetailID) AS NextLogin
                     ON ad.AttendeeDetailID=NextLogin.AttendeeDetailID

            SET @DuplicateLogout_prv = @DuplicateLogout

            SELECT @DuplicateLogout=Isnull(Sum(cnt), 0)
            FROM   (SELECT Count(*) AS cnt,
                           BuyerEmployeeID,
                           BranchNumber,
                           LogoutDatetime
                    FROM   #AttendeeDetail AS ad WITH (NOLOCK)
                    GROUP  BY BuyerEmployeeID,
                              BranchNumber,
                              LogoutDatetime
                    HAVING (Count(*)>1)
                           AND (NOT (BuyerEmployeeID IS NULL))) AS GetCount

            IF Isnull(@DuplicateLogout, 0)>=Isnull(@DuplicateLogout_prv, 0)
              BREAK
            ELSE
              CONTINUE
        END

      -------------------------------------------------------------------
      ---If the Logout Time < the Login Time set it to the Login Time----
      -------------------------------------------------------------------
      UPDATE #AttendeeDetail
      SET    LogoutDatetime=LoginDatetime
      WHERE  LogoutDatetime<LoginDatetime

      -------------------------------------------------------------------
      ---------------------Delete True Duplicate Logins------------------
      -------------------------------------------------------------------
      INSERT INTO #AttendeeDetailDUPS
                  (cnt,BuyerEmployeeID,BranchNumber,LoginDatetime,LogoutDatetime,MaxADDMID)
      SELECT Count(*) AS cnt,
             BuyerEmployeeID,
             BranchNumber,
             LoginDatetime,
             LogoutDatetime,
             Max(AttendeeDetailID) AS MaxADDMID
      FROM   #AttendeeDetail AS AD WITH (NOLOCK)
      GROUP  BY BuyerEmployeeID,
                BranchNumber,
                LogoutDatetime,
                LoginDatetime
      HAVING (Count(*)>1)
             AND (NOT (BuyerEmployeeID IS NULL))
      ORDER  BY cnt DESC

      DELETE FROM #AttendeeDetail
      FROM   #AttendeeDetail AS AD WITH (NOLOCK)
             INNER JOIN #AttendeeDetailDUPS AS ADDUPS WITH (NOLOCK)
               ON AD.BuyerEmployeeID=ADDUPS.BuyerEmployeeID
                  AND AD.BranchNumber=ADDUPS.BranchNumber
                  AND AD.LoginDatetime=ADDUPS.LoginDatetime
                  AND AD.LogoutDatetime=ADDUPS.LogoutDatetime
             LEFT OUTER JOIN #AttendeeDetailDUPS AS ADDUPS2 WITH (NOLOCK)
               ON AD.AttendeeDetailID=ADDUPS2.MaxADDMID
      WHERE  (ADDUPS2.MaxADDMID IS NULL)

      -------------------------------------------------------------------
      ----------------------Update Logout_Day_ID-------------------------
      -------------------------------------------------------------------
      UPDATE #AttendeeDetail
      SET    LogoutDayID=CONVERT(VARCHAR(8), LogoutDateTime, 112)
      WHERE  LogoutDayID IS NULL
          OR LogoutDayID=0
          OR LogoutDayID=19080203

      -------------------------------------------------------------------
      ----------------------Update Login_Duration-------------------------
      -------------------------------------------------------------------
      UPDATE #AttendeeDetail
      SET    LoginDuration=CASE
                             WHEN Datediff(mi, LoginDateTime, LogoutDateTime)=0 THEN 1
                             ELSE Datediff(mi, LoginDateTime, LogoutDateTime)
                           END
      WHERE  (LoginDuration IS NULL
               OR LoginDuration<=0
               OR LoginDuration>1440)
         AND LoginDateTime<LogoutDateTime

      -------------------------------------------------------------------
      ------------------Update Auction_Schedule_ID-----------------------
      -------------------------------------------------------------------
      UPDATE #AttendeeDetail
      SET    AuctionID=auctsched.AuctionID
      FROM   dbo.DimAuction auctsched WITH (NOLOCK)
             INNER JOIN dbo.DimBranch br WITH (NOLOCK)
               ON auctsched.BranchNumber=br.BranchNumber
             INNER JOIN #AttendeeDetail ad WITH (NOLOCK)
               ON CONVERT(VARCHAR(8), auctsched.AuctionDateTime, 112)=ad.LoginDayId
                  AND br.BranchNumber=ad.BranchNumber
      WHERE  ad.AuctionID IS NULL

      ---------------------------------------------------------------------
      ------------------Update Display Location--------------------
      ---------------------------------------------------------------------
      CREATE TABLE #DisplayLocation
        (displaylocation VARCHAR(50),
         LoginDayID      INT,
         attendeeid      VARCHAR(32),
         BranchNumber    VARCHAR(3))
      INSERT INTO #DisplayLocation
                  (displaylocation,LoginDayID,attendeeid,BranchNumber)
      SELECT DISTINCT displaylocation,
                      Cast(CONVERT(VARCHAR(20), cddate, 112) AS INT) AS LoginDayID,
                      attendeeid,
                      LEFT(RIGHT(facilityid, 4), 3) AS BranchNumber
      FROM   BI_Auction_Rep.dbo.amsattendeedata WITH (NOLOCK)
      WHERE  LEFT(RIGHT(facilityid, 4), 3)<>'est' and LEFT(RIGHT(facilityid, 4), 3)<>'wes'
         --AND cddate>Dateadd(d, -10, @UpdateLastRun)
         
      UPDATE ad
      SET    DisplayLocation=amsad.displaylocation
      --SELECT * 
      FROM #AttendeeDetail ad WITH (NOLOCK)
             --INNER JOIN EDW.dbo.DimBuyerEmployee b WITH (NOLOCK)
             --  ON ad.Bidder_DM_ID=b.Bidder_DM_ID
             --INNER JOIN EDW.dbo.DimBranch AS br WITH (NOLOCK)
             --  ON ad.BranchNumber=br.BranchNumber
             INNER JOIN #DisplayLocation AS amsad
               ON ad.LoginDayId=amsad.LoginDayId
                  AND ad.LoginName=amsad.attendeeid
                  AND ad.BranchNumber=amsad.BranchNumber
      WHERE  Isnull(ad.DisplayLocation, '')<>Isnull(amsad.displaylocation, '')
      
      UPDATE ad
      SET    DisplayLocation=Isnull(b.CityName, '')+'-'+Isnull(b.StateAbbreviation, 'XX')
      FROM   #AttendeeDetail ad WITH (NOLOCK)
             INNER JOIN EDW.dbo.DimBuyer b WITH (NOLOCK)
               ON ad.BuyerID=b.BuyerID
      WHERE  ad.DisplayLocation IS NULL 
      -------------------------------------------------------------------
      ----------------INSERT THE DATA FROM THE TEMP TABLE----------------
      -------------------------------------------------------------------
      BEGIN TRAN

      INSERT INTO dbo.DimAttendeeDetail
                  (AutoReconnect,BuyerEmployeeID,BranchNumber,BuyerID,LoginDatetime,LoginDayId,LoginDuration,LoginName,LoginStatusType,LogoutDatetime,
                   LogoutDayId,
                   ProperLogout,AuctionID,IPAddress,IPNumber,LocID,DisplayLocation,LoginLogID,DeviceType,AuctionLane,Flavor,LaunchType,ETLLoadID,ETLLoadUpdateID)
      SELECT tAD.AutoReconnect,
             tAD.BuyerEmployeeID,
             tAD.BranchNumber,
             tAD.BuyerID,
             tAD.LoginDatetime,
             tAD.LoginDayId,
             tAD.LoginDuration,
             tAD.LoginName,
             tAD.LoginStatusType,
             tAD.LogoutDatetime,
             tAD.LogoutDayId,
             tAD.ProperLogout,
             tAD.AuctionID,
             tAD.Location AS IPAddress,
             tAD.IPNumber,
             tAD.LocID,
             tAD.DisplayLocation,
             tAD.LoginLogID,
             tAD.DeviceType,
             tAD.AuctionLane,
			 tAD.[Flavor],
			 tAD.[LaunchType],
             @ETLLoadID_sp,
             @ETLLoadID_sp
      FROM   #AttendeeDetail tAD
      LEFT OUTER JOIN dbo.DimAttendeeDetail DAD WITH (NOLOCK)
		  ON tAD.AuctionID = DAD.AuctionID
		  AND TAD.AuctionLane = DAD.AuctionLane
		  AND TAD.BuyerEmployeeID = DAD.BuyerEmployeeID
		  AND TAD.LoginDatetime  = DAD.LoginDatetime
		  AND TAD.LogoutDatetime = DAD.LogoutDatetime
	WHERE DAD.AttendeeDetailID IS NULL	  

      SELECT @InsertRowCount_sp=Count(*)
      FROM   #temp_RecordCount
      WHERE  ChangeType='INSERT'

      COMMIT TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp

      IF Object_id('tempdb..#ad') IS NOT NULL
        DROP TABLE #ad

      IF Object_id('tempdb..#LLAuctions') IS NOT NULL
        DROP TABLE #LLAuctions

      IF Object_id('tempdb..#AttendeeDetail') IS NOT NULL
        DROP TABLE #AttendeeDetail

      IF Object_id('tempdb..#AttendeeDetailDUPS') IS NOT NULL
        DROP TABLE #AttendeeDetailDUPS

      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL
        DROP TABLE #temp_RecordCount

      RETURN 0
  END TRY

  BEGIN CATCH
      SET NOCOUNT OFF

      IF @@TRANCOUNT>0
        ROLLBACK TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp,@ErrorCode = @@ERROR

      DECLARE @ErrorMessage  NVARCHAR(4000),
              @ErrorSeverity INT,
              @ErrorState    INT;

      SELECT @ErrorMessage=Error_message(),
             @ErrorSeverity=Error_severity(),
             @ErrorState=Error_state();

      RAISERROR (@ErrorMessage,
                 @ErrorSeverity,
                 @ErrorState);

      IF Object_id('tempdb..#ad') IS NOT NULL
        DROP TABLE #ad

      IF Object_id('tempdb..#LLAuctions') IS NOT NULL
        DROP TABLE #LLAuctions

      IF Object_id('tempdb..#AttendeeDetail') IS NOT NULL
        DROP TABLE #AttendeeDetail

      IF Object_id('tempdb..#AttendeeDetailDUPS') IS NOT NULL
        DROP TABLE #AttendeeDetailDUPS

      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL
        DROP TABLE #temp_RecordCount

      RETURN -1
  END CATCH

