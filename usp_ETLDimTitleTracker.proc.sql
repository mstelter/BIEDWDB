/****** Object:  StoredProcedure [dbo].[usp_ETLDimRapidBidDetail]    
    USE EDW
    GO 
    ==============================================================================
    Author		: Matt Stelter 
    Create date	: 02/19/2013
    Description	: Populate the EDW DimTitleTracker table 
    ==============================================================================
    REVISION History
    ChangeDate	     Developer	    	Release/Problem Number
    ==============================================================================
    Usage Example:
    Exec usp_ETLDimTitleTracker
    ==============================================================================
    */
CREATE PROCEDURE [dbo].[usp_ETLDimTitleTracker]
AS
  BEGIN
      DECLARE @ExtractRowCount_sp INT,
              @ETLLoadID_sp       INT,
              @InsertRowCount_sp  INT,
              @UpdateRowCount_sp  INT,
              @CurrentDatetime    DATETIME

      SET NOCOUNT ON;

      BEGIN TRY
          --Run the Audit Begin
          EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimRapiBidDetail'

          SET @CurrentDatetime = (SELECT ER.MaxDatetime
                                  FROM   dbo.ETLRun ER WITH (NOLOCK)
                                  WHERE  ETLJobName='DailyETLJob')

          /**************************************************CREATE TEMP TABLES***********************************************************/
          CREATE TABLE #DimTitleTracker
            (TitleTrackerId INT)

          IF Object_id('tempdb..#tmp_RecordCount') IS NOT NULL
            DROP TABLE #tmp_RecordCount

          CREATE TABLE #tmp_RecordCount
            (ChangeType VARCHAR(30))

          INSERT INTO #DimTitleTracker
                      (TitleTrackerID)
          SELECT DISTINCT Title_Tracker_Id = tfi.TMAFileInfoId
          FROM   Staging.dbo.TMA_FileInfo tfi WITH (NOLOCK)

          BEGIN TRAN

          DELETE FROM DTT
          FROM   dbo.DimTitleTracker DTT
                 INNER JOIN #DimTitleTracker TDTT WITH (NOLOCK)
                   ON DTT.TitleTrackerId=TDTT.TitleTrackerId

          INSERT INTO dbo.DimTitleTracker
                      (StockID,
                       TitleTrackerID,
                       TitleHandlerName,
                       TitleTrackerTask,
                       TitleTrackerLastActivityDateTime,
                       TitleTrackerFollowUpDate,
                       IsTitleTrackerTitleDirect,
                       TitleTrackerLienholderName,
                       TitleTrackerFileType,
                       IsActive,
                       BIUpdateDatetime)
          SELECT DISTINCT DS.StockID,
                          tfi.TMAFileInfoId AS Title_Tracker_Id,
                          Isnull((SELECT TOP 1 HandlerName
                                  FROM   (SELECT DISTINCT ca.CSAT_Salvage_Provider_Employee_ID AS [CSATSalvageProviderEmployeeID],
                                                          Last_Name+', '+First_Name AS HandlerName
                                          FROM   Staging.dbo.CSAT_ASAPID_to_CSAID_Xref ca WITH(NOLOCK)
                                                 INNER JOIN BI_ASAP_REP..User_Association ua WITH (nolock)
                                                   ON ca.Employee_ID=ua.Salvage_Provider_Employee_ID
                                                 INNER JOIN BI_ASAP_REP..User_Info ui WITH (nolock)
                                                   ON ua.User_ID=ui.User_ID
                                                 INNER JOIN BI_ASAP_REP..Salvage_Provider_Employee spe WITH (nolock)
                                                   ON ca.Employee_ID=spe.Salvage_Provider_Employee_ID
                                          WHERE  spe.Status_Code<>'INA'
                                             AND ui.Last_Name IS NOT NULL
                                             AND ui.First_Name IS NOT NULL) vth
                                  WHERE  tfi.HandlerID=vth.CSATSalvageProviderEmployeeID
                                  ORDER  BY vth.HandlerName), (SELECT Isnull(pe.Last_Name, '')+Isnull(', '+pe.First_Name, '')
                                                               FROM   Staging.dbo.Provider_Employee pe WITH(NOLOCK)
                                                               WHERE  pe.CSAT_Salvage_Provider_Employee_ID=tfi.HandlerID)) AS Title_Handler_Name,
                          CASE
                            WHEN tfi.CancelInd=1 THEN 'Cancelled Claim File'
                            ELSE
                              CASE
                                WHEN tst.ProcessId IN (1, 4) THEN (SELECT Replace(ts.StatusDescription, 'LH', 'LH '+CONVERT(VARCHAR(10),
                                                                          tfl.LienHolderSerialId))
                                                                   FROM   Staging.dbo.TMA_Status ts WITH(NOLOCK)
                                                                   WHERE  tstt.statusid=ts.statusid)
                                ELSE (SELECT ts.StatusDescription
                                      FROM   Staging.dbo.TMA_Status ts WITH(NOLOCK)
                                      WHERE  tst.statusid=ts.statusid)
                              END
                          END AS Title_Tracker_Task,
                          CASE
                            WHEN tfi.CancelInd=1 THEN tfi.mddate
                            ELSE
                              CASE
                                WHEN tst.ProcessId IN (1, 4) THEN tstt.statusdatetime
                                ELSE tst.statusdatetime
                              END
                          END AS Title_Tracker_Last_Activity_DateTime,
                          CASE
                            WHEN tst.ProcessId IN (1, 4) THEN tstt.FollowupDate
                            ELSE tst.FollowupDate
                          END AS Title_Tracker_Follow_Up_Date,
                          tfi.IsTitleDirect AS IsTitleTrackerTitleDirect,
                          tfl.Name AS TitleTrackerLienholderName,
                          (SELECT TOP 1 FileTypeName
                           FROM   Staging.dbo.TMA_FileType tft WITH(NOLOCK)
                           WHERE  tft.FileTypeCodeId=tfi.FileTypeCodeId
                           ORDER  BY FileTypeName) AS TitleTrackerFileType,
                          CASE
                            WHEN tst.ActiveStatusInd=1
                                 AND Isnull(tstt.ActiveStatusInd, 1)=1 THEN 1
                            ELSE 0
                          END AS IsActive,
                          GETDATE() AS BIUpdateDatetime
          FROM   Staging.dbo.TMA_FileInfo tfi WITH (NOLOCK)
                 LEFT OUTER JOIN Staging.dbo.TMA_FileLienHolderInfo tfl WITH (NOLOCK)
                   ON tfi.TMAFileInfoID=tfl.TMAFileInfoID
                 LEFT OUTER JOIN Staging.dbo.TMA_StatusTracker tst WITH(NOLOCK)
                   ON tfi.TMAFileInfoId=tst.TMAFileInfoId
                 LEFT OUTER JOIN Staging.dbo.TMA_SubTaskTracker tstt WITH(NOLOCK)
                   ON tfl.TMAFileLienHolderInfoId=tstt.TMAFileLienHolderInfoId
                 LEFT OUTER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
                   ON tfi.SalvageId=DS.ASAPSalvageID

          COMMIT TRAN

          EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
          @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp

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

          RETURN -1
      END CATCH
  END