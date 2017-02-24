CREATE PROCEDURE [dbo].[usp_ETLRun] @ETLJobName VarChar(20)
/*************************************************************
USE EDW
GO 
Description: Updates the ETLRun table for daily load.

Revision History
Date      Author	Revision Description
8/11/2011	VV		Created
8/16/2011	MS		Added ETLJobName, MinDate, MaxDate
12/7/2011   VV      Added - Execute StocProc usp_ETLBusinessDay
12/22/2011  VV      Impletemented Try/Catch.(DBA Code Review)
08/06/2011  MS      Added update for cdc BI_ASAP_AUDIT_Rep
08/07/2011  VV      Added update for cdc BI_CSA_REP
08/07/2011  MS      Added update for cdc BI_Auction_Rep
08/28/2016  MW      Inserted new ETLJobName for table EDW.dbo.ETLRun, User Story 190497

Usage:
EXEC [dbo].[usp_ETLRun] @ETLJobName = 'DailyETLJob';
*************************************************************/

/*************************************************************
--USE THIS CODE TO RESET OR INITIALLY POPULATE THE TABLE

USE BI_ASAP_Rep
GO

DECLARE @StartLSN BINARY(10),@EndLSN BINARY(10), @begin_time DATETIME, @end_time DATETIME, @ETLJobName VARCHAR(20);

SET @end_time = Getdate();
SET @begin_time = Dateadd(d, -1, @end_time);
SET @ETLJobName = 'DailyETLJob'

SELECT @StartLSN=sys.Fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
SELECT @EndLSN=sys.Fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);

DELETE FROM dbo.ETLRun WHERE  ETLJobName=@ETLJobName

INSERT INTO dbo.ETLRun
SELECT ETLJobName = @ETLJobName,
       MinDatetime = @begin_time,
       MaxDatetime = @end_time,
       MinLSN = @StartLSN,
       MaxLSN = @EndLSN;

USE BI_ASAP_AUDIT_Rep
GO

DECLARE @StartLSN BINARY(10),@EndLSN BINARY(10), @begin_time DATETIME, @end_time DATETIME, @ETLJobName VARCHAR(20);

SET @end_time = Getdate();
SET @begin_time = Dateadd(d, -3, @end_time);
SET @ETLJobName = 'DailyETLJob'

SELECT @StartLSN=sys.Fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
SELECT @EndLSN=sys.Fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);

DELETE FROM edw.dbo.ETLRun WHERE  ETLJobName=@ETLJobName + '_BI_ASAP_AUDIT_Rep'

INSERT INTO edw.dbo.ETLRun
SELECT ETLJobName = @ETLJobName + '_BI_ASAP_AUDIT_Rep',
       MinDatetime = @begin_time,
       MaxDatetime = @end_time,
       MinLSN = @StartLSN,
       MaxLSN = @EndLSN;

SELECT * FROM   edw.dbo.ETLRun; 
************************************************************/

AS
BEGIN
SET NOCOUNT ON;

BEGIN TRY

  UPDATE dbo.ETLRun
  SET	--Below is commented out for testing
		MinDatetime = MaxDatetime,
		MaxDatetime = GETDATE(),
		MinLSN = [BI_ASAP_Rep].sys.fn_cdc_increment_lsn(MaxLSN),
		MaxLSN = [BI_ASAP_Rep].sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE()) 
		
		--Below is added for testing
		--MinDatetime = Dateadd(DD, -2, GETDATE()),
		--MaxDatetime = GETDATE(),
		--MinLSN = BI_ASAP_Rep.sys.Fn_cdc_map_time_to_lsn('smallest greater than', Dateadd(DD, -2, GETDATE())),
		--MaxLSN = BI_ASAP_Rep.sys.Fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE())
	WHERE ETLJobName = @ETLJobName

  UPDATE dbo.ETLRun
  SET	--Below is commented out for testing
		MinDatetime = MaxDatetime,
		MaxDatetime = GETDATE(),
		MinLSN = [BI_ASAP_AUDIT_Rep].sys.fn_cdc_increment_lsn(MaxLSN),
		MaxLSN = [BI_ASAP_AUDIT_Rep].sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE()) 
		
		--Below is added for testing
		--MinDatetime = Dateadd(DD, -2, GETDATE()),
		--MaxDatetime = GETDATE(),
		--MinLSN = BI_ASAP_AUDIT_Rep.sys.Fn_cdc_map_time_to_lsn('smallest greater than', Dateadd(DD, -2, GETDATE())),
		--MaxLSN = BI_ASAP_AUDIT_Rep.sys.Fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE())
	WHERE ETLJobName = @ETLJobName + '_BI_ASAP_AUDIT_Rep'
	
	
	UPDATE dbo.ETLRun
    SET	--Below is commented out for testing
		MinDatetime = MaxDatetime,
		MaxDatetime = GETDATE(),
		MinLSN = [BI_CSA_REP].sys.fn_cdc_increment_lsn(MaxLSN),
		MaxLSN = [BI_CSA_REP].sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE()) 
		
		--Below is added for testing
		--MinDatetime = Dateadd(DD, -2, GETDATE()),
		--MaxDatetime = GETDATE(),
		--MinLSN = BI_CSA_REP.sys.Fn_cdc_map_time_to_lsn('smallest greater than', Dateadd(DD, -2, GETDATE())),
		--MaxLSN = BI_CSA_REP.sys.Fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE())
	  WHERE ETLJobName = @ETLJobName + '_BI_CSA_REP'
	
	
	UPDATE dbo.ETLRun
    SET	--Below is commented out for testing
		MinDatetime = MaxDatetime,
		MaxDatetime = GETDATE(),
		MinLSN = [BI_Auction_Rep].sys.fn_cdc_increment_lsn(MaxLSN),
		MaxLSN = [BI_Auction_Rep].sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE()) 
		
		--Below is added for testing
		--MinDatetime = Dateadd(DD, -2, GETDATE()),
		--MaxDatetime = GETDATE(),
		--MinLSN = BI_CSA_REP.sys.Fn_cdc_map_time_to_lsn('smallest greater than', Dateadd(DD, -2, GETDATE())),
		--MaxLSN = BI_CSA_REP.sys.Fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE())
	  WHERE ETLJobName = @ETLJobName + '_BI_Auction_Rep'


	UPDATE dbo.ETLRun
    SET	--Below is commented out for testing
		MinDatetime = (Select Case when datepart(dw,MaxDatetime) in (7) then dateadd(dd,-1,MaxDatetime) else MaxDatetime end  ),
		MaxDatetime = GETDATE(),
		MinLSN = [BI_ASAP_Rep].sys.fn_cdc_increment_lsn(MaxLSN),
		MaxLSN = [BI_ASAP_Rep].sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE()) 
		
		--Below is added for testing
		--MinDatetime = Dateadd(DD, -2, GETDATE()),
		--MaxDatetime = GETDATE(),
		--MinLSN = BI_CSA_REP.sys.Fn_cdc_map_time_to_lsn('smallest greater than', Dateadd(DD, -2, GETDATE())),
		--MaxLSN = BI_CSA_REP.sys.Fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE())
	  WHERE ETLJobName = @ETLJobName + '_EDWCSATodayDataLoad'
	
	EXEC [dbo].[usp_ETLBusinessDay];
	
    RETURN 0
 
END TRY

BEGIN CATCH

    RETURN -1
    
END CATCH



END;


GO


