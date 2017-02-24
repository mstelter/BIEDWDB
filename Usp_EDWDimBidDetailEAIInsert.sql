/*************************************************************
USE EDW
GO 
Description: Inserts into the imBidDetailEai table.

Revision History
Date			Author			 Revision Description
4/24/2014	  Yogitha Alwarsity			Created

Usage:
EXEC [dbo].[usp_EDWDimBidDetailEaiInsert]
SELECT TOP 1000 * FROM [EDW].dbo.Bid_Detail_Eai
*************************************************************/
CREATE PROCEDURE [dbo].[usp_EDWDimBidDetailEaiInsert] 
AS
BEGIN

      DECLARE @Last_Run_Start DATETIME 

      SET nocount ON
	  BEGIN TRY 

      SELECT @Last_Run_Start = Max(maxdatetime) FROM dbo.etlrun WITH (nolock) 

          CREATE TABLE #tmp_first_last_bid_datetime 
            ( 
               auction_schedule_dm_id INT, 
               bid_event_day_id       INT, 
               stock_item_dm_id       INT, 
               first_bid_datetime     DATETIME, 
               last_bid_datetime      DATETIME, 
               min_sequence_number    INT 
            ) 

          INSERT INTO #tmp_first_last_bid_datetime 
                      (auction_schedule_dm_id, 
                       bid_event_day_id, 
                       stock_item_dm_id, 
                       first_bid_datetime, 
                       last_bid_datetime, 
                       min_sequence_number) 
          SELECT DISTINCT AI.auction_schedule_id, 
                          DBD.bideventdayid, 
                          SS.stockid, 
                          Min(BDF.firstbiddatetime) AS First_Bid_Datetime, 
                          Max(BDF.lastbiddatetime)  AS Last_Bid_Datetime, 
                          Min(DBD.sequencenumber)   AS MIN_Sequence_Number 
          FROM   dbo.dimbiddetail AS DBD WITH (nolock) 
                INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                   ON AI.auction_item_id = DBD.auctionitemid 
                INNER JOIN dbo.dimstock SS WITH (nolock) 
                   ON AI.salvage_id = SS.asapsalvageid 
                INNER JOIN dbo.factbidsummary AS BDF WITH (nolock) 
                   ON DBD.auctionitemid = BDF.auctionitemid 
          WHERE  DBD.bidstatuscode = 'K' 
                 AND DBD.bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 
          GROUP  BY AI.auction_schedule_id, 
                    SS.stockid, 
                    DBD.bideventdayid 

          CREATE TABLE #tmp_first_last_confirm_datetime 
            ( 
               auction_schedule_dm_id INT, 
               bid_event_day_id       INT, 
               stock_item_dm_id       INT, 
               first_bid_datetime     DATETIME, 
               last_bid_datetime      DATETIME, 
               min_sequence_number    INT 
            ) 

          INSERT INTO #tmp_first_last_confirm_datetime 
                      (auction_schedule_dm_id, 
                       bid_event_day_id, 
                       stock_item_dm_id, 
                       first_bid_datetime, 
                       last_bid_datetime, 
                       min_sequence_number) 
          SELECT DISTINCT AI.auction_schedule_id, 
                          DBD.bideventdayid, 
                          SS.stockid, 
                          Min(BDF.firstbiddatetime) AS First_Bid_Datetime, 
                          Max(BDF.lastbiddatetime)  AS Last_Bid_Datetime, 
                          Min(DBD.sequencenumber)   AS MIN_Sequence_Number 
          FROM   dbo.dimbiddetail AS DBD WITH (nolock) 
                 INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                   ON AI.auction_item_id = DBD.auctionitemid 
                INNER  JOIN dbo.dimstock SS WITH (nolock) 
                   ON AI.salvage_id = SS.asapsalvageid 
                 INNER JOIN dbo.factbidsummary AS BDF WITH (nolock) 
                   ON DBD.auctionitemid = BDF.auctionitemid 
          WHERE  DBD.bidstatuscode = 'C' 
                 AND DBD.bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 
          GROUP  BY AI.auction_schedule_id, 
                    SS.stockid, 
                    DBD.bideventdayid 

          CREATE TABLE #tmp_last_bid_info 
            ( 
               [stock_item_dm_id]       [INT], 
               [auction_schedule_dm_id] [INT], 
               [bid_event_datetime]     [DATETIME], 
               [bid_event_day_id]       [INT], 
               [sequence_number]        [INT], 
               [bid_status_code]        [NCHAR](2), 
               [bid_amount]             [MONEY] 
            ) 

          INSERT INTO #tmp_last_bid_info 
                      (stock_item_dm_id, 
                       auction_schedule_dm_id, 
                       bid_event_datetime, 
                       bid_event_day_id, 
                       sequence_number, 
                       bid_status_code, 
                       bid_amount) 
          SELECT DISTINCT SS.stockid, 
                          FAI.auction_schedule_id, 
                          DBD.bideventdatetime, 
                          DBD.bideventdayid, 
                          DBD.sequencenumber, 
                          DBD.bidstatuscode, 
                          DBD.bidamount 
          FROM   #tmp_first_last_bid_datetime AS TFLBD 
                 INNER JOIN dbo.dimbiddetail AS DBD WITH (nolock) 
                         ON TFLBD.bid_event_day_id = DBD.bideventdayid 
                            AND TFLBD.last_bid_datetime = DBD.bideventdatetime 
                 INNER JOIN bi_asap_rep.dbo.auction_item FAI WITH (nolock) 
                         ON FAI.auction_item_id = DBD.auctionitemid 
                 INNER JOIN dbo.dimstock SS WITH (nolock) 
                         ON FAI.salvage_id = SS.asapsalvageid 
          WHERE  DBD.bidstatuscode = 'K' 

          CREATE TABLE #tmp_confirm_bid_info 
            ( 
               [stock_item_dm_id]       [INT], 
               [auction_schedule_dm_id] [INT], 
               [bid_event_datetime]     [DATETIME], 
               [bid_event_day_id]       [INT], 
               [sequence_number]        [INT], 
               [bid_status_code]        [NCHAR](2), 
               [bid_amount]             [MONEY] 
            ) 

          INSERT INTO #tmp_confirm_bid_info 
                      (stock_item_dm_id, 
                       auction_schedule_dm_id, 
                       bid_event_datetime, 
                       bid_event_day_id, 
                       sequence_number, 
                       bid_status_code, 
                       bid_amount) 
          SELECT DISTINCT SS.stockid, 
                          FAI.auction_schedule_id, 
                          DBD.bideventdatetime, 
                          DBD.bideventdayid, 
                          DBD.sequencenumber, 
                          DBD.bidstatuscode, 
                          DBD.bidamount 
          FROM   #tmp_first_last_confirm_datetime AS TFLCD 
                 INNER JOIN dbo.dimstock SS WITH (nolock) 
                         ON SS.stockid = TFLCD.stock_item_dm_id 
                 INNER JOIN bi_asap_rep.dbo.auction_item FAI WITH (nolock) 
                         ON FAI.salvage_id = SS.asapsalvageid 
                 INNER JOIN dbo.dimbiddetail AS DBD WITH (nolock) 
                         ON FAI.auction_item_id = DBD.auctionitemid 
          WHERE  DBD.bidstatuscode = 'C' 

          CREATE TABLE #tmp_winning_bid_info 
            ( 
               [stock_item_dm_id]            [INT], 
               [auction_schedule_dm_id]      [INT], 
               [bid_event_day_id]            [INT], 
               [winning_bid_amount]          [MONEY], 
               [winning_bid_event_datetime]  [DATETIME], 
               [last_bid_sequence_number]    [INT], 
               [confirm_bid_amount]          [MONEY], 
               [confirm_bid_event_datetime]  [DATETIME], 
               [confirm_bid_sequence_number] [INT] 
            ) 

          INSERT INTO #tmp_winning_bid_info 
                      (stock_item_dm_id, 
                       auction_schedule_dm_id, 
                       bid_event_day_id, 
                       winning_bid_amount, 
                       winning_bid_event_datetime, 
                       last_bid_sequence_number, 
                       confirm_bid_amount, 
                       confirm_bid_event_datetime, 
                       confirm_bid_sequence_number) 
          SELECT DISTINCT TLBI.stock_item_dm_id, 
                          TLBI.auction_schedule_dm_id, 
                          TLBI.bid_event_day_id, 
                          TLBI.bid_amount         AS Winning_Bid_Amount, 
                          TLBI.bid_event_datetime AS Winning_Bid_Event_Datetime, 
                          TLBI.sequence_number    AS Last_Bid_Sequence_Number, 
                          TCBI.bid_amount         AS Confirm_Bid_Amount, 
                          TCBI.bid_event_datetime AS Confirm_Bid_Event_Datetime, 
                          TCBI.sequence_number    AS Confirm_Bid_Sequence_Number 
          FROM   #tmp_last_bid_info AS TLBI  WITH (nolock)
                 INNER JOIN #tmp_confirm_bid_info AS TCBI  WITH (nolock)
                         ON TLBI.stock_item_dm_id = TCBI.stock_item_dm_id 
                            AND TLBI.auction_schedule_dm_id = TCBI.auction_schedule_dm_id 
          WHERE  TLBI.bid_amount = TCBI.bid_amount 

          CREATE TABLE #tmp_bid_detail_eai_source1 
            ( 
               [bid_detail_filtered_dm_id] [INT], 
               [bid_log_item]              [INT], 
               [asap_auction_schedule_id]  [INT], 
               [auction_start_datetime]    [DATETIME], 
               [auction_end_datetime]      [DATETIME], 
               [bid_amount]                [MONEY], 
               [bid_event_datetime]        [DATETIME], 
               [bid_status_code]           [NCHAR] (2), 
               [branch_number]             [INT], 
               [asap_provider_id]          [INT], 
               [asap_salvage_id]           [INT], 
               [bidder_name]               [NVARCHAR] (100), 
               [sort_order]                [INT], 
               [winning_bid_ind]           [INT], 
               [create_datetime]           [DATETIME], 
               [provider_group_name]       [VARCHAR] (60) 
            ) 

          INSERT INTO #tmp_bid_detail_eai_source1 
                      (bid_detail_filtered_dm_id, 
                       bid_log_item, 
                       asap_auction_schedule_id, 
                       auction_start_datetime, 
                       auction_end_datetime, 
                       bid_amount, 
                       bid_event_datetime, 
                       bid_status_code, 
                       branch_number, 
                       asap_provider_id, 
                       asap_salvage_id, 
                       bidder_name, 
                       sort_order, 
                       winning_bid_ind, 
                       create_datetime, 
                       provider_group_name) 
          SELECT BDF.biddetailid, 
                 BDF.bidlogitem, 
                 DPAC.auction_schedule_id, 
                 AUS.auction_datetime     AS Auction_Start_Datetime, 
                 AUS.auction_datetime     AS Auction_End_Datetime, 
                 BDF.bidamount, 
                 ( BDF.bideventdatetime ) AS BidEventDatetime, 
                 BDF.bidstatuscode, 
                 IB.branchnumber, 
                 PRV.providerid, 
                 STK.asapsalvageid, 
                 CASE 
                   WHEN BDF.displayname = 'OnSite' THEN 'OnSite' 
                   ELSE BDF.displaylocation 
                 END                      AS Bidder_Name, 
                 BDF.sortorder, 
                 CASE 
                   WHEN TWBI.stock_item_dm_id IS NOT NULL THEN 1 
                   ELSE 0 
                 END                      AS Winning_Bid_Ind, 
                 Getdate()                AS Create_Datetime, 
                 PRV.providergroupname 
          FROM   dbo.dimbiddetail AS BDF WITH (nolock) 
                 INNER JOIN bi_asap_rep.dbo.auction_item DPAC WITH (nolock) 
                         ON DPAC.auction_item_id = BDF.auctionitemid 
                 INNER JOIN bi_asap_rep.dbo.auction_schedule AUS WITH (nolock) 
                         ON AUS.auction_schedule_id = DPAC.auction_schedule_id 
                 INNER JOIN dbo.dimstock STK WITH (nolock) 
                         ON DPAC.salvage_id = STK.asapsalvageid 
                 INNER JOIN dbo.factbidsummary AS BDFS WITH (nolock) 
                         ON BDF.auctionitemid = BDFS.auctionitemid 
                 INNER JOIN dbo.dimbranch AS IB WITH (nolock) 
                         ON STK.administrativebranchnumber = IB.branchnumber 
                 INNER JOIN dbo.dimprovider AS PRV WITH (nolock) 
                         ON STK.currentproviderid = PRV.providerid 
                 LEFT OUTER JOIN #tmp_winning_bid_info AS TWBI  WITH (nolock)
                              ON BDF.bideventdatetime = 
                                 TWBI.winning_bid_event_datetime 
                                 AND BDF.bidamount = TWBI.winning_bid_amount 
          WHERE  BDF.bidstatuscode IN ( 'K' ) 
                 AND BDF.bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 
          ORDER  BY TWBI.stock_item_dm_id, 
                    BDF.sequencenumber 

          SELECT asap_salvage_id, 
                 bid_amount, 
                 Max(bid_event_datetime) AS Bid_Event_Datetime 
          INTO   #tmp_bid_detail_final_source 
          FROM   #tmp_bid_detail_eai_source1  WITH (nolock)
          GROUP  BY bid_amount, 
                    asap_salvage_id 
          ORDER  BY asap_salvage_id 

          CREATE TABLE #tmp_bid_detail_eai_source 
            ( 
               [bid_detail_filtered_dm_id] [INT], 
               [bid_log_item]              [INT], 
               [asap_auction_schedule_id]  [INT], 
               [auction_start_datetime]    [DATETIME], 
               [auction_end_datetime]      [DATETIME], 
               [bid_amount]                [MONEY], 
               [bid_event_datetime]        [DATETIME], 
               [bid_status_code]           [NCHAR] (2), 
               [branch_number]             [INT], 
               [asap_provider_id]          [INT], 
               [asap_salvage_id]           [INT], 
               [bidder_name]               [NVARCHAR] (100), 
               [sort_order]                [INT], 
               [winning_bid_ind]           [INT], 
               [create_datetime]           [DATETIME], 
               [provider_group_name]       [VARCHAR] (60) 
            ) 

          INSERT INTO #tmp_bid_detail_eai_source 
                      (bid_detail_filtered_dm_id, 
                       bid_log_item, 
                       asap_auction_schedule_id, 
                       auction_start_datetime, 
                       auction_end_datetime, 
                       bid_amount, 
                       bid_event_datetime, 
                       bid_status_code, 
                       branch_number, 
                       asap_provider_id, 
                       asap_salvage_id, 
                       bidder_name, 
                       sort_order, 
                       winning_bid_ind, 
                       create_datetime, 
                       provider_group_name) 
          SELECT DISTINCT bid_detail_filtered_dm_id, 
                 bid_log_item, 
                 asap_auction_schedule_id, 
                 auction_start_datetime, 
                 auction_end_datetime, 
                 BDE.bid_amount, 
                 BDE.bid_event_datetime, 
                 bid_status_code, 
                 branch_number, 
                 asap_provider_id, 
                 BDE.asap_salvage_id, 
                 bidder_name, 
                 sort_order, 
                 winning_bid_ind, 
                 create_datetime, 
                 provider_group_name 
          FROM   #tmp_bid_detail_eai_source1 BDE  WITH (nolock)
                 INNER JOIN #tmp_bid_detail_final_source FS 
                   ON BDE.asap_salvage_id = FS.asap_salvage_id 
                      AND BDE.bid_amount = FS.bid_amount 
                      AND BDE.bid_event_datetime = FS.bid_event_datetime 

          BEGIN TRAN 

          INSERT INTO dbo.Bid_Detail_Eai 
                      (bid_detail_filtered_dm_id, 
                       bid_log_item, 
                       asap_auction_schedule_id, 
                       auction_start_datetime, 
                       auction_end_datetime, 
                       bid_amount, 
                       bid_event_datetime, 
                       bid_status_code, 
                       branch_number, 
                       asap_provider_id, 
                       asap_salvage_id, 
                       bidder_name, 
                       sort_order, 
                       winning_bid_ind, 
                       create_datetime, 
                       provider_group_name) 
          SELECT TBDES.bid_detail_filtered_dm_id, 
                 TBDES.bid_log_item, 
                 TBDES.asap_auction_schedule_id, 
                 TBDES.auction_start_datetime, 
                 TBDES.auction_end_datetime, 
                 TBDES.bid_amount, 
                 TBDES.bid_event_datetime, 
                 TBDES.bid_status_code, 
                 TBDES.branch_number, 
                 TBDES.asap_provider_id, 
                 TBDES.asap_salvage_id, 
                 TBDES.bidder_name, 
                 TBDES.sort_order, 
                 TBDES.winning_bid_ind, 
                 TBDES.create_datetime, 
                 TBDES.provider_group_name 
          FROM   #tmp_bid_detail_eai_source AS TBDES  WITH (nolock)
                 LEFT OUTER JOIN dbo.Bid_Detail_Eai BDE WITH (nolock) 
					ON TBDES.bid_detail_filtered_dm_id = BDE.bid_detail_filtered_dm_id 
          WHERE  BDE.bid_detail_filtered_dm_id IS NULL 

	DELETE FROM BDE
	FROM [EDW].dbo.Bid_Detail_Eai BDE WITH (NOLOCK)
	LEFT OUTER JOIN BI_ASAP_REP.dbo.Salvage S WITH (NOLOCK)
	ON BDE.asap_salvage_id = S.Salvage_ID
	WHERE S.Salvage_ID IS NULL 

          COMMIT TRAN 

		IF OBJECT_ID('tempdb..#tmp_first_last_bid_datetime') IS NOT NULL DROP TABLE #tmp_first_last_bid_datetime
		IF OBJECT_ID('tempdb..#tmp_first_last_confirm_datetime') IS NOT NULL DROP TABLE #tmp_first_last_confirm_datetime
		IF OBJECT_ID('tempdb..#tmp_last_bid_info') IS NOT NULL DROP TABLE #tmp_last_bid_info
		IF OBJECT_ID('tempdb..#tmp_confirm_bid_info') IS NOT NULL DROP TABLE #tmp_confirm_bid_info
		IF OBJECT_ID('tempdb..#tmp_winning_bid_info') IS NOT NULL DROP TABLE #tmp_winning_bid_info
		IF OBJECT_ID('tempdb..#tmp_bid_detail_eai_source1') IS NOT NULL DROP TABLE #tmp_bid_detail_eai_source1
		IF OBJECT_ID('tempdb..#tmp_bid_detail_eai_source') IS NOT NULL DROP TABLE #tmp_bid_detail_eai_source

	RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		
		IF OBJECT_ID('tempdb..#tmp_first_last_bid_datetime') IS NOT NULL DROP TABLE #tmp_first_last_bid_datetime
		IF OBJECT_ID('tempdb..#tmp_first_last_confirm_datetime') IS NOT NULL DROP TABLE #tmp_first_last_confirm_datetime
		IF OBJECT_ID('tempdb..#tmp_last_bid_info') IS NOT NULL DROP TABLE #tmp_last_bid_info
		IF OBJECT_ID('tempdb..#tmp_confirm_bid_info') IS NOT NULL DROP TABLE #tmp_confirm_bid_info
		IF OBJECT_ID('tempdb..#tmp_winning_bid_info') IS NOT NULL DROP TABLE #tmp_winning_bid_info
		IF OBJECT_ID('tempdb..#tmp_bid_detail_eai_source1') IS NOT NULL DROP TABLE #tmp_bid_detail_eai_source1
		IF OBJECT_ID('tempdb..#tmp_bid_detail_eai_source') IS NOT NULL DROP TABLE #tmp_bid_detail_eai_source

		RETURN -1
	

END CATCH

END



GO


