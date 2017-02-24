/*  
-- =============================================  
-- Author:    YALWARSETTY  
-- Create date: 05/23/2014  -- Description:  STORED PROCEDURE TO EXTRACT XCIRA BID HISTORY INFORMATION FROM BI TO CSATODAY   -> TABLE WILL BE SENT TO CSATODAY VIA REPLICATION  
--8/28/2015   Pratyusha Koduru  Changed Bid_Status_Description_Mapping to the Codes 'E' AND B' and Deleted duplicates - I100
--9/2/2015    Pratyusha Koduru  Removed 'E' from Auction_Sale_Count logic and Deleted Second Record of Counter Offer 'E' 
-- 10/9/2015  Juan X. Jacome  Update syntax bidstatuscode 'E' WHEN 'E' THEN 'Seller''s Counter Bid' 

  EXEC: Exec EDW.[dbo].[usp_EDWBidDetailProviderInsert]  
-- =============================================  
*/ 
CREATE PROCEDURE [dbo].[usp_EDWBidDetailProviderInsert] 
AS 
  BEGIN 

      DECLARE @Last_Run_Start DATETIME 

      SET NOCOUNT ON
        BEGIN TRY

      SELECT @Last_Run_Start = Max(maxdatetime) FROM [EDW].dbo.etlrun WITH (nolock) 
 

CREATE TABLE #tmp_bid_record (stockid INT,
                                                Floor_Override_Count INT,
                                                New_Item_Count INT,
                                                Auction_Sale_Count INT,
                                                Confirm_Auction_Sale_Count INT,
                                                Accepted_Bid_Count INT)

CREATE TABLE #tmp_awarded_bid_record(stockid INT,
      Category_Type VARCHAR(2))

CREATE TABLE #tmp_run_once_awarded_twice(Stock_Item_DM_ID INT,
      Category_Type VARCHAR(2))

CREATE TABLE #tmp_run_twice_awarded_twice(Stock_Item_DM_ID INT,
      Category_Type VARCHAR(2))

CREATE TABLE #tmp_run_twice_awarded_once(stockid INT,
      sequencenumber INT,
      bidstatuscode VARCHAR(1),
      Category_Type VARCHAR(2))

CREATE TABLE #tmp_capture_new_item_event_sequence(stockid INT,
      First_New_Item_Event INT,
      Second_New_Item_Event INT)

CREATE TABLE #tmp_sub_category_for_category_2b(stockid INT,
      First_Group_Accepted_Bid_Count INT,
      Second_Group_Accepted_Bid_Count INT)

CREATE TABLE #tmp_category_2bb(stockid INT,
      Category_Type VARCHAR(3))

CREATE TABLE #tmp_bid_detail_provider_source(Bid_Detail_Categorized_DM_ID INT,
      bidlogitem INT,
      auction_schedule_id INT,
      auction_datetime datetime,
      bidamount money,
      Bid_Event_Datetime datetime,
      bidstatuscode varchar(1),
      Bid_Status_Display_Mapping varchar(100),
      branchnumber INT,
      providerid INT,
      asapsalvageid INT,
      displayname varchar(50),
      Displaylocation varchar(50),
      sortorder INT,
      Winning_Bid_Ind INT,
      stockid INT,
      devicetype varchar(50))

CREATE TABLE #tmp_sale_amount_lesser_auction_sale(Stock_ITem_DM_ID INT,
      Auction_Schedule_DM_ID INT)

CREATE TABLE #tmp_category_2a_min_sale_sequence(Stock_ITem_DM_ID INT,
      Auction_Schedule_DM_ID INT,
      MIN_Sale_Sequence_Number INT)

CREATE TABLE #tmp_purge_category_2a_entries(Stock_ITem_DM_ID INT,
      Auction_Schedule_DM_ID INT,
      Bid_Status_Code VARCHAR(1),
      Purge_Sequence_Number INT)

CREATE TABLE #tmp_category_2b_min_sale_sequence(Stock_ITem_DM_ID INT,
      Auction_Schedule_DM_ID INT,
      Item_Awarded_Sequence_Number INT,
      MIN_New_Item_Sequence_Number INT,
      MAX_New_Item_Sequence_Number INT,
      MAX_Sequence_Number INT)

CREATE TABLE #tmp_category_2b_min_sale_sequence_delete(stock_item_dm_id INT,
      auction_schedule_dm_id INT,
      Start_Delete_Sequence_Number INT,
      End_Delete_Sequence_Number INT,
      DELETE_PREFERENCE INT)

CREATE TABLE #tmp_category_2c_sequence(Stock_ITem_DM_ID INT,
      Auction_Schedule_DM_ID INT,
      MIN_Sale_Sequence_Number INT,
      MAX_New_Item_Sequence_Number INT)

CREATE TABLE #tmp_purge_category_2c_entries(Stock_ITem_DM_ID INT,
      Auction_Schedule_DM_ID INT,
      Purge_Sequence_Number INT,
      min_sale_sequence_number INT,
      max_new_item_sequence_number INT)

CREATE TABLE #tmp_reconcile_sale_date_range (business_day_id INT) 

CREATE TABLE #tmp_remove_reconciling_stocks 
        ([auction_schedule_dm_id]    [INT], 
           [stock_item_dm_id]          [INT], 
           [auction_datetime]          [DATETIME], 
           [auction_entry_reason_code] [CHAR] (3), 
           [accept_bid_status_code]    [CHAR] (3), 
           [bid_amount]                [DECIMAL], 
           [buyer_id]                  [INT], 
           [entry_datetime]            [DATETIME], 
           [negotiated_sale_ind]       [BIT], 
           [update_datetime]           [DATETIME])

INSERT INTO #tmp_bid_record(stockid,
      Floor_Override_Count,
      New_Item_Count,
      Auction_Sale_Count,
      Confirm_Auction_Sale_Count,
      Accepted_Bid_Count)
      SELECT DISTINCT SS.stockid, 
                      Sum(CASE 
                            WHEN bidstatuscode = 'O' THEN 1 
                            ELSE 0 
                          END) AS Floor_Override_Count, 
                      Sum(CASE 
                            WHEN bidstatuscode = '^' THEN 1 
                            ELSE 0 
                          END) AS New_Item_Count, 
                      Sum(CASE 
                            --WHEN bidstatuscode IN('S','B','E') THEN 1 
                              WHEN bidstatuscode IN('S','B') THEN 1 
                            ELSE 0 
                          END) AS Auction_Sale_Count, 
                      Sum(CASE 
                            WHEN bidstatuscode = 'C' THEN 1 
                            ELSE 0 
                          END) AS Confirm_Auction_Sale_Count, 
                      Sum(CASE 
                            WHEN bidstatuscode = 'K' THEN 1 
                            ELSE 0 
                          END) AS Accepted_Bid_Count 
      --INTO   #tmp_bid_record 
      FROM   [EDW].dbo.dimbiddetail DBD WITH (NOLOCK)
             INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                     ON AI.auction_item_id = DBD.auctionitemid 
             INNER JOIN edw.dbo.dimstock SS WITH (nolock) 
                     ON AI.salvage_id = SS.asapsalvageid 
      WHERE  bidstatuscode IN ( '^', 'C', 'K', 'O', 
                                'P', 'R', 'S', 'U','B','E' ) 
             AND bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 
      GROUP  BY SS.stockid 

      /**************************************************************************************************************************************** 
      --TOTAL Stocks Entries: 568033 
      1. NORMAL PATTERNS 
      a. AWARDED BID RECORD 
      - RUN THROUGH AUCTION ONCE 
      - AWARDED ONCE 
      - ONE OR MORE ACCEPTED BIDS 
      - UNDO DOES NOT EXCEED ACCEPTED BID 
      - OVERRIDE BID COUNT DOES NOT EXCEED ACCEPTED INTERNET BID COUNT 
      -- STOCK ITEM COUNTS: 522468 
      ****************************************************************************************************************************************/ 
      INSERT INTO #tmp_awarded_bid_record (stockid,Category_Type)
        SELECT DISTINCT SS.stockid, 
                      '1A' AS Category_Type 
      --INTO   #tmp_awarded_bid_record 
      FROM   edw.dbo.factbidsummary FBS WITH (nolock) 
             INNER JOIN [EDW].dbo.dimbiddetail AS DBD WITH (nolock) 
                     ON FBS.auctionitemid = DBD.auctionitemid 
             INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                     ON AI.auction_item_id = DBD.auctionitemid 
             INNER JOIN edw.dbo.dimstock SS WITH (nolock) 
                     ON AI.salvage_id = SS.asapsalvageid 
             INNER JOIN #tmp_bid_record BR WITH (nolock) 
                     ON BR.stockid = SS.stockid 
      WHERE  new_item_count = 1 
             AND auction_sale_count = 1 
             AND FBS.bidacceptedcount >= 1 
             AND FBS.bidacceptedcount > FBS.bidundocount 
             AND floor_override_count <= FBS.acceptedinternetbidcount 
             AND bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 

      /****************************************************************************************************************************************  
      2. MULTIPLE PATTERNS  
        a. RUN ONCE, AWARDED TWICE  
          - RUN THROUGH AUCTION ONCE  
          - AWARDED TWICE  
          - ONE OR MORE ACCEPTED BIDS  
        -- STOCK ITEM COUNTS: 250  
      ****************************************************************************************************************************************/ 
      INSERT INTO #tmp_run_once_awarded_twice(Stock_Item_DM_ID,Category_Type)
        SELECT stockid AS Stock_Item_DM_ID, 
             '2A'    AS Category_Type 
      --INTO   #tmp_run_once_awarded_twice 
      FROM   #tmp_bid_record  WITH (NOLOCK)
      WHERE  new_item_count = 1 
             AND auction_sale_count = 2 
             AND accepted_bid_count >= 1 

      --SELECT '#TMP_Run_Once_Awarded_Twice', '2A', @@ROWCOUNT  
      /**************************************************************************************************************************************** 
      2. MULTIPLE PATTERNS 
      C. RUN TWICE, AWARDED TWICE 
      - RUN THROUGH AUCTION MORE THAN ONCE 
      - RUN THROUGH AUCTION TWICE 
      - AWARDED MORE THAN ONCE 
      - ONE OR MORE ACCEPTED BIDS 
      -- STOCK ITEM COUNTS: 1065 
      08-NOV-2009 REVISION 
      - RUN THROUGH AUCTION TWICE 
      - AWARDED TWICE 
      - ONE OR MORE ACCEPTED BIDS 
       
      ****************************************************************************************************************************************/ 
      INSERT INTO #tmp_run_twice_awarded_twice(Stock_Item_DM_ID,Category_Type)
        SELECT stockid AS Stock_Item_DM_ID, 
             '2C'    AS Category_Type 
      --INTO   #tmp_run_twice_awarded_twice 
      FROM   #tmp_bid_record  WITH (NOLOCK)
      WHERE  new_item_count = 2 
             AND auction_sale_count = 2 
             AND accepted_bid_count >= 1 

      --SELECT '#TMP_Run_Twice_Awarded_Twice', '2C', @@ROWCOUNT  
      /**************************************************************************************************************************************** 
      2. MULTIPLE PATTERNS 
      - RUN THROUGH AUCTION TWICE 
      - AWARDED ONCE 
      - ONE OR MORE ACCEPTED BIDS 
       
      ****************************************************************************************************************************************/ 
      INSERT INTO #tmp_run_twice_awarded_once (stockid,sequencenumber,bidstatuscode,Category_Type)
        SELECT DISTINCT SS.stockid, 
                      DBD.sequencenumber, 
                      DBD.bidstatuscode, 
                      '2B' AS Category_Type 
      --INTO   #tmp_run_twice_awarded_once 
      FROM   edw.dbo.factbidsummary FBS WITH (nolock) 
             INNER JOIN [EDW].dbo.dimbiddetail AS DBD WITH (nolock) 
                     ON FBS.auctionitemid = DBD.auctionitemid 
             INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                     ON AI.auction_item_id = DBD.auctionitemid 
             INNER JOIN edw.dbo.dimstock SS WITH (nolock) 
                     ON AI.salvage_id = SS.asapsalvageid 
             INNER JOIN #tmp_bid_record TBR  WITH (NOLOCK)
                     ON TBR.stockid = SS.stockid 
      WHERE  new_item_count = 2 
             AND auction_sale_count = 1 
             AND FBS.bidacceptedcount >= 1 
             AND bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 

      /**************************************************************************************************************************************************************** 
      Category 2B: Run Twice Awarded Once 
       
      Sub Category 
      I.  There is 1 or more accepted (K) bid after the new item (^) 
      a.  This will not show up in the CSAToday output (Bid_Detail_Provider) 
      EX 1: ^,K,K,S,C,^,K 
      EX 2: ^,K,^,K,K,K,S,C 
       
      II.  There was no accepted (K) bid after the new item (^) 
      a.  This will show up in the CSAToday output (Bid_Detail_Provider) 
      EX 1: ^,K,K,S,C,^,P 
      EX 2: ^,P,^,K,K,K,S,C 
       
      ****************************************************************************************************************************************************************/ 
      INSERT INTO #tmp_capture_new_item_event_sequence(stockid,First_New_Item_Event,Second_New_Item_Event)
        SELECT DISTINCT SS.stockid, 
                      Min(DBD.sequencenumber) AS First_New_Item_Event, 
                      Max(DBD.sequencenumber) AS Second_New_Item_Event 
      --INTO   #tmp_capture_new_item_event_sequence 
      FROM   [EDW].dbo.dimbiddetail AS DBD WITH (nolock) 
             INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                     ON AI.auction_item_id = DBD.auctionitemid 
             INNER JOIN edw.dbo.dimstock SS WITH (nolock) 
                     ON AI.salvage_id = SS.asapsalvageid 
      WHERE  DBD.bidstatuscode = '^' 
             AND bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 
      GROUP  BY SS.stockid 

        INSERT INTO #tmp_sub_category_for_category_2b(stockid,
            First_Group_Accepted_Bid_Count,
            Second_Group_Accepted_Bid_Count)
      SELECT DISTINCT TSCFC2BS.stockid, 
                      Sum(Isnull(CASE 
                                   WHEN TSCFC2BS.sequencenumber BETWEEN 
                                        TCNIES.first_new_item_event AND 
                                        TCNIES.second_new_item_event THEN 
                                   1 
                                 END, 0)) AS First_Group_Accepted_Bid_Count, 
                      Sum(Isnull(CASE 
                                   WHEN TSCFC2BS.sequencenumber >= 
                                        TCNIES.second_new_item_event THEN 1 
                                 END, 0)) AS Second_Group_Accepted_Bid_Count 
      --INTO   #tmp_sub_category_for_category_2b 
      FROM   #tmp_run_twice_awarded_once AS TSCFC2BS  WITH (NOLOCK)
             INNER JOIN #tmp_capture_new_item_event_sequence AS TCNIES  WITH (NOLOCK)
                     ON TSCFC2BS.stockid = TCNIES.stockid 
      WHERE  TSCFC2BS.bidstatuscode = 'K' 
      GROUP  BY TSCFC2BS.stockid 

        INSERT INTO #tmp_category_2bb(stockid,
            Category_Type)
      SELECT DISTINCT stockid, 
                      ( CASE 
                          WHEN first_group_accepted_bid_count >= 1 
                               AND second_group_accepted_bid_count >= 1 THEN 
                          '2Ba' 
                          WHEN ( first_group_accepted_bid_count = 0 
                                 AND second_group_accepted_bid_count >= 1 ) 
                                OR ( first_group_accepted_bid_count >= 1 
                                     AND second_group_accepted_bid_count = 0 ) 
                        THEN 
                          '2Bb' 
                          ELSE '2BX' 
                        END ) Category_Type 
      --INTO   #tmp_category_2bb 
      FROM   #tmp_sub_category_for_category_2b  WITH (NOLOCK)

      INSERT INTO #tmp_bid_detail_provider_source(Bid_Detail_Categorized_DM_ID,
                  bidlogitem,
                  auction_schedule_id,
                  auction_datetime,
                  bidamount,
                  Bid_Event_Datetime,
                  bidstatuscode,
                  Bid_Status_Display_Mapping,
                  branchnumber,
                  providerid,
                  asapsalvageid,
                  displayname,
                  Displaylocation,
                  sortorder,
                  Winning_Bid_Ind,
                  stockid,
                  devicetype)
        SELECT Max(BDF.biddetailid)      AS Bid_Detail_Categorized_DM_ID, 
             BDF.bidlogitem, 
             AI.auction_schedule_id, 
             AUS.auction_datetime, 
             BDF.bidamount, 
             Max(BDF.bideventdatetime) AS Bid_Event_Datetime, 
             BDF.bidstatuscode, 
             CASE BDF.bidstatuscode 
               WHEN '^' THEN 'New Item' 
               WHEN 'P' THEN 'Proposed Bid' 
               WHEN 'K' THEN 'High Bid' 
               WHEN 'C' THEN 'Winning Bid' 
               WHEN 'S' THEN 'Winning Bid' 
                     WHEN 'B' THEN 'Sold on Call' 
                     WHEN 'E' THEN 'Seller''s Counter Bid' 
             END                       AS Bid_Status_Display_Mapping, 
             IB.branchnumber, 
             PRV.providerid, 
             SS.asapsalvageid, 
             BDF.displayname, 
             CASE 
               WHEN BDF.biddertype = 'F' THEN 'OnSite' 
               ELSE BDF.displaylocation 
             END                       AS Displaylocation, 
             BDF.sortorder, 
             CASE 
               WHEN SD.saleamount IS NOT NULL 
                    AND SD.saleamount = BDF.bidamount 
                    AND BDF.bidstatuscode = 'C' THEN 1 
               ELSE 0 
             END                       AS Winning_Bid_Ind, 
             SS.stockid, 
             BDF.devicetype --19254  
      --INTO   #tmp_bid_detail_provider_source 
      FROM   [EDW].dbo.dimbiddetail AS BDF WITH (nolock) 
             INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                     ON AI.auction_item_id = BDF.auctionitemid 
             INNER JOIN bi_asap_rep.dbo.auction_schedule AUS WITH (nolock) 
                     ON AUS.auction_schedule_id = AI.auction_schedule_id 
             INNER JOIN edw.dbo.dimstock SS WITH (nolock) 
                     ON AI.salvage_id = SS.asapsalvageid 
             INNER JOIN [EDW].dbo.dimbranch AS IB WITH (nolock) 
                     ON SS.administrativebranchnumber = IB.branchnumber 
             INNER JOIN [EDW].dbo.dimprovider AS PRV WITH (nolock) 
                     ON SS.currentproviderid = PRV.providerid 
             LEFT OUTER JOIN #tmp_awarded_bid_record ABR WITH (nolock) 
                          ON ABR.stockid = SS.stockid 
             LEFT OUTER JOIN #tmp_category_2bb C2B WITH (nolock) 
                          ON C2B.stockid = SS.stockid 
             LEFT OUTER JOIN [EDW].dbo.dimsolddetail AS SD WITH (nolock) 
                          ON AI.auction_item_id = SD.auctionitemid 
           
      WHERE  BDF.bidstatuscode IN ( '^', 'P', 'K', 'C', 'S','E','B' ) 
             AND BDF.bideventdatetime >= Dateadd(d, -3, @Last_Run_Start) 
             AND ( ABR.category_type = '1A' 
                    OR C2B.category_type = '2Bb' ) 
    
      GROUP  BY BDF.bidlogitem, 
                AI.auction_schedule_id, 
                AUS.auction_datetime, 
                BDF.bidamount, 
                BDF.bidstatuscode, 
                CASE BDF.bidstatuscode 
                  WHEN '^' THEN 'New Item' 
                  WHEN 'P' THEN 'Proposed Bid' 
                  WHEN 'K' THEN 'High Bid' 
                  WHEN 'C' THEN 'Winning Bid' 
                  WHEN 'S' THEN 'Winning Bid' 
                          WHEN 'B' THEN 'Sold on Call' 
                        WHEN 'E' THEN 'Seller''s Counter Bid' 
               END, 
                IB.branchnumber, 
                PRV.providerid, 
                SS.asapsalvageid, 
                BDF.displayname, 
                CASE 
                  WHEN BDF.biddertype = 'F' THEN 'OnSite' 
                  ELSE BDF.displaylocation 
                END, 
                BDF.sortorder, 
                CASE 
                  WHEN SD.saleamount IS NOT NULL 
                       AND SD.saleamount = BDF.bidamount 
                       AND BDF.bidstatuscode = 'C' THEN 1 
                  ELSE 0 
                END, 
                SS.stockid, 
                BDF.devicetype 

      /******************************************************************************************************************************************  
      IF SALE AMOUNT FROM ASAP STOCK SALE TABLE IS LESSER THEN BID HISTORY AUCTION SALE AMOUNT, DON'T SHOW THE RECORD  
      ******************************************************************************************************************************************/ 
      INSERT INTO #tmp_sale_amount_lesser_auction_sale(Stock_ITem_DM_ID,Auction_Schedule_DM_ID )
        SELECT SS.stockid              AS Stock_ITem_DM_ID, 
             AUS.auction_schedule_id AS Auction_Schedule_DM_ID 
      --INTO   #tmp_sale_amount_lesser_auction_sale 
      FROM   [EDW].dbo.dimbiddetail AS BDF WITH (nolock) 
             INNER JOIN bi_asap_rep.dbo.auction_item AI WITH (nolock) 
                     ON AI.auction_item_id = BDF.auctionitemid 
             INNER JOIN bi_asap_rep.dbo.auction_schedule AUS WITH (nolock) 
                     ON AUS.auction_schedule_id = AI.auction_schedule_id 
             INNER JOIN edw.dbo.dimstock SS WITH (nolock) 
                     ON AI.salvage_id = SS.asapsalvageid 
             INNER JOIN [EDW].dbo.dimsolddetail AS SDD WITH (nolock) 
                     ON BDF.auctionitemid = SDD.auctionitemid 
             INNER JOIN #tmp_bid_record BDC WITH (nolock) 
                     ON BDC.stockid = SS.stockid 
             LEFT OUTER JOIN #tmp_awarded_bid_record ABR WITH (nolock) 
                          ON ABR.stockid = SS.stockid 
             LEFT OUTER JOIN #tmp_category_2bb C2B WITH (nolock) 
                          ON C2B.stockid = SS.stockid 
      WHERE  BDF.bidstatuscode = ( CASE 
                                     WHEN BDC.confirm_auction_sale_count >= 
                                          BDC.auction_sale_count THEN 'C' 
                                     ELSE 'S' 
                                   END ) 
             AND SDD.saleamount < BDF.bidamount 
             AND ( ABR.category_type = '1A' 
                    OR C2B.category_type = '2Bb' ) 

      DELETE tbdf 
      FROM   #tmp_bid_detail_provider_source AS TBDF  WITH (NOLOCK)
             INNER JOIN #tmp_sale_amount_lesser_auction_sale AS TSALAS  WITH (NOLOCK)
                     ON tbdf.stockid = TSALAS.stock_item_dm_id 
                        AND tbdf.auction_schedule_id = 
                            TSALAS.auction_schedule_dm_id 

      /******************************************************************************************************************************************  
      - DON'T SHOW RECONCILING STOCKS WHICH ARE NOT SETTLED FROM DAY OF AUCTION PLUS 2 BUSINESS DAYS  
      - NEGOTIATED SALE 'NSC', 'NSA'  
      - DON'T SHOW RECONCILE STOCKS 'ASC', 'SCC'  
      ******************************************************************************************************************************************/ 
      INSERT INTO #tmp_reconcile_sale_date_range 
                  (business_day_id) 
      SELECT TOP 3 Cal_Day.dayid AS Business_Day_ID 
      FROM   [EDW].dbo.dimday AS Cal_Day WITH (nolock) 
      WHERE  Cal_Day.dayid >= CONVERT(CHAR(8), @Last_Run_Start, 112) 
             AND Cal_Day.weekdayflag = 1 
             AND Cal_Day.holidayflag = 0 
      ORDER  BY Cal_Day.dayid 

      DECLARE @Reconcile_Day_ID_Start INT, 
              @Reconcile_Day_ID_End   INT 

      SELECT @Reconcile_Day_ID_Start = Min(business_day_id), 
             @Reconcile_Day_ID_End = Max(business_day_id) 
      FROM   #tmp_reconcile_sale_date_range 

      INSERT INTO #tmp_remove_reconciling_stocks 
                  (auction_schedule_dm_id, 
                   stock_item_dm_id, 
                   auction_datetime, 
                   auction_entry_reason_code, 
                   accept_bid_status_code, 
                   bid_amount, 
                   buyer_id, 
                   entry_datetime, 
                   negotiated_sale_ind, 
                   update_datetime) 
      SELECT ODS_ASCH.auction_schedule_id Auction_Schedule_DM_ID, 
             STK.stockid                  AS Stock_Item_DM_ID, 
             ODS_ASCH.auction_datetime, 
             AE.auction_entry_reason_code, 
             AE.accept_bid_status_code, 
             AE.bid_amount, 
             AE.buyer_id, 
             AE.entry_datetime, 
             AE.negotiated_sale_ind, 
             AE.update_datetime 
      FROM   [BI_ASAP_REP].dbo.auction_entry AS AE WITH (nolock) 
             INNER JOIN [BI_ASAP_REP].dbo.auction_schedule AS ODS_ASCH WITH ( 
                        nolock) 
                     ON AE.auction_schedule_id = ODS_ASCH.auction_schedule_id 
             INNER JOIN [BI_ASAP_REP].dbo.auction_item AS AI WITH (nolock) 
                     ON AE.auction_schedule_id = AI.auction_schedule_id 
                        AND AE.auction_item_id = AI.auction_item_id 
             INNER JOIN [EDw].dbo.dimstock AS STK WITH (nolock) 
                     ON STK.asapsalvageid = AI.salvage_id 
             LEFT OUTER JOIN #tmp_awarded_bid_record ABR WITH (nolock) 
                          ON ABR.stockid = STK.stockid 
             LEFT OUTER JOIN #tmp_category_2bb C2B WITH (nolock) 
                          ON C2B.stockid = STK.stockid 
      WHERE  CONVERT(CHAR(8), ODS_ASCH.auction_datetime, 112) BETWEEN 
                    @Reconcile_Day_ID_Start AND @Reconcile_Day_ID_End 
             AND ( 
                 /*************************************************************************************  
                 CAPTURE RECONCILE STOCKS  
                 *************************************************************************************/ 
                 ( AE.validated_entry_ind = 0 
                   -- CHECKS WHETHER ENTRY IS VALIDATED, RECONCILE = 0???, IF 0 means primary and secondary doesn't match  
                   AND AE.auction_entry_reason_code NOT IN ( 'ASC', 'SCC' ) 
                   -- Secondary and secondary corrected  
                   AND AE.negotiated_sale_ind = 0 -- Currently not negotiated  
                   AND AE.no_bid_ind = 0 -- NO Bid   
                   AND ( ABR.category_type = '1A' 
                          OR C2B.category_type = '2Bb' ) ) 
                  /*************************************************************************************  
                  CAPTURE NEGOTIATED SALE STOCKS  
                  *************************************************************************************/ 
                  OR ( AE.auction_entry_reason_code = 'NSC' ) ) 

      DELETE tbdps 
      FROM   #tmp_bid_detail_provider_source AS TBDPS 
             INNER JOIN #tmp_remove_reconciling_stocks AS TRRS WITH (NOLOCK)
                     ON tbdps.stockid = TRRS.stock_item_dm_id 
                        AND tbdps.auction_schedule_id = 
                            TRRS.auction_schedule_dm_id 

 /*************************************************************************************************************************  
  **************************************************************************************************************************/ 
      /*************************************************************************************************************************************************  
      *************************************************************************************************************************************************  
      1. RULES TO HANDLE CATEGORY 2A  
        -> RUN ONCE, AWARDED TWICE  
      *************************************************************************************************************************************************  
      *************************************************************************************************************************************************/ 
      INSERT INTO #tmp_category_2a_min_sale_sequence(Stock_ITem_DM_ID,
                  Auction_Schedule_DM_ID,
                  MIN_Sale_Sequence_Number)
        SELECT TBDF.stockid             AS Stock_ITem_DM_ID, 
             TBDF.auction_schedule_id AS Auction_Schedule_DM_ID, 
             Min(TBDF.sortorder)      AS MIN_Sale_Sequence_Number 
      --INTO   #tmp_category_2a_min_sale_sequence 
      FROM   #tmp_bid_detail_provider_source AS TBDF  WITH (NOLOCK)
             INNER JOIN #tmp_bid_record BDC WITH (nolock) 
                     ON BDC.stockid = TBDF.stockid 
             LEFT OUTER JOIN #tmp_run_once_awarded_twice AT  WITH (NOLOCK)
                          ON AT.stock_item_dm_id = TBDF.stockid 
      WHERE  AT.category_type = '2A' 
             AND TBDF.bidstatuscode = ( CASE 
                                          WHEN BDC.confirm_auction_sale_count >= 
                                               BDC.auction_sale_count THEN 'C' 
                                          ELSE 'S' 
                                        END ) 
      GROUP  BY TBDF.stockid, 
                TBDF.auction_schedule_id 

            INSERT INTO #tmp_purge_category_2a_entries(Stock_ITem_DM_ID,
                  Auction_Schedule_DM_ID,
                  Bid_Status_Code,
                  Purge_Sequence_Number)
      SELECT TBDF.stockid             AS Stock_ITem_DM_ID, 
             TBDF.auction_schedule_id AS Auction_Schedule_DM_ID, 
             TBDF.bidstatuscode       AS Bid_Status_Code, 
             Max(TBDF.sortorder)      AS Purge_Sequence_Number 
      --INTO   #tmp_purge_category_2a_entries 
      FROM   #tmp_bid_detail_provider_source AS TBDF  WITH (NOLOCK)
             LEFT OUTER JOIN #tmp_run_once_awarded_twice BDC  WITH (NOLOCK)
                          ON BDC.stock_item_dm_id = TBDF.stockid 
             INNER JOIN #tmp_category_2a_min_sale_sequence AS TCMSS  WITH (NOLOCK)
                     ON TBDF.stockid = TCMSS.stock_item_dm_id 
                        AND TBDF.auction_schedule_id = 
                            TCMSS.auction_schedule_dm_id 
      WHERE  BDC.category_type = '2A' 
             AND TBDF.bidstatuscode IN ( 'K', 'S','B','E', 'C', '*' ) 
             AND TBDF.sortorder <= TCMSS.min_sale_sequence_number 
      GROUP  BY TBDF.stockid, 
                TBDF.auction_schedule_id, 
                TBDF.bidstatuscode 

      DELETE tbdf 
      FROM   #tmp_bid_detail_provider_source AS TBDF 
             INNER JOIN #tmp_purge_category_2a_entries AS TPC2E  WITH (NOLOCK)
                     ON tbdf.stockid = TPC2E.stock_item_dm_id 
                        AND tbdf.auction_schedule_id = 
                            TPC2E.auction_schedule_dm_id 
                        AND tbdf.bidstatuscode = TPC2E.bid_status_code 
                        AND tbdf.sortorder = TPC2E.purge_sequence_number 

      /*************************************************************/ 
      INSERT INTO #tmp_category_2b_min_sale_sequence(Stock_ITem_DM_ID,
                  Auction_Schedule_DM_ID,
                  Item_Awarded_Sequence_Number,
                  MIN_New_Item_Sequence_Number,
                  MAX_New_Item_Sequence_Number,
                  MAX_Sequence_Number)
        SELECT TBDF.stockid             Stock_ITem_DM_ID, 
             TBDF.auction_schedule_id Auction_Schedule_DM_ID, 
             Min(CASE 
                   WHEN bidstatuscode IN('S','B','E') THEN TBDF.sortorder 
                 END)                 AS Item_Awarded_Sequence_Number, 
             Min(CASE 
                   WHEN bidstatuscode = '^' THEN TBDF.sortorder 
                 END)                 AS MIN_New_Item_Sequence_Number, 
             Max(CASE 
                   WHEN bidstatuscode = '^' THEN TBDF.sortorder 
                 END)                 AS MAX_New_Item_Sequence_Number, 
             Max(TBDF.sortorder)      AS MAX_Sequence_Number 
      --INTO   #tmp_category_2b_min_sale_sequence 
      FROM   #tmp_bid_detail_provider_source AS TBDF  WITH (NOLOCK)
             LEFT OUTER JOIN #tmp_category_2bb C2B WITH (nolock) 
                          ON TBDF.stockid = C2B.stockid 
      WHERE  C2B.category_type IN ( '2Ba', '2Bb', '2Bx' ) 
      GROUP  BY TBDF.stockid, 
                TBDF.auction_schedule_id 
      
      INSERT INTO #tmp_category_2b_min_sale_sequence_delete(stock_item_dm_id,
            auction_schedule_dm_id,
            Start_Delete_Sequence_Number,
            End_Delete_Sequence_Number,
            DELETE_PREFERENCE)
      SELECT TCMSS.stock_item_dm_id, 
             TCMSS.auction_schedule_dm_id, 
             ( CASE 
                 WHEN TCMSS.item_awarded_sequence_number > 
                      TCMSS.max_new_item_sequence_number 
               THEN min_new_item_sequence_number 
                 WHEN TCMSS.item_awarded_sequence_number < 
                      TCMSS.max_new_item_sequence_number 
               THEN max_new_item_sequence_number 
               END ) AS Start_Delete_Sequence_Number, 
             ( CASE 
                 WHEN TCMSS.item_awarded_sequence_number > 
                      TCMSS.max_new_item_sequence_number 
               THEN max_new_item_sequence_number 
                 WHEN TCMSS.item_awarded_sequence_number < 
                      TCMSS.max_new_item_sequence_number 
               THEN max_sequence_number 
               END ) AS End_Delete_Sequence_Number, 
             ( CASE 
                 WHEN TCMSS.item_awarded_sequence_number > 
                      TCMSS.max_new_item_sequence_number 
               THEN 0 
                 WHEN TCMSS.item_awarded_sequence_number < 
                      TCMSS.max_new_item_sequence_number 
               THEN 1 
               END ) AS DELETE_PREFERENCE 
      --INTO   #tmp_category_2b_min_sale_sequence_delete 
      FROM   #tmp_category_2b_min_sale_sequence AS TCMSS  WITH (NOLOCK)

      --DELETE_PREFERENCE 0  
      DELETE tbdf 
      FROM   #tmp_bid_detail_provider_source AS TBDF 
             INNER JOIN #tmp_category_2b_min_sale_sequence_delete AS TC2MSSD  WITH (NOLOCK)
                     ON tbdf.stockid = TC2MSSD.stock_item_dm_id 
                        AND tbdf.auction_schedule_id = 
                            TC2MSSD.auction_schedule_dm_id 
                        AND tbdf.sortorder >= 
                            TC2MSSD.start_delete_sequence_number 
                        AND tbdf.sortorder < TC2MSSD.end_delete_sequence_number 
      WHERE  TC2MSSD.delete_preference = 0 

      --DELETE_PREFERENCE 1  
      DELETE tbdf 
      FROM   #tmp_bid_detail_provider_source AS TBDF 
             INNER JOIN #tmp_category_2b_min_sale_sequence_delete AS TC2MSSD  WITH (NOLOCK)
                     ON tbdf.stockid = TC2MSSD.stock_item_dm_id 
                        AND tbdf.auction_schedule_id = 
                            TC2MSSD.auction_schedule_dm_id 
                        AND tbdf.sortorder BETWEEN 
                            TC2MSSD.start_delete_sequence_number AND 
                            TC2MSSD.end_delete_sequence_number 
      WHERE  TC2MSSD.delete_preference = 1 

      /*************************************************************************************************************************************************  
      *************************************************************************************************************************************************  
      3. RULES TO HANDLE CATEGORY 2C  
        -> RUN ONCE, AWARDED TWICE  
      *************************************************************************************************************************************************  
      *************************************************************************************************************************************************/ 
      INSERT INTO #tmp_category_2c_sequence(Stock_ITem_DM_ID,
            Auction_Schedule_DM_ID,
            MIN_Sale_Sequence_Number,
            MAX_New_Item_Sequence_Number)
        SELECT TBDF.stockid             Stock_ITem_DM_ID, 
             TBDF.auction_schedule_id AS Auction_Schedule_DM_ID, 
             Min(CASE 
                   WHEN bidstatuscode IN ('S','B','E') THEN TBDF.sortorder 
                 END)                 AS MIN_Sale_Sequence_Number, 
             Max(CASE 
                   WHEN bidstatuscode = '^' THEN TBDF.sortorder 
                 END)                 AS MAX_New_Item_Sequence_Number 
      --INTO   #tmp_category_2c_sequence 
      FROM   #tmp_bid_detail_provider_source AS TBDF  WITH (NOLOCK)
             LEFT OUTER JOIN #tmp_run_twice_awarded_twice AS BDC WITH (nolock) 
                          ON TBDF.stockid = BDC.stock_item_dm_id 
      WHERE  BDC.category_type = '2C' 
      GROUP  BY TBDF.stockid, 
                TBDF.auction_schedule_id 

      INSERT INTO #tmp_purge_category_2c_entries(Stock_ITem_DM_ID,
            Auction_Schedule_DM_ID,
            Purge_Sequence_Number,
            min_sale_sequence_number,
            max_new_item_sequence_number)
        SELECT TBDF.stockid             AS Stock_ITem_DM_ID, 
             TBDF.auction_schedule_id AS Auction_Schedule_DM_ID, 
             Max(TBDF.sortorder)      AS Purge_Sequence_Number, 
             TC2S.min_sale_sequence_number, 
             TC2S.max_new_item_sequence_number 
      --INTO   #tmp_purge_category_2c_entries
      FROM   #tmp_bid_detail_provider_source AS TBDF  WITH (NOLOCK)
             LEFT OUTER JOIN #tmp_run_twice_awarded_twice AS BDC WITH (nolock) 
                          ON TBDF.stockid = BDC.stock_item_dm_id 
             INNER JOIN #tmp_category_2c_sequence AS TC2S  WITH (NOLOCK)
                     ON TBDF.stockid = TC2S.stock_item_dm_id 
                        AND TBDF.auction_schedule_id = 
                            TC2S.auction_schedule_dm_id 
      WHERE  BDC.category_type = '2C' 
             AND TBDF.bidstatuscode IN ( 'K' ) 
             AND TBDF.sortorder <= TC2S.min_sale_sequence_number 
      GROUP  BY TBDF.stockid, 
                TBDF.auction_schedule_id, 
                TC2S.min_sale_sequence_number, 
                TC2S.max_new_item_sequence_number 

      DELETE tbdf 
      FROM   #tmp_bid_detail_provider_source AS TBDF 
             INNER JOIN #tmp_purge_category_2c_entries AS TPC2E  WITH (NOLOCK)
                     ON tbdf.stockid = TPC2E.stock_item_dm_id 
                        AND tbdf.auction_schedule_id = 
                            TPC2E.auction_schedule_dm_id 
                        AND tbdf.sortorder BETWEEN TPC2E.purge_sequence_number AND TPC2E.max_new_item_sequence_number 
                        
     -----Deleting Second Record of Counter Offer 'E' 
                        
       DELETE TBDPS
       --SELECT * 
       FROM #tmp_bid_detail_provider_source TBDPS WITH (NOLOCK)
       INNER JOIN EDW.dbo.DimBidDetail DBD WITH (NOLOCK)
       ON TBDPS.BidStatusCode = DBD.BidStatusCode
       AND TBDPS.Bid_Event_Datetime = DBD.BidEventDatetime
       WHERE DBD.GroupNumber <>1
       AND TBDPS.bidstatuscode = 'E'                   

      BEGIN TRAN 

      INSERT INTO [EDW].dbo.bid_detail_provider 
                  (bid_detail_filtered_dm_id, 
                   bid_log_item, 
                   asap_auction_schedule_id, 
                   auction_datetime, 
                   bid_amount, 
                   bid_event_datetime, 
                   bid_status_code, 
                   bid_status_description_mapping, 
                   branch_number, 
                   asap_provider_id, 
                   asap_salvage_id, 
                   displayname, 
                   displaylocation, 
                   sort_order, 
                   winning_bid_ind, 
                   device_type) 
      SELECT TBDPS.bid_detail_categorized_dm_id AS Bid_Detail_Filtered_DM_ID, 
             TBDPS.bidlogitem                   AS Bid_Log_Item, 
             TBDPS.auction_schedule_id          AS ASAP_Auction_Schedule_ID, 
             TBDPS.auction_datetime, 
             TBDPS.bidamount                    AS Bid_Amount, 
             TBDPS.bid_event_datetime, 
             TBDPS.bidstatuscode                AS Bid_Status_Code, 
             TBDPS.bid_status_display_mapping, 
             TBDPS.branchnumber                 AS Branch_Number, 
             TBDPS.providerid                   AsASAP_Provider_ID, 
             TBDPS.asapsalvageid                AS ASAP_Salvage_ID, 
             TBDPS.displayname, 
             TBDPS.displaylocation              AS DisplayLocation, 
             TBDPS.sortorder                    Sorting_Order, 
             TBDPS.winning_bid_ind, 
             TBDPS.devicetype                   AS Device_Type --19254  
      FROM   #tmp_bid_detail_provider_source AS TBDPS  WITH (NOLOCK)
        LEFT OUTER JOIN [EDW].dbo.bid_detail_provider BDP WITH (NOLOCK)
            ON TBDPS.bid_detail_categorized_dm_id = BDP.bid_detail_filtered_dm_id
     
COMMIT TRAN 

      IF Object_id('tempdb..#tmp_bid_record') IS NOT NULL DROP TABLE #tmp_bid_record
      IF Object_id('tempdb..#tmp_awarded_bid_record') IS NOT NULL DROP TABLE #tmp_awarded_bid_record
      IF Object_id('tempdb..#tmp_run_once_awarded_twice') IS NOT NULL DROP TABLE #tmp_run_once_awarded_twice
      IF Object_id('tempdb..#tmp_run_twice_awarded_twice') IS NOT NULL DROP TABLE #tmp_run_twice_awarded_twice
      IF Object_id('tempdb..#tmp_run_twice_awarded_once') IS NOT NULL DROP TABLE #tmp_run_twice_awarded_once
      IF Object_id('tempdb..#tmp_capture_new_item_event_sequence ') IS NOT NULL DROP TABLE #tmp_capture_new_item_event_sequence
      IF Object_id('tempdb..#tmp_sub_category_for_category_2b') IS NOT NULL DROP TABLE #tmp_sub_category_for_category_2b
      IF Object_id('tempdb..#tmp_category_2bb') IS NOT NULL DROP TABLE #tmp_category_2bb
      IF Object_id('tempdb..#tmp_bid_detail_provider_source') IS NOT NULL DROP TABLE #tmp_bid_detail_provider_source
      IF Object_id('tempdb..#tmp_sale_amount_lesser_auction_sale') IS NOT NULL DROP TABLE #tmp_sale_amount_lesser_auction_sale
      IF Object_id('tempdb..#tmp_category_2a_min_sale_sequence') IS NOT NULL DROP TABLE #tmp_category_2a_min_sale_sequence
      IF Object_id('tempdb..#tmp_purge_category_2a_entries') IS NOT NULL DROP TABLE #tmp_purge_category_2a_entries
      IF Object_id('tempdb..#tmp_category_2b_min_sale_sequence') IS NOT NULL DROP TABLE #tmp_category_2b_min_sale_sequence
      IF Object_id('tempdb..#tmp_category_2b_min_sale_sequence_delete') IS NOT NULL DROP TABLE #tmp_category_2b_min_sale_sequence_delete
      IF Object_id('tempdb..#tmp_category_2c_sequence') IS NOT NULL DROP TABLE #tmp_category_2c_sequence
      IF Object_id('tempdb..#tmp_purge_category_2c_entries') IS NOT NULL DROP TABLE #tmp_purge_category_2c_entries
      IF Object_id('tempdb..#tmp_reconcile_sale_date_range') IS NOT NULL DROP TABLE #tmp_reconcile_sale_date_range
      IF Object_id('tempdb..#tmp_remove_reconciling_stocks ') IS NOT NULL DROP TABLE #tmp_remove_reconciling_stocks
      IF Object_id('tempdb..#DimSoldDetail') IS NOT NULL DROP TABLE #DimSoldDetail

      RETURN 0
END TRY

BEGIN CATCH

      SET NOCOUNT OFF
      IF @@TRANCOUNT > 0 
            ROLLBACK TRAN
            
            DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
            SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
            RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
            
      IF Object_id('tempdb..#tmp_bid_record') IS NOT NULL DROP TABLE #tmp_bid_record
      IF Object_id('tempdb..#tmp_awarded_bid_record') IS NOT NULL DROP TABLE #tmp_awarded_bid_record
      IF Object_id('tempdb..#tmp_run_once_awarded_twice') IS NOT NULL DROP TABLE #tmp_run_once_awarded_twice
      IF Object_id('tempdb..#tmp_run_twice_awarded_twice') IS NOT NULL DROP TABLE #tmp_run_twice_awarded_twice
      IF Object_id('tempdb..#tmp_run_twice_awarded_once') IS NOT NULL DROP TABLE #tmp_run_twice_awarded_once
      IF Object_id('tempdb..#tmp_capture_new_item_event_sequence ') IS NOT NULL DROP TABLE #tmp_capture_new_item_event_sequence
      IF Object_id('tempdb..#tmp_sub_category_for_category_2b') IS NOT NULL DROP TABLE #tmp_sub_category_for_category_2b
      IF Object_id('tempdb..#tmp_category_2bb') IS NOT NULL DROP TABLE #tmp_category_2bb
      IF Object_id('tempdb..#tmp_bid_detail_provider_source') IS NOT NULL DROP TABLE #tmp_bid_detail_provider_source
      IF Object_id('tempdb..#tmp_sale_amount_lesser_auction_sale') IS NOT NULL DROP TABLE #tmp_sale_amount_lesser_auction_sale
      IF Object_id('tempdb..#tmp_category_2a_min_sale_sequence') IS NOT NULL DROP TABLE #tmp_category_2a_min_sale_sequence
      IF Object_id('tempdb..#tmp_purge_category_2a_entries') IS NOT NULL DROP TABLE #tmp_purge_category_2a_entries
      IF Object_id('tempdb..#tmp_category_2b_min_sale_sequence') IS NOT NULL DROP TABLE #tmp_category_2b_min_sale_sequence
      IF Object_id('tempdb..#tmp_category_2b_min_sale_sequence_delete') IS NOT NULL DROP TABLE #tmp_category_2b_min_sale_sequence_delete
      IF Object_id('tempdb..#tmp_category_2c_sequence') IS NOT NULL DROP TABLE #tmp_category_2c_sequence
      IF Object_id('tempdb..#tmp_purge_category_2c_entries') IS NOT NULL DROP TABLE #tmp_purge_category_2c_entries
      IF Object_id('tempdb..#tmp_reconcile_sale_date_range') IS NOT NULL DROP TABLE #tmp_reconcile_sale_date_range
      IF Object_id('tempdb..#tmp_remove_reconciling_stocks ') IS NOT NULL DROP TABLE #tmp_remove_reconciling_stocks
      IF Object_id('tempdb..#DimSoldDetail') IS NOT NULL DROP TABLE #DimSoldDetail

            RETURN -1
      

END CATCH

END








GO


