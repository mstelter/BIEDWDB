/* 
USE EDW
GO 

==============================================================================
Author            : Juan X Jacome
Create date : 05/15/2012
Description : Populate the EDW FactMinimumBidDetail table every day
==============================================================================
REVISION History
08/01/2013      svelupal          Added CallerName & Changed Logic for MinimumBidSource
16/01/2013      svelupal          Changed datatype for date in the Join to salvagenote table
19/01/2013      svelupal          Added Case statement for ASAPUpdatedUserID
6/18/2013            mstelter             Change logic for Min Bid User
11/12/2013           mstelter             Change logic for Min Bid User
03/13/2014           bkutnick             Change datetype for [Max_Cddate]  from date to datetime
03/13/2014           bkutnick             Changed INSERT INTO #TMP_CSADATE_Data statement, 
                                                       replacing [BI_CSA_Rep].[dbo].[csat_Auto_MinimumBid_Update_History_New] with
                                                       dbo.CSATAutoMinimumBidUpdateHistoryNew
03/13/2014           bkutnick             Changed UPDATE TMD statement, replacing [BI_CSA_Rep].[dbo].[csat_Auto_MinimumBid_Update_History_New] with
                                                       dbo.CSATAutoMinimumBidUpdateHistoryNew and replacing the update of ASAP_Minimum_Bid_Amount with data from
                                                       #TMP_Salvage_MinBidData 
09/24/2014           BKutnick             Modified structure and code to correct for large number of missing MinimumBidCallerName entries.
                                                       1. Added fields TMP_SPI_Data_UpDate_DateTime and NoteInfo which while technically not necessary now will be useful for auditing.
                                                       2. Added table #TMP_FactMinimumBidDetail_CallerName2; this table will replace #TMP_FactMinimumBidDetail_CallerName as the source
                                                          of the CallerName attribute that will be loaded into the fact table. 
                                                       3. Modified logic for selection into #TMP_FactMinimumBidDetail_CallerName.  Added another text search phrase that was missing
                                                          ('The Minimum Bid has been updated from%'), and MORE IMPORTANT, removed the rule where the Salvage Note and Salvage Provider Info record
                                                          had to be created at the same minute.  THIS IS ROOT CAUSE OF MOST OF THE MISSING DATA.
                                                       4. Created logic to find the Salvage Note that was created CLOSEST IN TIME to that of the Salvage Provider record update. This is the record that 
                                                          we use to find the Caller Name and it is loaded into the new table #TMP_FactMinimumBidDetail_CallerName2.
                                                       5. The insert into table #TMP_FactMinimumBidDetail uses #TMP_FactMinimumBidDetail_CallerName2 instead of #TMP_FactMinimumBidDetail_CallerName and more important,
                                                          the restriction of the matching time between Salvage Provier Info update and Salvage Note Update has been removed.
11/10/2014           JJacome                    Updated logic to #TMP_FactMinimumBidDetail table. Use SalvageNoteUpdateDateTime instead of SalvageProvider_Info.Update_DateTime
04/24/2015           BKutnick             Total revision, using Salvage Note as the primary driver
07/22/2015           Juan X. Jacome       - Update logic to Amount for #DataList
07/24/2015      Pratyusha Koduru Update MinimumBidSource to Show User Override if there was a prior minimum bid on a Stock 
8/14/2015		Matt Stelter		Added dl.Note_Text like '%I-Buy Fast removed%' AND dl.NOTE_TEXT LIKE '%by CSAToday, Automated Min Bid Update%' as Auto
11/02/2015      Pratyusha Koduru    Update MinimumBidOriginalAmount to show the First MinimumBidAmount if MinimumBidAmount > 0 
2/26/2016		Matt Stelter		Changed Min Bid User to set all to User Override if after Automatic but not after Call
6/08/2016       Pratyusha Koduru    Updated MinimumBidSource to UserOverride if there was a prior minimum bid on a Stock to the note 'I-Buy Fast Price/Minimum Bid set to $%' 
=============================================================================
Usage Example:
Exec usp_ETLFactMinimumBidDetail
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactMinimumBidDetail]

AS     

BEGIN


DECLARE @ETLLoadID INT,
              @ExtractRowCount INT = 0,
              @InsertRowCount INT,
              @UpdateRowCount INT = 0,
              @DeleteRowCount INT = 0,
              ----@StartLSN BINARY(10),
              ----@EndLSN BINARY(10)
              @StartDate datetime = DATEADD(dd, -40, GETDATE())


SET NOCOUNT ON;

----Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactMinimumBidDetail'



SELECT sn.Salvage_Id, 
              sn.Salvage_Note_ID,
              [Update_DateTime],
              sn.Update_User_ID,
              ds.StockID,
              a.ASAPSalvageNoteID FMBDSalvageNoteId,
              b.StockID FMBDStockID,
              CASE
                     WHEN c.StockID IS NULL
                           THEN 0
                     ELSE 1
              END AS StockExistsInFMBDFlag,
              sn.Note_Text,
              0 StartAt,
              0 EndAt,
              CONVERT(money, 0.00) Amount
INTO   #DataList
FROM   BI_ASAP_REP.dbo.Salvage_Note sn WITH (NOLOCK)
              INNER JOIN EDW.dbo.DimStock ds WITH (NOLOCK)
                     ON ds.ASAPSalvageID = sn.Salvage_ID
              LEFT OUTER JOIN 
                           (
                           SELECT DISTINCT fmbd2.ASAPSalvageNoteID
                           FROM   EDW.dbo.FactMinimumBidDetail fmbd2 WITH (NOLOCK)
                                         INNER JOIN EDW.dbo.DimStock ds2 WITH (NOLOCK)
                                                ON ds2.StockID = fmbd2.StockID
                           WHERE  NOT ASAPSalvageID IS NULL
                           ) a
                     ON a.ASAPSalvageNoteID = sn.Salvage_Note_Id
              LEFT OUTER JOIN
                           (
                           SELECT fmbd3.stockID,
                                         fmbd3.[ASAPUpdateDate] 
                           FROM   EDW.dbo.FactMinimumBidDetail fmbd3 WITH (NOLOCK)
                           --WHERE       NOT ASAPSalvageID IS NULL
                           GROUP BY
                                         stockID,
                                         [ASAPUpdateDate] 
                           ) b
                     ON     b.StockID = ds.StockID
                     AND    b.[ASAPUpdateDate] = sn.[Update_DateTime]
              LEFT OUTER JOIN
                           (
                           SELECT DISTINCT fmbd4.stockID
                           FROM   EDW.dbo.FactMinimumBidDetail fmbd4 WITH (NOLOCK)
                           ) c
                     ON c.StockID = ds.stockID
WHERE  sn.Update_DateTime > = @StartDate
AND           [Salvage_Event_Category_Code] = 'MIN'



/**********************************************************************************************************************************************************/
/*
              We have to determine if this minimum bid note already exists within the FMBD table.  To do this with the highest degree of accuracy
              we need to parse out the amount from the note we just gathered into our targetset (#Datalist).  If the StockIDs, ASAPUpdateDates and
              amounts ALL match between our targetset (#DataList) and the most recent MinimumBidCurrentAmount in the FMBD table we want to DELETE
              from our targetset, as this will be a duplicate record being inserted.

              There will be at least 2 other DEDUPING sections of code further on down the old codestream!

                                                                                                                                                                                                                                                                       */
/**********************************************************************************************************************************************************/

-- start the parsing of the targetset data
UPDATE #DataList
SET           StartAt =
              CASE
                     WHEN Left ( Note_Text,1) = '$' and  Note_Text like '%Minimum Bid placed during assignment%' THEN 2
                     WHEN  Note_text like 'I-Buy Fast Price/Minimum Bid changed from $%' AND  Note_text like '%Minimum Bid to $%' THEN   (CHARINDEX('to $', Note_text))
                     WHEN  Note_text like  'I-Buy Fast Price/Minimum Bid set to $%' THEN (CHARINDEX('to $', Note_text))
                     WHEN  Note_text like 'Minimum bid set to $%' THEN  (CHARINDEX('to $', Note_text))
                     WHEN  Note_text like 'The Minimum Bid has been updated from $%'  AND  Note_text like '%to $%' THEN (CHARINDEX('to $', Note_text))
                     WHEN  Note_text like '%The minimum bid was updated from $%' AND   Note_text like '%to $%' THEN   (CHARINDEX('to $', Note_text))   --NEEDS UPDATE!
                     WHEN  Note_text like 'The minimum bid was updated from Call: Yes to $%' THEN (CHARINDEX('to $', Note_text))
                     ELSE 0
              END  

UPDATE #DataList
SET           EndAt =              
              CASE
                     WHEN Left ( Note_Text,1) = '$' and  Note_Text like '%Minimum Bid placed during assignment%' THEN (CHARINDEX(' Minimum', Note_text)) - 1 
                     WHEN  Note_text like 'I-Buy Fast Price/Minimum Bid changed from $%' AND  Note_text like '%Minimum Bid to $%' THEN LEN(LEFT(Note_text,  (CHARINDEX(' by', Note_text))))  
                     WHEN  Note_text like  'I-Buy Fast Price/Minimum Bid set to $%' THEN  LEN(LEFT( Note_text,  (CHARINDEX(' Minimum Bid',  Note_text))))
                     WHEN  Note_text like 'Minimum bid set to $%' THEN LEN(LEFT(Note_text,  (CHARINDEX(' by',  Note_text))))
                     WHEN  Note_text like 'The Minimum Bid has been updated from $%'  AND  Note_text like '%to $%' THEN LEN(LEFT( Note_text,  (CHARINDEX('on',  Note_text))))
                     WHEN  Note_text like '%The minimum bid was updated from $%' AND   Note_text like '%to $%' THEN   LEN(LEFT( Note_text,  (CHARINDEX('on',  Note_text))))  --NEEDS UPDATE!
                     WHEN  Note_text like 'The minimum bid was updated from Call: Yes to $%' THEN LEN(LEFT( Note_text,  (CHARINDEX(' on',  Note_text))))
                     ELSE 0
              END 

UPDATE #DataList
SET           StartAt = startAt+4
WHERE  Note_Text like 'The minimum bid was updated from $%' AND Note_Text like '%to $%'

UPDATE #DataList
SET           EndAt = EndAt-2
WHERE  Note_Text like 'The minimum bid was updated from $%' AND Note_Text like '%to $%'


--SELECT * FROM #datalist order by Note_Text 
UPDATE #DataList
SET           Amount =
              CASE
                     WHEN  Note_Text like '%minimum Bid Placed During Assignment%' THEN  CONVERT (money,(RIGHT (LEFT(Note_Text, EndAt),(Endat- (StartAt-1)))  ))
                     WHEN  Note_text like 'I-Buy Fast Price/Minimum Bid changed from $%'  THEN  CONVERT (money, (RIGHT(LEFT(Note_Text, EndAt),(Endat- (StartAt+3)))))    
                     WHEN  Note_text like  'I-Buy Fast Price/Minimum Bid set to $%' THEN    CONVERT (money, (RIGHT(LEFT(Note_Text, EndAt)    ,(Endat- (StartAt + 3 )    )) ))
                     WHEN  Note_text like 'Minimum bid set to $%' THEN  CONVERT (money, (RIGHT(LEFT(Note_Text, EndAt),(Endat- (StartAt + 3)))))
                     WHEN  Note_text like 'The Minimum Bid has been updated from $%'  AND  Note_text like '%to $%' THEN  CONVERT (money,(RIGHT(LEFT(Note_Text, EndAt-2),(Endat- (StartAt +5  ) )) ))
                     WHEN  Note_text like '%The minimum bid was updated from $%' AND   Note_text like '%to $%' AND Note_text NOT Like '%to Call:%' THEN CONVERT (money, (RIGHT(LEFT(Note_Text, EndAt),(Endat- (StartAt-1)))))
                     WHEN  Note_text like 'The minimum bid was updated from Call: Yes to $%' THEN CONVERT (money, (RIGHT     (LEFT(Note_Text, EndAt),(Endat- (StartAt + 3)   ))))
                           ELSE 0
                     END                                                                                                                                                                                             

/* If this salvage Note ID is in FSMBD table and amounts match dont bring it in */
SELECT a.StockID,
              a.MinimumBidCurrentAmount,
             a.ASAPUpdateDate,
              b.Update_DateTime,
              b.Salvage_ID,
              b.Salvage_Note_ID
INTO   #FSMBExists
FROM   EDW.dbo.FactMinimumBidDetail a
              INNER JOIN #dataList b WITH (NOLOCK)
                     ON b.FMBDStockID = a.StockID
                     AND    b.Amount = a.MinimumBidCurrentAmount 
                     AND b.Update_DateTime = a.ASAPUpdateDate
                     order by StockID

DELETE  
FROM   #DataList 
WHERE  Salvage_Note_ID IN (select Salvage_Note_ID FROM #FSMBExists) 
AND           FMBDStockID IS NOT NULL



/*
-- is the amount in #dataList the same as in FSMB - if so delete from DataList   otherwise do not delete 
*/

/*********************************************************************************************************/
/*
    Start mapping notes to a Minimum Bid Source.
       Based upon the note text we map to a minimum bid source.  We are also setting default values 
       (0) for StartAt and EndAt attributes, which are used to determine where our parsing starts and
       ends to find caller name.   We also set the default value of the amount to 0.00 and will update
       this value later in the codestream.                                           
                                                                                                                                                                                  */
/********************************************************************************************************/
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              1 NoteType,
              dl.Note_Text,
              'Automatic' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag
INTO   #t1
FROM   #DataList dl WITH (NOLOCK) 
WHERE  dl.Note_Text like '%Minimum bid set to $%' AND dl.NOTE_TEXT LIKE '%by CSAToday, Automated Min Bid Update%'  -- Scenario #1
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              Update_User_ID,
              2 NoteType,
              dl.Note_Text,
              'Automatic' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag 
FROM   #DataList dl WITH (NOLOCK) 
WHERE  dl.Note_Text like '%I-Buy Fast Price/Minimum Bid changed from $%' AND dl.NOTE_TEXT LIKE '%by CSAToday, Automated Min Bid Update%'  -- Scenario #2
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              Update_User_ID,
              2 NoteType,
              dl.Note_Text,
              'Automatic' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag 
FROM   #DataList dl WITH (NOLOCK) 
WHERE	dl.Note_Text like '%I-Buy Fast removed%' AND dl.NOTE_TEXT LIKE '%by CSAToday, Automated Min Bid Update%'
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              3 NoteType,
              dl.Note_Text,
              'User Override' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag  
FROM   #DataList dl WITH (NOLOCK)
WHERE  dl.Note_Text like '%I-Buy Fast Price/Minimum Bid changed from $%' AND (dl.NOTE_TEXT NOT LIKE '%CSAToday%' OR dl.NOTE_TEXT NOT LIKE '%EDI%' ) AND Update_User_ID <> 626   -- Scenario #3
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              4 NoteType,
              dl.Note_Text,
              'User' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag
FROM   #DataList dl WITH (NOLOCK) 
WHERE  dl.Note_Text like 'Minimum bid set to $%' AND dl.NOTE_TEXT NOT LIKE '%CSAToday%' AND dl.NOTE_TEXT NOT LIKE '%EDI%' AND dl.NOTE_TEXT NOT LIKE 'I Buy Fast%'  -- Scenario #4
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              5 NoteType,
              dl.Note_Text,
              'User' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag
FROM   #DataList dl WITH (NOLOCK)
WHERE  dl.Note_Text like '%Minimum Bid placed during assignment on%' -- Scenario #5
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              6 NoteType,
              dl.Note_Text,
              'User Override' MinBidSource,                                                          
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag
FROM   #DataList dl WITH (NOLOCK)
WHERE  dl.Note_Text like '%The minimum bid was updated from $%' 
              AND dl.Note_text NOT LIKE '%to Call:%'                                            -- Scenario #6
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              7 NoteType,
              dl.Note_Text,
              'User' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag  
FROM   #DataList dl WITH (NOLOCK) 
WHERE  dl.Note_Text like 'Minimum bid set to $%' AND dl.NOTE_TEXT  LIKE '%EDI%' AND dl.Update_User_ID = 3 -- Scenario #7
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              8 NoteType,
              dl.Note_Text,
              'Call' MinBidSource ,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag
FROM   #DataList dl WITH (NOLOCK)
WHERE  dl.Note_Text like '%Minimum Bid was placed as call%'   --- scenario #8, Original Bid $0 
----UNION ALL
----SELECT    sn.Salvage_ID,
----          sn.Salvage_Note_ID,
----          sn.Update_DateTime,
----          Convert(DateTime, DATEDIFF(DAY, 0, sn.Update_DateTime)) DateAtMidNight,
----          sn.Update_User_ID,
----          9 NoteType,
----          sn.Note_Text,
----          'Automatic' MinBidSource,
----          0 StartAt,
----          0 EndAt,
----          0.00 Amount  
----FROM      BI_ASAP_REP.dbo.Salvage_NOTE sn WITH (NOLOCK)
----          INNER JOIN #DataList dl WITH (NOLOCK)
----                 ON dl.Salvage_ID = sn.Salvage_ID
----WHERE     sn.Note_Text like 'I-Buy Fast Price/Minimum Bid changed from $%' AND   sn.Note_text LIKE '%by CSAToday%'  -- Scenario #9
UNION ALL
SELECT  dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              Update_User_ID,
              10 NoteType,
              dl.Note_Text,
              'User' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag 
FROM   #DataList dl WITH (NOLOCK)        
WHERE  dl.Note_Text like '%The minimum bid was updated from Call: Yes to $%'  -- Scenario #10
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              11 NoteType,
              dl.Note_Text,
              'Call',
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag
FROM   #DataList dl WITH (NOLOCK) 
WHERE  dl.Note_Text like '%The minimum bid was updated from $%' AND dl.Note_Text LIKE '%to Call: Yes%'    -- Scenario #11
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              Update_User_ID,
              12 NoteType,
              dl.Note_Text,
              'UPDATE' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag  
FROM   #DataList dl WITH (NOLOCK)
WHERE  dl.Note_Text like  'I-Buy Fast Price/Minimum Bid set to $%' And dl.Update_User_ID NOT IN (3, 626)  -- Scenario #12
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              dl.Update_User_ID,
              13 NoteType,
              dl.Note_Text,
              'Call' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag
FROM   #DataList dl WITH (NOLOCK)  
WHERE  dl.Note_Text like '%The minimum bid was updated from Call: No to Call: Yes%'  -- Scenario #13
UNION ALL
SELECT dl.Salvage_ID,
              dl.Salvage_Note_ID,
              dl.Update_DateTime,
              Convert(DateTime, DATEDIFF(DAY, 0, dl.Update_DateTime)) DateAtMidNight,
              Update_User_ID,
              14 NoteType,
              dl.Note_Text,
              'User Override' MinBidSource,
              0 StartAt,
              0 EndAt,
              0.00 Amount,
              dl.StockID,
              dl.StockExistsInFMBDFlag  
FROM   #DataList dl WITH (NOLOCK) 
WHERE  dl.Note_Text like  'The Minimum Bid has been updated from $%' And dl.Note_Text like '%PA Sale Management Tool%'  -- Scenario 14


/***************************************************************************************/
/*     Group the data      and parse out a contact name (or get it from DimUser).                  */
/*                                                                                                                                                 */
/***************************************************************************************/
SELECT t1.Salvage_ID,
              MAX(t1.Salvage_Note_ID) Salvage_Note_ID,
              t1.Update_DateTime,
              t1.DateAtMidNight,
              t1.Update_User_ID,
              t1.NoteType,
              t1.Note_Text,
             sn2.Salvage_Note_ID Salvage_Note_IDCallerName,
              CASE   
                     WHEN (sn3.note_text LIKE 'Minimum Bid Contact Information:%') 
                           THEN   Substring(Substring(sn3.note_text, 34, 100), 0, (Charindex('Phone', Substring(sn3.note_text, 34, 100))-2))
              ELSE du.FirstName + ' ' + du.LastName    
              END AS CallerName,
              t1.MinBidSource,
              0 StartAt,
              0 EndAt,
              CONVERT(money, 0.00)  Amount,
              t1.StockID,
              MAX(t1.StockExistsInFMBDFlag)  StockExistsInFMBDFlag

INTO      #group1  -- select * from #group1  Update_User_ID
FROM   #t1 t1
              LEFT OUTER JOIN
                           (
                           SELECT Salvage_ID,
                                         MAX(Salvage_Note_ID) Salvage_Note_ID,
                                         MAX(Convert(DateTime, DATEDIFF(DAY, 0, Update_DateTime)) ) Update_dateTime
                           FROM  BI_ASAP_Rep.dbo.Salvage_Note  sn2 WITH (NOLOCK)
                           WHERE sn2.Note_text like '%minimum Bid Contact Information%'
                           GROUP BY
                                         Salvage_ID
                           ) sn2
                     ON     sn2.Salvage_ID =  t1.Salvage_ID
                     AND    sn2.Update_dateTime = t1.DateAtMidNight
              LEFT OUTER JOIN 
                           (
                           SELECT Salvage_Note_ID,
                                         Note_text
                           FROM   BI_ASAP_Rep.dbo.Salvage_Note  sn3 WITH (NOLOCK)
                           WHERE  SN3.Salvage_Event_Category_Code = 'MIN'
                           AND           SN3.Note_Text like '%Contact%'
                           ) SN3
                     ON sn3.Salvage_Note_ID = sn2.Salvage_Note_ID
              INNER JOIN EDW.dbo.DimUser du WITH (NOLOCK)
                     ON du.UserID = t1.Update_User_ID
              INNER JOIN EDW.dbo.DimStock DS with (NOLOCK)
                     ON DS.ASAPSalvageID = t1.Salvage_ID
              LEFT OUTER JOIN  
                           (
                           SELECT DISTINCT StockID FROM EDW.[dbo].[FactMinimumBidDetail] FMBD with (NOLOCK)
                           ) FMBD
                     ON FMBD.StockID = ds.StockID
                     
GROUP BY
              t1.Salvage_ID,
              t1.Salvage_Note_ID,
              t1.Update_DateTime,
              t1.DateAtMidNight,
              t1.Update_User_ID,
              t1.NoteType,
              t1.Note_Text,
             sn2.Salvage_Note_ID,
              sn3.Note_Text,
              du.LastName,
              du.FirstName,
              t1.MinBidSource,
              t1.StockID 

/*************************************************************************/
/*  Start the work for parsing out the amounts. We put in an extra          */
/*  catch to insure that the amount is numeric.                                        */
/*  We repeat the process for each note type.                                          */
/*************************************************************************/
UPDATE #group1 
SET           StartAt = (CHARINDEX('$',Note_text)),
              EndAt = LEN(LEFT(Note_text,  (CHARINDEX(' by CSAToday', Note_text))))
WHERE  NoteType = 1  ---  'Minimum bid set to $xxx by CSAToday, Automated Min Bid Update'  

UPDATE #group1 
SET           Amount = RIGHT
                                  (
                                  LEFT(Note_Text, EndAt)
                                  ,(Endat- (StartAt))
                                  )
              WHERE  noteType = 1 
AND    ISNUMERIC
              (
              RIGHT
                                  (
                                  LEFT(Note_Text, EndAt)
                                  ,(Endat- (StartAt))
                                  )
              )  = 1



/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType2) */
UPDATE #group1 
SET           StartAt = (CHARINDEX('to $', Note_text)) + 4 ,
              EndAt = LEN(LEFT(Note_text,  (CHARINDEX(' by CSAToday',  Note_text))))
WHERE  NoteType =2   -- I-Buy Fast Price/Minimum Bid changed from $xxx Minimum Bid to $xxx by CSAToday, Automated Min Bid Update 

UPDATE #group1 
SET           Amount =
              RIGHT
                     (
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1))
                     )
WHERE  noteType = 2 
and ISNumeric
(
RIGHT
                     (
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1))
                     )
)= 1

       
  
/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType3) */
UPDATE #group1 
SET           StartAt = (CHARINDEX('to $',Note_text)),
              EndAt = LEN(LEFT(Note_text,  (CHARINDEX(' by', Note_text))))  
WHERE  NoteType = 3    ----I-Buy Fast Price/Minimum Bid changed from $xxx Minimum Bid to $xxx by <lastname, firstname>

UPDATE #group1 
SET           Amount =
              RIGHT
                     (
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt+3))
                     )
where noteType = 3  
and ISNumeric
(
  RIGHT
                     (
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt+3))
                     )
)= 1



/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType4) */
UPDATE #group1 
SET           StartAt =   (CHARINDEX('to $', Note_text)),
              EndAt = LEN(LEFT(Note_text,  (CHARINDEX(' by',  Note_text))))
WHERE  NoteType = 4  ---Minimum bid set to $2000.00 by <LastName, FirstName> on Oct 19 2014 10:15:10 PM     
 

UPDATE #group1 
SET           Amount =
              RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt+ 3 )
                     ))
WHERE  noteType = 4  
and ISNumeric
(
   RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt+ 3 )
                     ))
)= 1



/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType5) */
UPDATE #group1 
SET           StartAt = 2,
              EndAt = (CHARINDEX(' Minimum', Note_text)) - 1 
WHERE  NoteType = 5   ---$xxx.00 Minimum Bid placed during assignment on Sep 12 2014 11:11AM



DELETE  from #group1 where notetype = 5
and ISNumeric
(
RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1)
                     )) 
)= 0

UPDATE #group1 
SET           Amount =
              RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1)
                     ))
WHERE  noteType = 5   
AND ISNumeric
(
RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1)
                     ))
)= 1



/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType6) */
UPDATE #group1 
SET           StartAt = (CHARINDEX('to $', Note_text)),  
              
              EndAt =  LEN(LEFT( Note_text,  (CHARINDEX('on',  Note_text))))
WHERE  NoteType = 6   ---The minimum bid was updated from $xx.xx to $xx.xx on Sep 12 2014 11:56AM. 

-- select * from #group1 WHERE    NoteType = 6
UPDATE #group1 
SET           StartAt = startAt+4
WHERE  NoteType = 6

UPDATE #group1
SET           EndAt = EndAt-2
WHERE  NoteType = 6

UPDATE #group1 
SET           Amount =
              RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1)
                     ))
WHERE noteType = 6   
AND ISNUMERIC 
       (
       RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1)
                     ))
       )  = 1


  
/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType7) */
UPDATE #group1 
SET         StartAt = 21,
              EndAt =  LEN(LEFT( Note_text,  (CHARINDEX(' by EDI',  Note_text))))
WHERE  NoteType = 7   ---Minimum bid set to $xxx.00 by EDI, EDI on Nov 23 2014 10:36:31 PM

UPDATE #group1 
SET           Amount =
              RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1)
                     ))
where noteType = 7   
AND ISNUMERIC 
       (
       RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt-1)
                     ))

       ) = 1

       

/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType8) */
-- 'Minimum Bid was placed as call' is the note text
-- the amount on this notetype is always 0.00, no further logic needed, but we have a simple select statement below for testing 
-- select * from #group1 where notetype = 8 
 

/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType10) */
UPDATE #group1 
SET         StartAt = (CHARINDEX('to $', Note_text)),  
               EndAt =  LEN(LEFT( Note_text,  (CHARINDEX(' on',  Note_text))))
WHERE  NoteType = 10  ---The minimum bid was updated from Call: Yes to $.....Reason for Minimum Bid Change: Salvage Provider Request. 

UPDATE #group1 
SET           Amount =
              RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt + 3)
                     ))
where noteType = 10  
AND ISNUMERIC 
       (
                     RIGHT
                     (
              
                     LEFT(Note_Text, EndAt)
                     ,(Endat- (StartAt + 3)
                     ))
       )  =1

       
/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType11) */
-- 'The minimum bid was updated from $xxx to Call: Yes on <datetime>. Reason for Minimum Bid Change: Salvage Provider Request.'
--- These will always be set to a $0.00 amount, the default value, so no further logic is required here, but we have a simple select statement below for testing 
-- select * from  #group1  where noteType = 11


/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType12) */
UPDATE #group1 
SET         StartAt = (CHARINDEX('to $', Note_text)),  
               EndAt =  LEN(LEFT( Note_text,  (CHARINDEX(' Minimum Bid',  Note_text))))
WHERE  NoteType = 12  -- I-Buy Fast Price/Minimum Bid set to $125.00 Minimum Bid by Hissong, Jim on Mar  6 2015 12:43:34 PM

UPDATE #group1 
SET           Amount =
              RIGHT
              (
              
              LEFT(Note_Text, EndAt)
              ,(Endat- (StartAt + 3 )
              ))
where noteType = 12  
AND    ISNUMERIC     
       (
              RIGHT
              (
              
              LEFT(Note_Text, EndAt)
              ,(Endat- (StartAt + 3 )
              ))
       ) = 1



/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType13) */
-- 'The minimum bid was updated from Call: No to Call: Yes on Apr 20 2015 11:28AM. Reason for Minimum Bid Change: Other. Notes: PER PROVIDER.'
--- These will always be set to a $0.00 amount, the default value, so no further logic is required here, but we have a simple select statement below for testing 
-- select * from  #group1  where noteType = 13


/* Update Group1 Start/EndAt and Test For Accurate Amt (NoteType14) */
UPDATE #group1 
SET         StartAt = (CHARINDEX('to $', Note_text)) +3  ,
              EndAt =  LEN(LEFT( Note_text,  (CHARINDEX(' on',  Note_text))))   
WHERE  NoteType = 14  --'The Minimum Bid has been updated from $350.00 to $400.00 on 01/07/2015 14:36:32 PM.  Reason for Minimum Bid Change: Other.  Notes: PA Sale Management Tool.'

UPDATE #group1 
SET           Amount =

  RIGHT
              (
              
              LEFT(Note_Text, EndAt)
              ,(Endat- (StartAt  )
              ))
where noteType = 14 
AND ISNUMERIC
       (
       RIGHT
              (
              
              LEFT(Note_Text, EndAt)
              ,(Endat- (StartAt  )
              ))
       )
       = 1


 /*********************************************************************************************/
/*                        Parsing is now complete, lets find and remove 'duplicates'                               */
/*********************************************************************************************/
  -- if the amounts, day date, user and callername are all identical, we have dups, remove all but the first record  
  -- select count(*) from #group1
DELETE  FROM #group1 
WHERE  Salvage_Note_ID NOT IN
              (
              SELECT MIN (Salvage_Note_ID) Salvage_Note_ID
              FROM   #Group1  g1

              GROUP BY
                           g1.Salvage_ID,
                           g1.Amount,
                           g1.DateAtMidNight,
                           g1.Update_User_ID,
                           g1.CallerName
              )


/* if there is a 1 hr difference but amounts and caller names are identical, consider it a duplicate */
SELECT g.StockID,
              g.Amount,
              g.CallerName,
              g.Update_DateTime
INTO   #AdditionalDuplicates
FROM   EDW.dbo.FactMinimumBidDetail FSMB with (NOLOCK)
              INNER JOIN    #group1  g WITH (NOLOCK)
                     ON     g.StockID = FSMB.StockID
                     AND    g.Amount = FSMB.MinimumBidCurrentAmount
                     AND    g.CallerName = FSMB.CallerName
                     AND    DATEDIFF(hh,FSMB.[ASAPUpdateDate], g.Update_DateTime) Between -1 and 1

DELETE FROM #group1 
WHERE  StockID IN
              (
              SELECT StockID
              FROM   #AdditionalDuplicates d WITH (NOLOCK)
              WHERE  d.StockID = #group1.StockID
              AND           d.CallerName = #group1.CallerName
              AND           d.Amount = #group1.Amount
              )


/* our friends from ASAP have 'EDI, EDI' user creating duplicate notes...in this scenario, even if the DATE is different, find the LATEST record in FMBD table
   for user 'EDI EDI'.  Compare this to data in our incoming dataset (#Group1) and if they match on user and Amount, delete the record
*/ 
SELECT g1.StockId,
              g1.Salvage_ID,
              g1.Salvage_Note_ID,
              g1.Amount,
              g1.CallerName,
              g1.StockExistsInFMBDFlag,
              B.FactMinimumBidID,
              B.[MinimumBidCurrentAmount]
INTO   #DuplicateEDI
FROM   #group1 g1 WITH (NOLOCK)
              INNER JOIN 
                           (
                           SELECT StockID,
                                         MAX(FactMinimumBidID) FactMinimumBidID,
                                         MAX(CallerName) callerName,
                                         MAX([MinimumBidCurrentAmount]) [MinimumBidCurrentAmount] 
                           FROM   EDW.dbo.FactMinimumBidDetail FMBD WITH (NOLOCK)
                           WHERE  FMBD.CallerName = 'EDI EDI'
                           GROUP BY
                                         StockID
                           ) A
                     ON A.StockID = g1.StockID

              INNER JOIN EDW.dbo.FactMinimumBidDetail B WITH (NOLOCK)
                     ON     b.FactMinimumBidID = A.factMinimumBidID
                     AND    b.[MinimumBidCurrentAmount]  = A.[MinimumBidCurrentAmount]
WHERE  g1.CallerName = 'EDI EDI'
AND           g1.StockExistsInFMBDFlag = 1
AND           g1.Amount = B.MinimumBidCurrentAmount


DELETE FROM #Group1 WHERE STOCKID IN 
(SELECT StockID from #DuplicateEDI)



/******************************************************************************************/
/*  In order to find PRIOR values we need to order the data using ROW NUMBER OVER.         */
/*  THIS IS FOR Records that do NOT have an existing entry in FSMBD table.  We have to    */
/*  to account for situations where more than 1 MIN BID note is created that day.          */
/*                                                                                                                                                       */
/*  We have additional logic following this to handle records that have a row or more      */
/*  in the FSMBD table.                                                                                                                    */
/******************************************************************************************/
/* Added PreviuosMinimumBidSource --------------------------------------------------------*/

SELECT --fmbd.FactMinimumBidID,
              g1.StockID,
              
              NULL   BranchNumber,
              CONVERT(money, 0.00)       MinimumBidPreviousAmount,
              g1.Amount     MinimumBidCurrentAmount,
              CONVERT(money, 0.00)  MinimumBidOriginalAmount,
              NULL   Reason,
              1             ProviderID,
              NULL   BidChangeDayID,
              NULL                 ASAPDayId,
              g1.Update_DateTime ASAPUpdateDate,
              g1.salvage_note_ID ASAPSalvageNoteID,
              g1.MinBidSource MinimumBidSource,
              NULL   ETLLoadID,
              NULL   ETLLoadUpdateID,
              g1.CallerName callerName,
              g1.Salvage_ID,
              ROW_NUMBER() OVER (Partition BY g1.Salvage_ID Order By g1.Salvage_Note_ID ASC) RowNumber,
              0 AS PriorRowNumber,
              g1.Note_Text,
              g1.StockExistsInFMBDFlag,
              g1.Update_User_ID,
              g1.MinBidSource AS PreviousMinimumBidSource
INTO   #RankOrder    
FROM   #Group1  g1
              ----LEFT OUTER JOIN EDW.dbo.FactMinimumBidDetail fmbd   WITH (NOLOCK)
              ----   ON     fmbd.StockID = g1.StockID
              ----   AND  fmbd.ASAPUpdateDate = g1.Update_DateTime
ORDER BY
       g1.Salvage_ID, g1.Salvage_Note_ID

UPDATE #RankOrder SET PreviousMinimumBidSource = ''

UPDATE R SET PreviousMinimumBidSource = RP.MinimumBidSource
--SELECT R.MinimumBidSource, RP.MinimumBidSource,* 
FROM #RankOrder R WITH (NOLOCK)
INNER JOIN (SELECT RowNumber, StockID, MinimumBidSource 
                     FROM #RankOrder R WITH (NOLOCK)) RP
       ON RP.StockID = R.StockID AND RP.RowNumber = R.RowNumber-1
WHERE R.RowNumber > 1

/*********************************************************************************************************************/
/* for those stocks that currently have a record in FSMBD we will need to get the ORIGINAL and CURRENT amount values */
/* the 'current' amount will actually revert to our PRIOR amount in a later update                                                  */
/*********************************************************************************************************************/
SELECT FMBD2.StockID,
              FMBD2.[FactMinimumBidID],
              FMBD2.[MinimumBidCurrentAmount],
              FMBD2.[MinimumBidOriginalAmount]
INTO   #CurrentAndOriginal
FROM   [dbo].[FactMinimumBidDetail] fmbd2 WITH (NOLOCK)
              INNER JOIN
                           (
                           SELECT MAX(fmbd1.FactMinimumBidID) FactMinimumBidID,
                                         fmbd1.StockID
                           FROM   [dbo].[FactMinimumBidDetail] fmbd1 WITH (NOLOCK)
                     
                           GROUP BY
                                         fmbd1.StockID 
                           ) FMBD1
                     ON FMBD1.FactMinimumBidID = fmbd2.FactMinimumBidID
              INNER JOIN #RankOrder RO WITH (NOLOCK)
                     ON     RO.StockID = fmbd2.StockID
                     AND    RO.StockExistsInFMBDFlag = 1
                     AND    RO.RowNumber = 1


UPDATE #RankOrder
SET           PriorRowNumber = RowNumber - 1



/* Find Original Amount - this will be the first NON ZERO amount (if there is only one record for the stock and it is $0 then $0 is the original amount */
/* We only want to do this for records that currently do NOT have row in FSMBD; the code above will get the Original Amount for those records.  */
-- drop table #FindOriginal 
SELECT StockId,
              Salvage_Note_ID,
              Salvage_ID,
              Amount,
              ROW_NUMBER() OVER (Partition BY Salvage_ID Order By Salvage_Note_ID ASC) RowNumber 
INTO   #FindOriginal 
FROM   #Group1 
WHERE  Amount > 0
AND           StockExistsInFMBDFlag = 0  -- will not be getting data for stocks that have existing records in FSMBD table!
ORDER BY
              Salvage_ID, Salvage_Note_ID


/* Update OriginalAmount for rows where a record DOES NOT exist in FSMBD */
UPDATE #RankOrder
SET           MinimumBidOriginalAmount =   Amount
FROM   #FindOriginal a
WHERE  a.Salvage_ID = #RankOrder.Salvage_ID

/* Update OriginalAmount for rows where a record already exists in FSMBD */
UPDATE #RankOrder
SET           MinimumBidOriginalAmount = a.[MinimumBidOriginalAmount]
FROM   #CurrentAndOriginal a
WHERE  a.StockID  = #RankOrder.StockID


/* Find Prior Amount */
UPDATE  #RankOrder
SET    MinimumBidPreviousAmount = c. MinimumBidCurrentAmount
FROM
                           (
       
                           SELECT a.Salvage_ID,
                                         a.RowNumber,
                                         a.PriorRowNumber,
                                         b.MinimumBidCurrentAmount
                           FROM   #RankOrder a

                                         INNER JOIN #RankOrder b
                                                ON     b.Salvage_ID = a.Salvage_ID
                                                AND    a.PriorRowNUmber = b.RowNUmber
                           WHERE  a.PriorRowNumber > 0
                           
                           ) c
       WHERE  c.Salvage_ID = #RankOrder .Salvage_ID
       AND           c.RowNumber = #RankOrder.RowNumber

/***********************************************************************************************************************************/
--- Now we need to find the PRIOR amount for any records in our dataset (#rankOrder) that have the StockExistsInFMBDFlag set to 1
--  We will simply gather the MINIMIMUBIDCURRENTAMOUNT value from the last FMBD record for that Stock
  /***********************************************************************************************************************************/
SELECT c.StockID,
              c.FactMinimumBidID,
              c.[MinimumBidCurrentAmount]
INTO   #UpdatePrior
FROM   EDW.[dbo].[FactMinimumBidDetail] c WITH (NOLOCK)
              INNER JOIN
                     (
                     SELECT a.StockID,
                                  MAX(a.FactMinimumBidID) FactMinimumBidID
                     FROM   EDW.dbo.FactMinimumBidDetail a WITH (NOLOCK)
                                  INNER JOIN #RankOrder b WITH (NOLOCK)
                                         ON b.StockID = a.StockID
                                         AND    b.StockExistsInFMBDFlag = 1
                     GROUP BY
                                  a.StockID
                     ) FMBID
              ON FMBID.FactMinimumBidID = c.FactMinimumBidID




UPDATE #RankOrder
SET           MinimumBidPreviousAmount = a.MinimumBidCurrentAmount
FROM   #UpdatePrior a WITH (NOLOCK)
              INNER JOIN #RankOrder b WITH (NOLOCK)
                     ON b.StockID = a.StockID
                     AND    b.RowNumber = 1   -- if there are multiple rows for a stock, we only want to update the first one!



 

/* Additional Rules Per M O Malley  */
UPDATE #RankOrder
SET           MinimumBidSource = 'User Override'
WHERE  MinimumBidSource = 'UPDATE'
AND           RowNumber >= 2


/* Final Updates to pre data for Insert into FactMinBidDetail */

UPDATE #RankOrder
SET           MinimumBidSource = 'User'
WHERE  MinimumBidSource = 'UPDATE'
AND           RowNumber >= 1


UPDATE #RankOrder
SET           BranchNumber = ds.AdministrativeBranchNumber,
              ProviderID = ds.CurrentProviderID
FROM   edw.dbo.DimStock ds WITH (NOLOCK)
WHERE  ds.StockID = #RankOrder.StockID


UPDATE #RankOrder
SET           ASAPDayID = dd.DayID,
              BidChangeDayID = dd.DayID
FROM   #RankOrder a
              INNER JOIN EDW.dbo.DimDay dd with (NOLOCK)
                     ON dd.DayDate = DATEADD(d,0,DATEDIFF(d,0,a.ASAPUpdateDate))


-- update 04-27-15
UPDATE #RankOrder
SET           MinimumBidSource = 'User'
WHERE  note_text like 'I-Buy Fast Price/Minimum Bid changed from $0.00 Minimum Bid%' and rownumber = 1 and minimumBidSource = 'User Override'

-- update 04-29-15 20:00:00
UPDATE #RankOrder
SET           MinimumBidSource = 'User'
WHERE  note_text like 'The minimum bid was updated from $0.00 to $%' and rownumber = 1 and minimumBidSource = 'User Override'

--select * from #rankOrder order by stockID, asapsalvagenoteID

--07-24-2015
--Update MinimumBidSource to Show User Override if there was a prior minimum bid on a Stock 
 
UPDATE #RankOrder
SET MinimumBidSource = 'User Override'
WHERE note_text like 'Minimum bid set to $%' AND Note_Text LIKE '%EDI%' AND rownumber > 1 
AND PreviousMinimumBidSource IN ('Automatic', 'User','USER Override')


---*Update MinimumBidSource to Show UserOverride if there was a prior minimum bid on a Stock to the note 'I-Buy Fast Price/Minimum Bid set to $%' 
UPDATE #RankOrder
SET MinimumBidSource = 'User Override'
WHERE note_text like 'I-Buy Fast Price/Minimum Bid set to $%'  
 AND rownumber > 1 
AND PreviousMinimumBidSource IN ('User','USER Override','User Override')
AND MinimumBidSource <> 'User Override'

UPDATE	RO
SET		MinimumBidSource = 'User Override'
--SELECT * 
FROM #RankOrder RO
inner join #RankOrder RO2
ON RO.Salvage_ID = RO2.Salvage_ID
	AND RO.RowNumber-1 = RO2.RowNumber
WHERE RO.MinimumBidSource = 'User' and RO2.MinimumBidSource IN ('User Override','User','Automatic')	  

UPDATE	RO
SET		MinimumBidSource = 'User'
--SELECT * 
FROM #RankOrder RO
inner join #RankOrder RO2
ON RO.Salvage_ID = RO2.Salvage_ID
	AND RO.RowNumber-1 = RO2.RowNumber
WHERE RO.MinimumBidSource = 'User Override' and RO2.MinimumBidSource IN ('Call')

---------Update MinimumBidOriginalAmount to show the First MinimumBidAmount if MinimumBidAmount > 0 -------------------

SELECT Distinct Salvage_ID 
INTO #SalvageIdfromDataList
FROM #DataList

SELECT FMBD.StockID,
              FMBD.[FactMinimumBidID],
              FMBD.[MinimumBidCurrentAmount],
              FMBD.[MinimumBidOriginalAmount],
			  ROW_NUMBER() OVER (Partition BY FMBD.StockId Order By FMBD.FactMinimumBidID ASC) RowNumber
INTO #OriginalMinBidAmount
FROM EDW.dbo.FactMinimumBidDetail FMBD WITH (NOLOCK)   
INNER JOIN 
              (
			     SELECT Min(FactMinimumBidID) FactMinimumBidID,
		                FMB.StockId
                  FROM EDW.dbo.FactMinimumBidDetail FMB WITH (NOLOCK)
                  INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
                     ON FMB.StockID = DS.StockId
                  INNER JOIN #SalvageIdfromDataList DL WITH (NOLOCK)
                     ON DL.Salvage_Id = DS.ASAPSalvageId
                  WHERE FMB.MinimumBidCurrentAmount > 0 
                  AND FMB.MinimumBidOriginalAmount = 0                
                  GROUP BY FMB.StockId
			 ) MI
ON MI.StockID = FMBD.StockID
AND FMBD.FactMinimumBidID >= MI.FactMinimumBidID 

---SELECT * FROM #OriginalMinBidAmount 

SELECT OM.StockID,
       OM.FactMinimumBidID,
       OM.MinimumBidCurrentAmount,
	   OM.RowNumber                                                  --SELECT * FROM #FirstRecordGreaterthanzero 
INTO #FirstRecordGreaterthanzero 
FROM #OriginalMinBidAmount OM WITH(NoLock)
     WHERE OM.RowNumber = 1


UPDATE T
SET T.MinimumBidOriginalAmount = FZ.MinimumBidCurrentAmount
--SELECT T.MinimumBidOriginalAmount,FZ.MinimumBidCurrentAmount,*
FROM EDW.dbo.FactMinimumBidDetail T WITH(NoLock)
INNER JOIN #OriginalMinBidAmount OM WITH(NoLock)
     ON T.FactMinimumBidID = OM.FactMinimumBidID 
INNER JOIN #FirstRecordGreaterthanzero FZ WITH(NoLock)
     ON FZ.StockID = OM.StockID
WHERE T.MinimumBidOriginalAmount <> FZ.MinimumBidCurrentAmount
 
--------------------------------------------
	  


BEGIN TRY
       BEGIN TRANSACTION
              INSERT INTO EDW.dbo.FactMinimumBidDetail
                     (
                     StockID, 
                     BranchNumber, 
                     MinimumBidPreviousAmount, 
                     MinimumBidCurrentAmount, 
                     MinimumBidOriginalAmount, 
                     Reason, 
                     ProviderID, 
                     BidChangeDayID, 
                     ASAPDayID, 
                     ASAPUpdateDate, 
                     ASAPUpdateUserID, -- NEED TO ADD TO #rankOrder
                     ASAPSalvageNoteID,
                     MinimumBidSource, 
                     ETLLoadID,                 -- NEED TO ADD
                     ETLLoadUpdateID,     -- NEED TO ADD
                     CallerName
                     )
       
              SELECT 
                     
                     StockID, 
                     BranchNumber, 
                     MinimumBidPreviousAmount, 
                     MinimumBidCurrentAmount, 
                     MinimumBidOriginalAmount, 
                     Reason, 
                     ProviderID, 
                     BidChangeDayID, 
                     ASAPDayID, 
                     ASAPUpdateDate, 
                     Update_User_ID,
                     ASAPSalvageNoteID,
                     MinimumBidSource, 
                     @ETLLoadID,                
                     @ETLLoadID,
                     CallerName
                     
              FROM #RankOrder WITH (NOLOCK)
                     ORDER BY
                           StockID,
                           ASAPSalvageNoteID

              SET @InsertRowCount = @@ROWCOUNT 

       COMMIT TRANSACTION

UPDATE FMBD SET [MinimumBidSource] = 'User Override'
--SELECT FMBD.* 
FROM [EDW].[dbo].[FactMinimumBidDetail]  FMBD
INNER JOIN (SELECT * FROM [EDW].[dbo].[FactMinimumBidDetail] WITH (NOLOCK)
			  where [MinimumBidSource] = 'Automatic') FU
on FMBD.StockID = fu.StockID
where FMBD.[MinimumBidSource] = 'User'
AND fu.[ASAPUpdateDate] < FMBD.[ASAPUpdateDate]
AND FMBD.FactMinimumBidID NOT IN (SELECT DISTINCT FU.FactMinimumBidID FROM [EDW].[dbo].[FactMinimumBidDetail]  FMBD WITH (NOLOCK)
									INNER JOIN (SELECT * FROM [EDW].[dbo].[FactMinimumBidDetail]  WITH (NOLOCK)
												  where [MinimumBidSource] = 'User') FU
									on FMBD.StockID = fu.StockID
									where FMBD.[MinimumBidSource] = 'Call'
									AND fu.[ASAPUpdateDate] > FMBD.[ASAPUpdateDate])

END TRY

BEGIN CATCH
       IF @@TRANCOUNT > 0
       BEGIN
              
              ROLLBACK TRANSACTION 
       END
              
       -- Raise an error with the details of the exception
       DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
       SELECT @ErrMsg = ERROR_MESSAGE(),
                     @ErrSeverity = ERROR_SEVERITY()

       RAISERROR(@ErrMsg, @ErrSeverity, 1)
END    CATCH








/* Run the audit update then Drop the Temp Tables */  

EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount


IF OBJECT_ID('tempdb..#DataList') IS NOT NULL DROP TABLE      #DataList 
IF OBJECT_ID('tempdb..#FSMBExists') IS NOT NULL DROP TABLE    #FSMBExists  
IF OBJECT_ID('tempdb..#t1') IS NOT NULL DROP TABLE  #t1  
IF OBJECT_ID('tempdb..#group1') IS NOT NULL DROP TABLE  #group1
IF OBJECT_ID('tempdb.. #FindOriginal ') IS NOT NULL DROP TABLE  #FindOriginal 
IF OBJECT_ID('tempdb.. #RankOrder') IS NOT NULL DROP TABLE  #RankOrder
IF OBJECT_ID('tempdb..#UpdatePrior') IS NOT NULL DROP TABLE  #UpdatePrior
IF OBJECT_ID('tempdb..#AdditionalDuplicates') IS NOT NULL DROP TABLE  #AdditionalDuplicates
IF OBJECT_ID('tempdb..#DuplicateEDI') IS NOT NULL DROP TABLE  #DuplicateEDI
IF OBJECT_ID('tempdb..#SalvageIdfromDataList') IS NOT NULL DROP TABLE  #SalvageIdfromDataList
IF OBJECT_ID('tempdb..#OriginalMinBidAmount') IS NOT NULL DROP TABLE   #OriginalMinBidAmount
IF OBJECT_ID('tempdb..#FirstRecordGreaterthanzero') IS NOT NULL DROP TABLE  #FirstRecordGreaterthanzero







END
