/*
USE [EDW]
GO
-- =============================================
-- Author:		MStelter
-- Create date: 4/12/2014
-- Description:	STORED PROCEDURE TO RETURN LIST OF AUCTIONS AND THE AUCTION CLOSED STATUS

 Usage 
	usp_Get_Auction_Closed_Information '4/3/2014 12:00:00 AM','4/3/2014 11:59:59 PM',Null

-- =============================================
*/
CREATE PROCEDURE [dbo].[usp_Get_Auction_Closed_Information]
	@dtAuction_Start_Datetime DATETIME
	, @dtAuction_End_Datetime DATETIME
	, @intBranch_Number INT = NULL
AS
BEGIN
	DECLARE @Process_Desc VARCHAR(100)
	DECLARE @Trans_Type VARCHAR(20)
	DECLARE @Records int
	DECLARE @Start_DateTime DATETIME
	DECLARE @End_DateTime DATETIME
	DECLARE @Error_Code INT

	SET NOCOUNT ON;

	IF @intBranch_Number IS NULL
		BEGIN
			SELECT ASCH.Auction_Schedule_ID, ASCH.Auction_Datetime, IB.BranchNumber AS Branch_ID, IB.BranchNumber as Branch_Number, IB.BranchName AS Branch_Name, IB.GMTOffset AS GMT_Offset
				, CASE WHEN ASCH.Primary_Clerk_Entry_Completed_Ind = 1
							AND  ASCH.Secondary_Clerk_Entry_Completed_Ind = 1
					THEN 1
					ELSE 0 END AS Auction_Closed_Ind
			, ASCH.UPdate_Datetime
			, AF.Facility_ID
			, COUNT(*) AS Auction_Item_Count
			FROM [BI_ASAP_REP].dbo.Auction_Schedule AS ASCH WITH (NOLOCK)
				INNER JOIN EDW.dbo.DimBranch AS IB WITH (NOLOCK)
					ON ASCH.Branch_Number = IB.BranchNumber
				INNER JOIN [BI_ASAP_REP].dbo.Auction_Item AS AI WITH (NOLOCK)
					ON ASCH.Auction_Schedule_Id = AI.Auction_Schedule_ID
				INNER JOIN [BI_ASAP_REP].dbo.Auction_Facility AS AF WITH (NOLOCK) -----Commented By Prerna For P17138
				--INNER JOIN [PBI-DB1].dbo.Auction_Facility AS AF WITH (NOLOCK)
					ON IB.BranchNumber = AF.Branch_Number
			WHERE CONVERT(CHAR(8),ASCH.Auction_Datetime,112) BETWEEN CONVERT(CHAR(8),@dtAuction_Start_Datetime,112) AND CONVERT(CHAR(8),@dtAuction_End_Datetime,112)
			GROUP BY ASCH.Auction_Schedule_ID, ASCH.Auction_Datetime, IB.BranchNumber, IB.BranchNumber, IB.BranchName, IB.GMTOffset
				, CASE WHEN ASCH.Primary_Clerk_Entry_Completed_Ind = 1
							AND  ASCH.Secondary_Clerk_Entry_Completed_Ind = 1
					THEN 1
					ELSE 0 END
			, ASCH.UPdate_Datetime, AF.Facility_ID
			ORDER BY Auction_Datetime 
		END
	ELSE
		BEGIN
			SELECT ASCH.Auction_Schedule_ID, ASCH.Auction_Datetime, IB.BranchNumber AS Branch_ID, IB.BranchNumber AS Branch_Number, IB.BranchName AS Branch_Name, IB.GMTOffset AS GMT_Offset
				, CASE WHEN ASCH.Primary_Clerk_Entry_Completed_Ind = 1
							AND  ASCH.Secondary_Clerk_Entry_Completed_Ind = 1
					THEN 1
					ELSE 0 END AS Auction_Closed_Ind
			, ASCH.UPdate_Datetime
			, AF.Facility_ID
			, COUNT(*) AS Auction_Item_Count
			FROM [BI_ASAP_REP].dbo.Auction_Schedule AS ASCH WITH (NOLOCK)
				INNER JOIN EDW.dbo.DimBranch AS IB WITH (NOLOCK)
					ON ASCH.Branch_Number = ASCH.Branch_Number
				INNER JOIN [BI_ASAP_REP].dbo.Auction_Item AS AI WITH (NOLOCK)
					ON ASCH.Auction_Schedule_Id = AI.Auction_Schedule_ID
				INNER JOIN [BI_ASAP_REP].dbo.Auction_Facility AS AF WITH (NOLOCK)-----Commented By Prerna For P17138
				--INNER JOIN [PBI-DB1].dbo.Auction_Facility AS AF WITH (NOLOCK)
					ON IB.BranchNumber = AF.Branch_Number
			WHERE CONVERT(CHAR(8),ASCH.Auction_Datetime,112) BETWEEN @dtAuction_Start_Datetime AND @dtAuction_End_Datetime
					AND IB.BranchNumber = @intBranch_Number
			GROUP BY ASCH.Auction_Schedule_ID, ASCH.Auction_Datetime, IB.BranchNumber, IB.BranchNumber, IB.BranchName, IB.GMTOffset
				, CASE WHEN ASCH.Primary_Clerk_Entry_Completed_Ind = 1
							AND  ASCH.Secondary_Clerk_Entry_Completed_Ind = 1
					THEN 1
					ELSE 0 END
			, ASCH.UPdate_Datetime, AF.Facility_ID
			ORDER BY Auction_Datetime 
		END
	END