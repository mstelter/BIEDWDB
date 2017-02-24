
/* 

==============================================================================
Author		: Mohammad Waheed
Create date	: 09/13/2016
Description	: Parse the XML data for file Envirnmental and Load it to the [EDW].[DBO].OHAInformation
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	

==============================================================================
Usage Example:
--Exec EDW.DBO.[LoadXMLDataToOHAInformation_P]

--EXEC EDW.[dbo].[usp_LoadXMLData] 
Select * from Edw.[dbo].[OHALoadRawXML]
--Truncate table EDW.[dbo].[OHALoadRawXML]
Select * from EDW.[dbo].[OHAFormsXMLData]
--Truncate table EDW.[dbo].[OHAFormsXMLData]
Select * from [EDW].[DBO].OHAInformation
--Truncate Table  [EDW].[DBO].OHAInformation
--Drop Table #XmlData
==============================================================================
*/
Create PROCEDURE [dbo].[usp_OHALoadXMLDataToOHAInformation_P]
AS
  BEGIN

--select *
delete 
FROM [EDW].[DBO].OHAInformation  WHERE   Id in (	select oi.id
														from [EDW].[DBO].OHAInformation oi
															inner join EDW.[dbo].[OHALoadRawXML] lrx 
															          on  oi.[QtrYear]=lrx.[QtrYear] and
																	      oi.[BranchName]=lrx.[BranchName] and
												                          oi.FileModifiedDateTime<lrx.FileModifiedDateTime and
																		  oi.FileType=lrx.FileType
													   )

SELECT 
     FileType
    ,BranchName
	,QtrYear
	,FileModifiedDateTime

-------------------------<S1>------------------------------
,x.value('(/myFields/Q1)[1]','varchar(2000)') AS S1_Q1Question
,x.value('(/myFields/Q2)[1]','varchar(2000)') AS S1_Q2Question
,x.value('(/myFields/Q3)[1]','varchar(2000)') AS S1_Q3Question
,x.value('(/myFields/Q4)[1]','varchar(2000)') AS S1_Q4Question
,x.value('(/myFields/Q5)[1]','varchar(2000)') AS S1_Q5Question
,x.value('(/myFields/Q6)[1]','varchar(2000)') AS S1_Q6Question
,x.value('(/myFields/Q7)[1]','varchar(2000)') AS S1_Q7Question
,x.value('(/myFields/Q8)[1]','varchar(2000)') AS S1_Q8Question
,x.value('(/myFields/Q9)[1]','varchar(2000)') AS S1_Q9Question
,x.value('(/myFields/Q10)[1]','varchar(2000)') AS S1_Q10Question
,x.value('(/myFields/Q11)[1]','varchar(2000)') AS S1_Q11Question
,x.value('(/myFields/Q12)[1]','varchar(2000)') AS S1_Q12Question
,x.value('(/myFields/Q13)[1]','varchar(2000)') AS S1_Q13Question
,x.value('(/myFields/Q14)[1]','varchar(2000)') AS S1_Q14Question
,x.value('(/myFields/Q15)[1]','varchar(2000)') AS S1_Q15Question
,x.value('(/myFields/Q16)[1]','varchar(2000)') AS S1_Q16Question
,x.value('(/myFields/Q17)[1]','varchar(2000)') AS S1_Q17Question
,x.value('(/myFields/Q18)[1]','varchar(2000)') AS S1_Q18Question
,x.value('(/myFields/Q19)[1]','varchar(2000)') AS S1_Q19Question
,x.value('(/myFields/Q20)[1]','varchar(2000)') AS S1_Q20Question
,x.value('(/myFields/Q21)[1]','varchar(2000)') AS S1_Q21Question
,x.value('(/myFields/Q22)[1]','varchar(2000)') AS S1_Q22Question
,x.value('(/myFields/Q23)[1]','varchar(2000)') AS S1_Q23Question
,x.value('(/myFields/Q24)[1]','varchar(2000)') AS S1_Q24Question
,x.value('(/myFields/Q25)[1]','varchar(2000)') AS S1_Q25Question
,x.value('(/myFields/Q26)[1]','varchar(2000)') AS S1_Q26Question
,x.value('(/myFields/Q27)[1]','varchar(2000)') AS S1_Q27Question

	
,x.value('(/myFields/S1/Q1YN)[1]','Int') AS S1_Q1Answer
,x.value('(/myFields/S1/Q2YN)[1]','Int') AS S1_Q2Answer
,x.value('(/myFields/S1/Q3YN)[1]','Int') AS S1_Q3Answer
,x.value('(/myFields/S1/Q4YN)[1]','Int') AS S1_Q4Answer
,x.value('(/myFields/S1/Q5YN)[1]','Int') AS S1_Q5Answer
,x.value('(/myFields/S1/Q6YN)[1]','Int') AS S1_Q6Answer
,x.value('(/myFields/S1/Q7YN)[1]','Int') AS S1_Q7Answer
,x.value('(/myFields/S1/Q8YN)[1]','Int') AS S1_Q8Answer
,x.value('(/myFields/S1/Q9YN)[1]','Int') AS S1_Q9Answer
,x.value('(/myFields/S1/Q10YN)[1]','Int') AS S1_Q10Answer
,x.value('(/myFields/S1/Q11YN)[1]','Int') AS S1_Q11Answer
,x.value('(/myFields/S1/Q12YN)[1]','Int') AS S1_Q12Answer
,x.value('(/myFields/S1/Q13YN)[1]','Int') AS S1_Q13Answer
,x.value('(/myFields/S1/Q14YN)[1]','Int') AS S1_Q14Answer
,x.value('(/myFields/S1/Q15YN)[1]','Int') AS S1_Q15Answer
,x.value('(/myFields/S1/Q16YN)[1]','Int') AS S1_Q16Answer
,x.value('(/myFields/S1/Q17YN)[1]','Int') AS S1_Q17Answer
,x.value('(/myFields/S1/Q18YN)[1]','Int') AS S1_Q18Answer
,x.value('(/myFields/S1/Q19YN)[1]','Int') AS S1_Q19Answer
,x.value('(/myFields/S1/Q20YN)[1]','Int') AS S1_Q20Answer		
,x.value('(/myFields/S1/Q21YN)[1]','Int') AS S1_Q21Answer	
,x.value('(/myFields/S1/Q22YN)[1]','Int') AS S1_Q22Answer
,x.value('(/myFields/S1/Q23YN)[1]','Int') AS S1_Q23Answer
,x.value('(/myFields/S1/Q24YN)[1]','Int') AS S1_Q24Answer
,x.value('(/myFields/S1/Q25YN)[1]','Int') AS S1_Q25Answer
,x.value('(/myFields/S1/Q26YN)[1]','Int') AS S1_Q26Answer		
,x.value('(/myFields/S1/Q27YN)[1]','Int') AS S1_Q27Answer			

,x.value('(/myFields/Q1S)[1]','Float') AS S1_Q1Score	
,x.value('(/myFields/Q2S)[1]','Float') AS S1_Q2Score	
,x.value('(/myFields/Q3S)[1]','Float') AS S1_Q3Score	
,x.value('(/myFields/Q4S)[1]','Float') AS S1_Q4Score	
,x.value('(/myFields/Q5S)[1]','Float') AS S1_Q5Score	
,x.value('(/myFields/Q6S)[1]','Float') AS S1_Q6Score	
,x.value('(/myFields/Q7S)[1]','Float') AS S1_Q7Score	
,x.value('(/myFields/Q8S)[1]','Float') AS S1_Q8Score	
,x.value('(/myFields/Q9S)[1]','Float') AS S1_Q9Score	
,x.value('(/myFields/Q10S)[1]','Float') AS S1_Q10Score	
,x.value('(/myFields/Q11S)[1]','Float') AS S1_Q11Score
,x.value('(/myFields/Q12S)[1]','Float') AS S1_Q12Score	
,x.value('(/myFields/Q13S)[1]','Float') AS S1_Q13Score	
,x.value('(/myFields/Q14S)[1]','Float') AS S1_Q14Score	
,x.value('(/myFields/Q15S)[1]','Float') AS S1_Q15Score	
,x.value('(/myFields/Q16S)[1]','Float') AS S1_Q16Score	
,x.value('(/myFields/Q17S)[1]','Float') AS S1_Q17Score	
,x.value('(/myFields/Q18S)[1]','Float') AS S1_Q18Score
,x.value('(/myFields/Q19S)[1]','Float') AS S1_Q19Score	
,x.value('(/myFields/Q20S)[1]','Float') AS S1_Q20Score	
,x.value('(/myFields/Q21S)[1]','Float') AS S1_Q21Score
,x.value('(/myFields/Q22S)[1]','Float') AS S1_Q22Score	
,x.value('(/myFields/Q23S)[1]','Float') AS S1_Q23Score	
,x.value('(/myFields/Q24S)[1]','Float') AS S1_Q24Score
,x.value('(/myFields/Q25S)[1]','Float') AS S1_Q25Score	
,x.value('(/myFields/Q26S)[1]','Float') AS S1_Q26Score	
,x.value('(/myFields/Q27S)[1]','Float') AS S1_Q27Score

,x.value('(/myFields/S1/Q1OPP)[1]','varchar(2000)') AS S1_Q1Comment
,x.value('(/myFields/S1/Q2OPP)[1]','varchar(2000)') AS S1_Q2Comment
,x.value('(/myFields/S1/Q3OPP)[1]','varchar(2000)') AS S1_Q3Comment
,x.value('(/myFields/S1/Q4OPP)[1]','varchar(2000)') AS S1_Q4Comment
,x.value('(/myFields/S1/Q5OPP)[1]','varchar(2000)') AS S1_Q5Comment
,x.value('(/myFields/S1/Q6OPP)[1]','varchar(2000)') AS S1_Q6Comment
,x.value('(/myFields/S1/Q7OPP)[1]','varchar(2000)') AS S1_Q7Comment
,x.value('(/myFields/S1/Q8OPP)[1]','varchar(2000)') AS S1_Q8Comment
,x.value('(/myFields/S1/Q9OPP)[1]','varchar(2000)') AS S1_Q9Comment
,x.value('(/myFields/S1/Q10OPP)[1]','varchar(2000)') AS S1_Q10Comment
,x.value('(/myFields/S1/Q11OPP)[1]','varchar(2000)') AS S1_Q11Comment
,x.value('(/myFields/S1/Q12OPP)[1]','varchar(2000)') AS S1_Q12Comment
,x.value('(/myFields/S1/Q13OPP)[1]','varchar(2000)') AS S1_Q13Comment
,x.value('(/myFields/S1/Q14OPP)[1]','varchar(2000)') AS S1_Q14Comment
,x.value('(/myFields/S1/Q15OPP)[1]','varchar(2000)') AS S1_Q15Comment
,x.value('(/myFields/S1/Q16OPP)[1]','varchar(2000)') AS S1_Q16Comment
,x.value('(/myFields/S1/Q17OPP)[1]','varchar(2000)') AS S1_Q17Comment
,x.value('(/myFields/S1/Q18OPP)[1]','varchar(2000)') AS S1_Q18Comment
,x.value('(/myFields/S1/Q19OPP)[1]','varchar(2000)') AS S1_Q19Comment
,x.value('(/myFields/S1/Q20OPP)[1]','varchar(2000)') AS S1_Q20Comment
,x.value('(/myFields/S1/Q21OPP)[1]','varchar(2000)') AS S1_Q21Comment
,x.value('(/myFields/S1/Q22OPP)[1]','varchar(2000)') AS S1_Q22Comment
,x.value('(/myFields/S1/Q23OPP)[1]','varchar(2000)') AS S1_Q23Comment
,x.value('(/myFields/S1/Q24OPP)[1]','varchar(2000)') AS S1_Q24Comment
,x.value('(/myFields/S1/Q25OPP)[1]','varchar(2000)') AS S1_Q25Comment
,x.value('(/myFields/S1/Q26OPP)[1]','varchar(2000)') AS S1_Q26Comment
,x.value('(/myFields/S1/Q27OPP)[1]','varchar(2000)') AS S1_Q27Comment
-------------------------</S1>------------------------------


into #XmlData	
from [EDW].[DBO].[OHAFormsXMLData] 
Cross Apply [XMLData].nodes('/myFields') AS T(x)
Where FileType='P'
and (  FileType+[QtrYear]+[BranchName] not in (select FileType+[QtrYear]+[BranchName] from Edw.dbo.OHAInformation ) )


--select * from #XmlData



------------------S1------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #S1_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Question,S1_Q2Question,S1_Q3Question,S1_Q4Question,S1_Q5Question,S1_Q6Question,S1_Q7Question,S1_Q8Question,S1_Q9Question,S1_Q10Question,S1_Q11Question,S1_Q12Question,S1_Q13Question,S1_Q14Question,S1_Q15Question,S1_Q16Question,S1_Q17Question,S1_Q18Question,S1_Q19Question,S1_Q20Question,S1_Q21Question,S1_Q22Question,S1_Q23Question,S1_Q24Question,S1_Q25Question,S1_Q26Question,S1_Q27Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					S1_Q1Question,S1_Q2Question,S1_Q3Question,S1_Q4Question,S1_Q5Question,S1_Q6Question,S1_Q7Question,S1_Q8Question,S1_Q9Question,S1_Q10Question,S1_Q11Question,S1_Q12Question,S1_Q13Question,S1_Q14Question,S1_Q15Question,S1_Q16Question,S1_Q17Question,S1_Q18Question,S1_Q19Question,S1_Q20Question,S1_Q21Question,S1_Q22Question,S1_Q23Question,S1_Q24Question,S1_Q25Question,S1_Q26Question,S1_Q27Question
					)
	) as unpvt

--Select * from #S1_Question

Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #S1_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Answer,S1_Q2Answer,S1_Q3Answer,S1_Q4Answer,S1_Q5Answer,S1_Q6Answer,S1_Q7Answer,S1_Q8Answer,S1_Q9Answer,S1_Q10Answer,S1_Q11Answer,S1_Q12Answer,S1_Q13Answer,S1_Q14Answer,S1_Q15Answer,S1_Q16Answer,S1_Q17Answer,S1_Q18Answer,S1_Q19Answer,S1_Q20Answer,S1_Q21Answer,S1_Q22Answer,S1_Q23Answer,S1_Q24Answer,S1_Q25Answer,S1_Q26Answer,S1_Q27Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					S1_Q1Answer,S1_Q2Answer,S1_Q3Answer,S1_Q4Answer,S1_Q5Answer,S1_Q6Answer,S1_Q7Answer,S1_Q8Answer,S1_Q9Answer,S1_Q10Answer,S1_Q11Answer,S1_Q12Answer,S1_Q13Answer,S1_Q14Answer,S1_Q15Answer,S1_Q16Answer,S1_Q17Answer,S1_Q18Answer,S1_Q19Answer,S1_Q20Answer,S1_Q21Answer,S1_Q22Answer,S1_Q23Answer,S1_Q24Answer,S1_Q25Answer,S1_Q26Answer,S1_Q27Answer
					)
	) as unpvt

--Select * from #S1_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #S1_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Score,S1_Q2Score,S1_Q3Score,S1_Q4Score,S1_Q5Score,S1_Q6Score,S1_Q7Score,S1_Q8Score,S1_Q9Score,S1_Q10Score,S1_Q11Score,S1_Q12Score,S1_Q13Score,S1_Q14Score,S1_Q15Score,S1_Q16Score,S1_Q17Score,S1_Q18Score,S1_Q19Score,S1_Q20Score,S1_Q21Score,S1_Q22Score,S1_Q23Score,S1_Q24Score,S1_Q25Score,S1_Q26Score,S1_Q27Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					S1_Q1Score,S1_Q2Score,S1_Q3Score,S1_Q4Score,S1_Q5Score,S1_Q6Score,S1_Q7Score,S1_Q8Score,S1_Q9Score,S1_Q10Score,S1_Q11Score,S1_Q12Score,S1_Q13Score,S1_Q14Score,S1_Q15Score,S1_Q16Score,S1_Q17Score,S1_Q18Score,S1_Q19Score,S1_Q20Score,S1_Q21Score,S1_Q22Score,S1_Q23Score,S1_Q24Score,S1_Q25Score,S1_Q26Score,S1_Q27Score
					)
	) as unpvt

--Select * from #S1_Score


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #S1_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Comment,S1_Q2Comment,S1_Q3Comment,S1_Q4Comment,S1_Q5Comment,S1_Q6Comment,S1_Q7Comment,S1_Q8Comment,S1_Q9Comment,S1_Q10Comment,S1_Q11Comment,S1_Q12Comment,S1_Q13Comment,S1_Q14Comment,S1_Q15Comment,S1_Q16Comment,S1_Q17Comment,S1_Q18Comment,S1_Q19Comment,S1_Q20Comment,S1_Q21Comment,S1_Q22Comment,S1_Q23Comment,S1_Q24Comment,S1_Q25Comment,S1_Q26Comment,S1_Q27Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					S1_Q1Comment,S1_Q2Comment,S1_Q3Comment,S1_Q4Comment,S1_Q5Comment,S1_Q6Comment,S1_Q7Comment,S1_Q8Comment,S1_Q9Comment,S1_Q10Comment,S1_Q11Comment,S1_Q12Comment,S1_Q13Comment,S1_Q14Comment,S1_Q15Comment,S1_Q16Comment,S1_Q17Comment,S1_Q18Comment,S1_Q19Comment,S1_Q20Comment,S1_Q21Comment,S1_Q22Comment,S1_Q23Comment,S1_Q24Comment,S1_Q25Comment,S1_Q26Comment,S1_Q27Comment
					)
	) as unpvt

--Select * from #S1_Comment



insert into OHAInformation
(BranchName,QtrYear,FileModifiedDateTime,FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment)
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #S1_Question  q 
left join #S1_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #S1_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #S1_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType


End




