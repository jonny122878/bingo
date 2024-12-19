

--113016137


  SELECT TOP (1000) [drawTerm]
      ,[dDate]
      ,[bigShowOrder]
      ,[createDate]
  FROM [p89880749_test].[dbo].[Bingo]


CREATE CLUSTERED INDEX IX_Bingo_drawTerm
ON [p89880749_test].[p89880749_p89880749].[Bingo] (drawTerm);

delete from Bingo

  DELETE FROM [p89880749_test].[p89880749_p89880749].[Bingo] where dDate >= '2024-02-26'


  SELECT dDate
  FROM [p89880749_test].[p89880749_p89880749].[Bingo]
  GROUP BY dDate



SELECT [drawTerm]
      ,[dDate]
      ,[bigShowOrder]
      ,[createDate]
  FROM [p89880749_test].[p89880749_p89880749].[Bingo]
  WHERE dDate BETWEEN '2024-11-01' AND '2024-11-30'
  ORDER BY [dDate]　DESC,[drawTerm] DESC