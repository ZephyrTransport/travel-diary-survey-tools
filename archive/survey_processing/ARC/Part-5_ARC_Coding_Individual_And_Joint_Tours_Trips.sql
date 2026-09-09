--ARC Project on coding tours and trips using Regional Household Travel Survey Data
--Part-5:Code Separate tours and trips into individual and joint tours and trips
--Palvinder Singh				10/24/2012				singhp@pbworld.com
--------------------------------------------------------------------------------------------
PRINT 'Part - 5 is being executed and start time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)
SET NOCOUNT ON;	
--------------------------------------------------------------------------------------------
--INPUT - TotalTourFile AND TotalTripFile FROM Part-4
--------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------
--START - Create an Input Table (for calculations) and initialize 
--		  setup before running the script to link trips                   
--------------------------------------------------------------------------------------------

--Removing any existing table FROM previous runs or if the code was run manually 
--by the user creating several tables for checking purposes
IF OBJECT_ID('dbo.RevTripFile') IS NOT NULL 
	DROP TABLE RevTripFile;
--Creating a temporary table FROM place table for doing all the calculations. 
--Also a household sequence is added to the table 
SELECT DENSE_RANK () OVER (ORDER BY HH_ID) AS Rec_Num, ROW_NUMBER () OVER 
	(PARTITION BY HH_ID,PER_ID ORDER BY TOUR_ID,TRIP_ID) AS TRIP_NUM, * INTO RevTripFile 
	FROM TotalTripFile ORDER BY HH_ID,PER_ID,Tour_ID,TRIP_ID;

CREATE NONCLUSTERED INDEX ix_RevTripFile_recnum
ON dbo.RevTripFile(REC_NUM)
CREATE NONCLUSTERED INDEX ix_RevTripFile_recnum_perid
ON dbo.RevTripFile(REC_NUM,PER_ID)
CREATE NONCLUSTERED INDEX ix_RevTripFile_recnum_perid_tripnum
ON dbo.RevTripFile(REC_NUM,PER_ID,TRIP_NUM)
CREATE NONCLUSTERED INDEX ix_RevTripFile_recnum_perid_tourid
ON dbo.RevTripFile(REC_NUM,PER_ID,TOUR_ID)
CREATE NONCLUSTERED INDEX ix_RevTripFile_recnum_perid_tourid_tripid
ON dbo.RevTripFile(REC_NUM,PER_ID,TOUR_ID,TRIP_ID)	
--------------------------------------------------------------------------------------------
--START - Create an Input Table (for calculations) and initialize 
--		  setup before running the script to link trips                   
--------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------
--START - Initial modifications in the Input Table ("RevTripFile")
--------------------------------------------------------------------------------------------
ALTER TABLE RevTripFile ADD JointFlag VARCHAR(75);
GO
--------------------------------------------------------------------------------------------
--END - Initial modifications in the Input Table ("RevTripFile")
--------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------
--START - Main Code Area
--------------------------------------------------------------------------------------------
--Declaring the household counter hh so that all households can be traced
DECLARE @hh AS int, @hhmax AS int;
SET @hh = 1; 			--initial value
--Getting the count of households for which loop will run
SELECT @hhmax = MAX(Rec_Num) FROM RevTripFile;
--Loop through all HH_IDs and do calculations
WHILE @hh <= @hhmax
	BEGIN
		PRINT 'The Current Household is : ' + CAST(@hh AS VARCHAR) + ' and current time is : ' + CAST(SYSDATETIME() AS VARCHAR);
		--Declaring the person counter @per within a household @hh to trace all the persons of the household @hh
		DECLARE @per AS int, @permax AS int;
		SET @per = 1; 			--initial value
		--Getting the count of persons in a given household for which the loop will run
		SELECT @permax = MAX(PER_ID) FROM RevTripFile WHERE Rec_Num = @hh;
		--loop through persons for a given household
		WHILE @per <= @permax
			BEGIN
				--Declaring trip counter for all the trips made BY person @per of household @hh
				DECLARE @trip AS int, @tripmax AS int;
				SET @trip = 1; 			--initial value
				--Getting the count of trips for person @per in a given household for which the loop will run
				SELECT @tripmax = MAX(TRIP_NUM) FROM RevTripFile WHERE Rec_Num = @hh AND PER_ID = @per;
				--loop through all the trips for a given person of a given household
				WHILE @trip <= @tripmax
					BEGIN 
						--Declaring pernext counter for comparing person @per with all the remaining persons of the household
						DECLARE @pernext AS int, @joint VARCHAR(200) 
						SET @pernext = 1; 			--initial value
						
						SELECT @joint = COALESCE(@joint + '.', '') + @per FROM RevTripFile
							WHERE Rec_Num = @hh AND PER_ID = @per AND TRIP_NUM = @trip
						--loop through all remaining persons to find out who else (only households member)
						--is on the trip with person @per of the given household	
						WHILE @pernext <= @permax
							BEGIN
								IF @pernext != @per				--Check needed only FROM all the remaining persons of the household
									BEGIN
										--Checks whehter someone else is on the trip with @per 
										DECLARE @PER_ORIG_XCORD AS real, @PER_ORIG_YCORD AS real, @PER_DEST_XCORD AS real, @PER_DEST_YCORD AS real;
										DECLARE @PER_ORIG_DEP_TIME AS int, @PER_DEST_ARR_TIME AS int;
										SET @PER_ORIG_XCORD = (SELECT ORIG_X FROM RevTripFile WHERE TRIP_NUM = @trip AND Rec_Num = @hh AND PER_ID = @per)
										SET @PER_ORIG_YCORD = (SELECT ORIG_Y FROM RevTripFile WHERE TRIP_NUM = @trip AND Rec_Num = @hh AND PER_ID = @per)
										SET @PER_DEST_XCORD = (SELECT DEST_X FROM RevTripFile WHERE TRIP_NUM = @trip AND Rec_Num = @hh AND PER_ID = @per)
										SET @PER_DEST_YCORD = (SELECT DEST_Y FROM RevTripFile WHERE TRIP_NUM = @trip AND Rec_Num = @hh AND PER_ID = @per)
										SET @PER_ORIG_DEP_TIME = (SELECT (60 * ORIG_DEP_HR) + ORIG_DEP_MIN FROM RevTripFile WHERE TRIP_NUM = @trip AND Rec_Num = @hh AND PER_ID = @per)
										SET @PER_DEST_ARR_TIME = (SELECT (60 * DEST_ARR_HR) + DEST_ARR_MIN FROM RevTripFile WHERE TRIP_NUM = @trip AND Rec_Num = @hh AND PER_ID = @per)
										IF EXISTS(SELECT * FROM RevTripFile WHERE Rec_Num = @hh AND PER_ID = @pernext 
													AND ORIG_X = @PER_ORIG_XCORD AND ORIG_Y = @PER_ORIG_YCORD 
													AND DEST_X = @PER_DEST_XCORD AND DEST_Y = @PER_DEST_YCORD
													AND (60 * ORIG_DEP_HR) + ORIG_DEP_MIN >= (@PER_ORIG_DEP_TIME - 4)  AND (60 * ORIG_DEP_HR) + ORIG_DEP_MIN <= (@PER_ORIG_DEP_TIME + 4)
													AND (60 * DEST_ARR_HR) + DEST_ARR_MIN >= (@PER_DEST_ARR_TIME - 4) AND (60 * DEST_ARR_HR) + DEST_ARR_MIN <= (@PER_DEST_ARR_TIME + 4))
													
											BEGIN
												SELECT @joint = COALESCE(@joint + '.', '') + CAST(PER_ID AS VARCHAR(50)) FROM RevTripFile
													WHERE Rec_Num = @hh AND PER_ID = (SELECT @pernext WHERE Rec_Num = @hh AND ORIG_X = @PER_ORIG_XCORD AND ORIG_Y = @PER_ORIG_YCORD 
													AND DEST_X = @PER_DEST_XCORD AND DEST_Y = @PER_DEST_YCORD
													AND (60 * ORIG_DEP_HR) + ORIG_DEP_MIN >= (@PER_ORIG_DEP_TIME - 4)  AND (60 * ORIG_DEP_HR) + ORIG_DEP_MIN <= (@PER_ORIG_DEP_TIME + 4)
													AND (60 * DEST_ARR_HR) + DEST_ARR_MIN >= (@PER_DEST_ARR_TIME - 4) AND (60 * DEST_ARR_HR) + DEST_ARR_MIN <= (@PER_DEST_ARR_TIME + 4))								
											END
										END
								SET @pernext = @pernext + 1
							END
						--Generate a jointflag identifying who else was on the trip with @per (NOTE: JointFlag also includes @per)	
						UPDATE RevTripFile SET JointFlag = (SELECT @joint) WHERE Rec_Num = @hh AND PER_ID = @per AND TRIP_NUM = @trip	
						SET @joint = NULL
						SET @trip = @trip + 1	
					END
				SET @per = @per + 1;
			END
		SET @hh = @hh + 1;
	END	
	
ALTER TABLE RevTripFile ADD NUMBER_HH int,PERSON_1 int,PERSON_2 int,PERSON_3 int,PERSON_4 int,
PERSON_5 int,PERSON_6 int,PERSON_7 int,PERSON_8 int,PERSON_9 int,PERTYPE_1 VARCHAR(75),
PERTYPE_2 VARCHAR(75),PERTYPE_3 VARCHAR(75),PERTYPE_4 VARCHAR(75),PERTYPE_5 VARCHAR(75),
PERTYPE_6 VARCHAR(75),PERTYPE_7 VARCHAR(75),PERTYPE_8 VARCHAR(75),PERTYPE_9 VARCHAR(75),
CNT int,PARCNT int,FULLY_JOINT int,PARTIALLY_JOINT int,COMPOSITION int;
GO

UPDATE RevTripFile SET CNT = f.CNT, PARCNT = f.CNT
	FROM(SELECT Rec_Num,PER_ID,TOUR_ID,COUNT(DISTINCT JointFlag) AS CNT from RevTripFile 
			GROUP BY Rec_Num,PER_ID,TOUR_ID) AS f,RevTripFile R
WHERE R.Rec_Num = f.Rec_Num AND R.PER_ID = f.PER_ID AND R.TOUR_ID = f.TOUR_ID
UPDATE RevTripFile SET CNT = 0 WHERE Len(JointFlag) = 1

UPDATE RevTripFile SET FULLY_JOINT = (CASE
	WHEN CNT = 1 THEN 1
	ELSE 0
  END)
UPDATE RevTripFile SET PARTIALLY_JOINT = (CASE
		WHEN FULLY_JOINT = 0 AND PARCNT != 1 THEN 1
		ELSE 0
										  END)
ALTER TABLE RevTripFile DROP COLUMN CNT, PARCNT;
GO

--Calculating several fields using JointFlag	
UPDATE RevTripfile SET NUMBER_HH = (LEN(JointFlag) - LEN(REPLACE(JointFlag, '.', ''))) + 1
--Persons on joint trip								
UPDATE RevTripFile SET PERSON_1 = PER_ID
UPDATE RevTripFile SET PERSON_2 = Jnt_Per FROM JointPersonFields(2) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
UPDATE RevTripFile SET PERSON_3 = Jnt_Per FROM JointPersonFields(3) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
UPDATE RevTripFile SET PERSON_4 = Jnt_Per FROM JointPersonFields(4) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
UPDATE RevTripFile SET PERSON_5 = Jnt_Per FROM JointPersonFields(5) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
UPDATE RevTripFile SET PERSON_6 = Jnt_Per FROM JointPersonFields(6) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
UPDATE RevTripFile SET PERSON_7 = Jnt_Per FROM JointPersonFields(7) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
UPDATE RevTripFile SET PERSON_8 = Jnt_Per FROM JointPersonFields(8) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
UPDATE RevTripFile SET PERSON_9 = Jnt_Per FROM JointPersonFields(9) J, RevTripFile R 
	WHERE R.Rec_Num = J.Rec_Num AND R.PER_ID = J.PER_ID AND R.TOUR_ID = J.TOUR_ID AND R.TRIP_ID = J.TRIP_ID 	
--Person type fields
UPDATE RevTripFile SET PERTYPE_1 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_1
UPDATE RevTripFile SET PERTYPE_2 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_2
UPDATE RevTripFile SET PERTYPE_3 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_3
UPDATE RevTripFile SET PERTYPE_4 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_4
UPDATE RevTripFile SET PERTYPE_5 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_5
UPDATE RevTripFile SET PERTYPE_6 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_6
UPDATE RevTripFile SET PERTYPE_7 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_7
UPDATE RevTripFile SET PERTYPE_8 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile	GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_8
UPDATE RevTripFile SET PERTYPE_9 = f.PERSONTYPE 
	FROM (SELECT Rec_Num,PER_ID,PERSONTYPE FROM RevTripFile GROUP BY Rec_Num,PER_ID,PERSONTYPE) AS f,RevTripFile R
	WHERE R.Rec_Num = f.Rec_num AND f.PER_ID = R.PERSON_9

--Update column for Composition
UPDATE RevTripFile SET COMPOSITION = -1;
UPDATE RevTripFile SET COMPOSITION = (CASE
				WHEN ((PERTYPE_1 IN ('FW','PW','US','NW','RE') OR PERTYPE_1 IS NULL) 
						AND (PERTYPE_2 IN ('FW','PW','US','NW','RE') OR PERTYPE_2 IS NULL)
						AND (PERTYPE_3 IN ('FW','PW','US','NW','RE') OR PERTYPE_3 IS NULL) 
						AND (PERTYPE_4 IN ('FW','PW','US','NW','RE') OR PERTYPE_4 IS NULL)
						AND (PERTYPE_5 IN ('FW','PW','US','NW','RE') OR PERTYPE_5 IS NULL) 
						AND (PERTYPE_6 IN ('FW','PW','US','NW','RE') OR PERTYPE_6 IS NULL)
						AND (PERTYPE_7 IN ('FW','PW','US','NW','RE') OR PERTYPE_7 IS NULL) 
						AND (PERTYPE_8 IN ('FW','PW','US','NW','RE') OR PERTYPE_8 IS NULL)
						AND (PERTYPE_9 IN ('FW','PW','US','NW','RE') OR PERTYPE_9 IS NULL))
					AND COMPOSITION = -1 THEN 1
				WHEN ((PERTYPE_1 IN ('DS','ND','PS') OR PERTYPE_1 IS NULL) 
						AND (PERTYPE_2 IN ('DS','ND','PS') OR PERTYPE_2 IS NULL)
						AND (PERTYPE_3 IN ('DS','ND','PS') OR  PERTYPE_3 IS NULL) 
						AND (PERTYPE_4 IN ('DS','ND','PS') OR PERTYPE_4 IS NULL)
						AND (PERTYPE_5 IN ('DS','ND','PS') OR  PERTYPE_5 IS NULL) 
						AND (PERTYPE_6 IN ('DS','ND','PS') OR PERTYPE_6 IS NULL)
						AND (PERTYPE_7 IN ('DS','ND','PS') OR  PERTYPE_7 IS NULL) 
						AND (PERTYPE_8 IN ('DS','ND','PS') OR PERTYPE_8 IS NULL)
						AND (PERTYPE_9 IN ('DS','ND','PS') OR  PERTYPE_9 IS NULL))
					AND COMPOSITION = -1 THEN 2
				ELSE 3
				END) WHERE FULLY_JOINT = 1											
--------------------------------------------------------------------------------------------
--END - Main Code Area
--------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------
--START - Create Individual AND Joint Tours (AND Trip) files
--------------------------------------------------------------------------------------------
-------------------------------------------
--1) Creating Individual AND Joint Tours
-------------------------------------------
--Removing any existing table FROM previous runs or 
--if the code was run manually by the user creating several tables for checking purposes
IF OBJECT_ID('dbo.IndividualTourFile') IS NOT NULL 
	DROP TABLE IndividualTourFile;
IF OBJECT_ID('dbo.JointTourFile') IS NOT NULL 
	DROP TABLE JointTourFile;

--Adding Partially_joint AND Fully_joint columns to TotalTourFile
ALTER TABLE TotalTourFile ADD FULLY_JOINT int, PARTIALLY_JOINT int;
GO
UPDATE TotalTourFile SET FULLY_JOINT = RevTripFile.FULLY_JOINT, PARTIALLY_JOINT = RevTripFile.PARTIALLY_JOINT 
	FROM RevTripFile,TotalTourFile
	WHERE TotalTourFile.HH_ID = RevTripFile.HH_ID AND TotalTourFile.PER_ID = RevTripFile.PER_ID 
		AND TotalTourFile.TOUR_ID =  RevTripFile.TOUR_ID
	
--Copying the Tour fields for individual tour records into IndividualTourFile WHERE fully_joint = 0
SELECT * INTO IndividualTourFile FROM TotalTourFile WHERE FULLY_JOINT = 0  ORDER BY HH_ID,PER_ID,TOUR_ID 

--Copying the Tour fields FROM TotalTourFile for Joint tour records into JointTourFile WHERE fully_joint = 1
SELECT DENSE_RANK () OVER (ORDER BY HH_ID) AS HH_Num, ROW_NUMBER()OVER(PARTITION BY HH_ID ORDER BY PER_ID,TOUR_ID) AS Ro_Num,* 
INTO JointTourFile FROM TotalTourFile WHERE FULLY_JOINT = 1 ORDER BY HH_ID,PER_ID,TOUR_ID 	

--Adjusting between fullyjoint AND partially joint for some of the records of JointTourFile	
ALTER TABLE JointTourFile ADD identify int;
GO
--Declaring a counter hh to trace all the households
DECLARE @hh AS int, @hhmax AS int;
SET @hh = 1;			--initial value
--Getting the count of households for which loop will run
SELECT @hhmax = MAX(HH_Num) FROM JointTourFile;
--loops through all the households
WHILE @hh <= @hhmax
	BEGIN	
		--Declaring a counter per to trace all the persons of the household hh
		DECLARE @temp AS int, @tempmax AS int;
		SET @temp = 1;			--initial value
		--Getting the count of person of the household @hh for which loop will run
		SELECT @tempmax = MAX(Ro_Num) FROM JointTourFile WHERE HH_Num = @hh;
		WHILE @temp <= @tempmax
			BEGIN
				UPDATE JointTourFile SET identify = (CASE
								WHEN 
									ORIG_X = SOME(SELECT ORIG_X FROM JointTourFile WHERE Ro_Num !=@temp AND HH_Num = @hh)
									AND	ORIG_Y = SOME(SELECT ORIG_Y FROM JointTourFile WHERE Ro_Num !=@temp AND HH_Num = @hh)
									AND	DEST_X = SOME(SELECT DEST_X FROM JointTourFile WHERE Ro_Num !=@temp AND HH_Num = @hh)
									AND DEST_Y = SOME(SELECT DEST_Y FROM JointTourFile WHERE Ro_Num !=@temp AND HH_Num = @hh)	THEN 1
								ELSE 0
													END) WHERE Ro_Num = @temp AND HH_Num = @hh
				SET @temp = @temp + 1;
			END
		SET @hh = @hh + 1;
	END

--Recode the Fullyjoint AND Partiallyjoint records for some misleading records
UPDATE JointTourFile SET FULLY_JOINT = 0, PARTIALLY_JOINT = 1 WHERE identify = 0;
--Also put back records to individualtourfile of individuals going for Mandatory Activities 
UPDATE JointTourFile SET FULLY_JOINT = 0, PARTIALLY_JOINT = 1 
	WHERE TOURPURP IN ('School','University','Work','WorkRelated') AND IS_SUBTOUR = 0
--Drop temporary columns
ALTER TABLE JointTourFile DROP COLUMN HH_NUM, Ro_Num, identify;
GO

--Inserting the records (that were not fullyjoint instead were partially joint) FROM JointTourFile To IndividualTourFile  
INSERT INTO IndividualTourFile SELECT * FROM JointTourFile 
	WHERE FULLY_JOINT = 0 ORDER BY HH_ID,PER_ID,TOUR_ID; 

--Updating revtripfile for fullyjoint AND partially joint records
UPDATE RevTripFile SET FULLY_JOINT = JointTourFile.FULLY_JOINT, PARTIALLY_JOINT = JointTourFile.PARTIALLY_JOINT 
FROM RevTripFile,JointTourFile WHERE JointTourFile.HH_ID = RevTripFile.HH_ID
	AND JointTourFile.PER_ID = RevTripFile.PER_ID AND JointTourFile.TOUR_ID =  RevTripFile.TOUR_ID;
--Also updating Fully_joint AND Partially_joint columns in TotalTourFile 
UPDATE TotalTourFile SET FULLY_JOINT = RevTripFile.FULLY_JOINT, PARTIALLY_JOINT = RevTripFile.PARTIALLY_JOINT
FROM RevTripFile,TotalTourFile WHERE TotalTourFile.HH_ID = RevTripFile.HH_ID 
	AND TotalTourFile.PER_ID = RevTripFile.PER_ID AND TotalTourFile.TOUR_ID =  RevTripFile.TOUR_ID;
	
--Delete the records from jointtourfile WHERE fully_joint=0
DELETE FROM JointTourFile WHERE FULLY_JOINT = 0;
		
--Adding additional columns from revtripfile to jointtourfile
ALTER TABLE JointTourFile ADD NUMBER_HH int,PERSON_1 int,PERSON_2 int,PERSON_3 int,PERSON_4 int,PERSON_5 int,
PERSON_6 int,PERSON_7 int,PERSON_8 int,PERSON_9 int,COMPOSITION int;
GO
UPDATE JointTourFile SET NUMBER_HH = RevTripFile.NUMBER_HH,PERSON_1 = RevTripFile.PERSON_1, 
			PERSON_2 = RevTripFile.PERSON_2,PERSON_3 = RevTripFile.PERSON_3,PERSON_4 = RevTripFile.PERSON_4, 
			PERSON_5 = RevTripFile.PERSON_5, PERSON_6 = RevTripFile.PERSON_6,PERSON_7 = RevTripFile.PERSON_7,
			PERSON_8 = RevTripFile.PERSON_8, PERSON_9 = RevTripFile.PERSON_9,COMPOSITION = RevTripFile.COMPOSITION
	FROM RevTripFile,JointTourFile
	WHERE JointTourFile.HH_ID = RevTripFile.HH_ID AND JointTourFile.PER_ID = RevTripFile.PER_ID 
			AND JointTourFile.TOUR_ID = RevTripFile.TOUR_ID 
GO
--Deleting Duplicate records FROM JointTourFile
WITH JointTourFile (HH_ID,duplicateRecordCount)
AS
(
SELECT HH_ID,
ROW_NUMBER()OVER(PARTITION BY HH_ID,ANCHOR_DEPART_HOUR,ANCHOR_DEPART_MIN,ORIG_X,ORIG_Y,DEST_X,DEST_Y ORDER BY HH_ID,PER_ID,TOUR_ID) 
	AS duplicateRecordCount 
FROM dbo.JointTourFile
)
DELETE
FROM JointTourFile
WHERE duplicateRecordCount > 1
GO
--Delete misleading records FROM JointTourFile
DELETE FROM JointTourFile WHERE (HH_ID = 4135331 AND PER_ID = 2) OR (HH_ID = 4258610 AND PER_ID = 3) OR (HH_ID = 4182173 AND PER_ID = 3) OR (HH_ID = 4228816 AND PER_ID = 5)

--Recoding TOUR_ID field in JointTourFile as duplicate rows have been deleted
UPDATE JointTourFile SET TOUR_ID =  ff.tourno 
FROM (SELECT DENSE_RANK () OVER (PARTITION BY HH_ID ORDER BY ANCHOR_DEPART_HOUR,ANCHOR_DEPART_MIN,PRIMDEST_ARRIVE_HOUR,PRIMDEST_ARRIVE_MIN) 
		AS tourno,* FROM JointTourFile) AS ff
WHERE JointTourFile.HH_ID = ff.HH_ID AND JointTourFile.PER_ID = ff.PER_ID
	AND JointTourFile.ORIG_PLACENO = ff.ORIG_PLACENO AND JointTourFile.DEST_PLACENO = ff.DEST_PLACENO
	AND JointTourFile.ANCHOR_DEPART_HOUR = ff.ANCHOR_DEPART_HOUR AND JointTourFile.ANCHOR_DEPART_MIN = ff.ANCHOR_DEPART_MIN 
	AND JointTourFile.PRIMDEST_ARRIVE_HOUR = ff.PRIMDEST_ARRIVE_HOUR AND JointTourFile.PRIMDEST_ARRIVE_MIN = ff.PRIMDEST_ARRIVE_MIN
--Recoding TOUR_ID field in IndividualTourFile
UPDATE IndividualTourFile SET TOUR_ID =  ff.tourno 
FROM (SELECT DENSE_RANK () OVER (PARTITION BY HH_ID,PER_ID ORDER BY ANCHOR_DEPART_HOUR,ANCHOR_DEPART_MIN,PRIMDEST_ARRIVE_HOUR,PRIMDEST_ARRIVE_MIN)
		AS tourno,* FROM IndividualTourFile) AS ff
WHERE IndividualTourFile.HH_ID = ff.HH_ID AND IndividualTourFile.PER_ID = ff.PER_ID AND IndividualTourFile.TOUR_ID = ff.TOUR_ID
	AND IndividualTourFile.ORIG_PLACENO = ff.ORIG_PLACENO AND IndividualTourFile.DEST_PLACENO = ff.DEST_PLACENO
	AND IndividualTourFile.ANCHOR_DEPART_HOUR = ff.ANCHOR_DEPART_HOUR AND IndividualTourFile.ANCHOR_DEPART_MIN = ff.ANCHOR_DEPART_MIN 
	AND IndividualTourFile.PRIMDEST_ARRIVE_HOUR = ff.PRIMDEST_ARRIVE_HOUR AND IndividualTourFile.PRIMDEST_ARRIVE_MIN = ff.PRIMDEST_ARRIVE_MIN

--Delete column PER_ID,PERSONTYPE FROM JointTourFile
ALTER TABLE JointTourFile DROP COLUMN PER_ID,PERSONTYPE,DRIVER;
GO
---------------------------------------------------------------------------------

---------------------------------------------------------------------------------
--2) Creating Individual AND Joint Trips
---------------------------------------------------------------------------------
--Removing any existing table FROM previous runs or if the code was run manually
--by the user creating several tables for checking purposes
IF OBJECT_ID('dbo.IndividualTripFile') IS NOT NULL 
	DROP TABLE IndividualTripFile;
IF OBJECT_ID('dbo.JointTripFile') IS NOT NULL 
	DROP TABLE JointTripFile;

--Adding Partially_joint AND Fully_joint columns to TotalTripFile
ALTER TABLE TotalTripFile ADD FULLY_JOINT int, PARTIALLY_JOINT int;
GO
UPDATE TotalTripFile SET FULLY_JOINT = RevTripFile.FULLY_JOINT, PARTIALLY_JOINT = RevTripFile.PARTIALLY_JOINT 
FROM RevTripFile,TotalTripFile
	WHERE TotalTripFile.HH_ID = RevTripFile.HH_ID AND TotalTripFile.PER_ID = RevTripFile.PER_ID 
		AND TotalTripFile.TOUR_ID =  RevTripFile.TOUR_ID AND TotalTripFile.TRIP_ID =  RevTripFile.TRIP_ID;
--Copying the Trip fields for individual trip records into IndividualTripFile FROM TotalTripFile WHERE fully_joint = 0
SELECT * INTO IndividualTripFile FROM TotalTripFile WHERE FULLY_JOINT = 0 ORDER BY HH_ID,PER_ID,TOUR_ID,TRIP_ID;
--Copying the Trip fields FROM RevTripFile for Joint trip records into JointTripFile WHERE fully_joint = 1
SELECT * INTO JointTripFile FROM RevTripFile WHERE FULLY_JOINT = 1 ORDER BY HH_ID,PER_ID,TOUR_ID,TRIP_ID;
GO

--Deleting Duplicate records FROM JointTripFile
WITH JointTripFile (HH_ID,duplicateRecordCount)
AS
(
SELECT HH_ID,
ROW_NUMBER()OVER(PARTITION BY HH_ID,ORIG_DEP_HR,ORIG_DEP_MIN,ORIG_X,ORIG_Y,DEST_X,DEST_Y ORDER BY HH_ID,PER_ID,TOUR_ID,TRIP_ID) 
	AS duplicateRecordCount
FROM dbo.JointTripFile
)
DELETE
FROM JointTripFile
WHERE duplicateRecordCount > 1
GO
--Delete misleading records FROM JointTripFile
DELETE FROM JointTripFile WHERE (HH_ID = 4081631 AND PER_ID = 2) OR (HH_ID = 4135331 AND PER_ID = 2) OR (HH_ID = 4078096 AND PER_ID = 3) OR (HH_ID = 4306519 AND PER_ID = 2 AND TOUR_ID = 1 AND TRIP_ID = 3)

--Recoding TOUR_ID field in JointTripFile as duplicate rows have been deleted
UPDATE JointTripFile SET TOUR_ID = ff.tourno 
FROM (SELECT DENSE_RANK () OVER (PARTITION BY HH_ID ORDER BY TOUR_ID,PER_ID) AS tourno,* FROM JointTripFile) AS ff
WHERE JointTripFile.HH_ID = ff.HH_ID AND JointTripFile.PER_ID = ff.PER_ID AND JointTripFile.TOUR_ID = ff.TOUR_ID 
	AND JointTripFile.TRIP_ID = ff.TRIP_ID AND JointTripFile.ORIG_X = ff.ORIG_X AND JointTripFile.ORIG_Y = ff.ORIG_Y
	AND JointTripFile.DEST_X = ff.DEST_X AND JointTripFile.DEST_Y = ff.DEST_Y
	AND JointTripFile.ORIG_DEP_HR = ff.ORIG_DEP_HR AND JointTripFile.ORIG_DEP_MIN = ff.ORIG_DEP_MIN 
	AND JointTripFile.DEST_ARR_HR = ff.DEST_ARR_HR AND JointTripFile.DEST_ARR_MIN =  ff.DEST_ARR_MIN
--Recoding TOUR_ID field in IndividualTripFile
UPDATE IndividualTripFile SET TOUR_ID =  ff.tourno 
FROM (SELECT DENSE_RANK () OVER (PARTITION BY HH_ID,PER_ID ORDER BY TOUR_ID) AS tourno,* FROM IndividualTripFile) AS ff
WHERE IndividualTripFile.HH_ID = ff.HH_ID AND IndividualTripFile.PER_ID = ff.PER_ID AND IndividualTripFile.TOUR_ID = ff.TOUR_ID 
	AND IndividualTripFile.TRIP_ID = ff.TRIP_ID AND IndividualTripFile.ORIG_X = ff.ORIG_X AND IndividualTripFile.ORIG_Y = ff.ORIG_Y
	AND IndividualTripFile.DEST_X = ff.DEST_X AND IndividualTripFile.DEST_Y = ff.DEST_Y
	AND IndividualTripFile.ORIG_DEP_HR = ff.ORIG_DEP_HR AND IndividualTripFile.ORIG_DEP_MIN = ff.ORIG_DEP_MIN 
	AND IndividualTripFile.DEST_ARR_HR = ff.DEST_ARR_HR AND IndividualTripFile.DEST_ARR_MIN =  ff.DEST_ARR_MIN

--Drop temporary columns
ALTER TABLE JointTripFile DROP COLUMN Rec_Num,TRIP_NUM,PER_ID,PERSONTYPE,jointflag,DRIVER,
PERTYPE_1,PERTYPE_2,PERTYPE_3,PERTYPE_4,PERTYPE_5,PERTYPE_6,PERTYPE_7,PERTYPE_8,PERTYPE_9;
GO
---------------------------------------------------------------------------------
--END - Create Individual AND Joint Tours (AND Trip) files
---------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------
--OUTPUT - IndividualTripFile,JointTripFile,IndividualTourFile, JointTourFile (in SQL Server database)
-------------------------------------------------------------------------------------------------------
PRINT 'Part - 5 execution is over and end time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)