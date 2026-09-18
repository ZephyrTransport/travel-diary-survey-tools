--ARC Project on coding tours and trips using Regional Household Travel Survey Data
--Part-4:Code Tour and Trip files FROM LinkedTripTable
--Palvinder Singh				10/24/2012				singhp@pbworld.com
---------------------------------------------------------------------------------------------------------------
--Description
--1. It is the main part of the code for generating tour and trip files
--2.The data FROM Part-3 run i.e. "LinkedTripTable" is used as an input for this part of the code.
--3.It calls several stored procedures (available in "ARC_Tours_And_Trips_Stored_Procedures.sql")
--starting with procedure "AddColumns". It adds some required columns for further calculations.
--4.A stored procedure "IdentifyTours" is called which results in identifying whether a tour 
--	was made or not and if made, it provides the sequence of tours.
--4.A stored procedure "IdentifyWorkSubTours" is called which results in identifying whether 
--an at-work sub-tour is present wihin the main tour and if present, a sequence is generated.
--5.Now code loops through each household using a temporary variable @hh. Within each household,
--  trips and tours for each person are coded using a second WHILE loop that loops through all 
--  the persons using a temporary variable @per within each household. 
--6.After the tours have been identified for a particular person, a third WHILE loop is used to 
--go through each and every tour and capture several features of the tour. An additional WHILE
--loop is used if at-work subtour is identified in the main tour (identified using procedure 
--"IdentifyWorkSubTours").
--7.If a tour is identified (main tour with or without at-work subtours), several stored 
--	procedures are used with the following purposes :
--	a)"AnchorPrimaryDestination"	-	It identifes the primary destination of the tour(both main
--										tour and at-work subtours) using a fuzzy logic while providing
--										different scores to different activities. Activity with the
--										least score is identified as the Tour Activity/Purpose.
--	b)"StopActivityFields"			-	It calculates the various features of the outbound and inbound
--										stops (for example: OUTBOUND_STOPS,INBOUND_STOPS,STOP_XCORD,
--										STOP_YCORD,STOP_ARR_HR,STOP_ARR_MIN,STOP_DEP_HR,STOP_DEP_MIN,
--										STOP_DUR_HR,STOP_DUR_MIN,STOP_PURP)	
--	c)TotalTourFileCreation			-	It calculates the fields required for the tour file including;
--										ORIG_PLACENO,DEST_PLACENO,ORIG_X,ORIG_Y,DEST_X,DEST_Y,
--										ANCHOR_DEPART_HOUR,ANCHOR_DEPART_MIN,PRIMDEST_ARRIVE_HOUR,
--										PRIMDEST_ARRIVE_MIN,PRIMDEST_DEPART_HOUR,PRIMDEST_DEPART_MIN,
--										ANCHOR_ARRIVE_HOUR,ANCHOR_ARRIVE_MIN,TOURPURP,IS_SUBTOUR,
--										PARENT_TOUR_ID,HAS_SUBTOUR,CHILD_TOUR_ID,PARENT_TOUR_MODE,
--										TOURMODE,TOUR_DUR_HR,TOUR_DUR_MIN
--	d)TotalTripFileCreation			-	It calculates the fields required for the trip file including;
--										ORIG_ARR_HR,ORIG_ARR_MIN,ORIG_DEP_HR,ORIG_DEP_MIN,ORIG_PURP,
--										DEST_ARR_HR,DEST_ARR_MIN,DEST_DEP_HR,DEST_DEP_MIN,DEST_PURP,
--										TRIP_DUR_HR,TRIP_DUR_MIN,DRIVER,SUBTOUR,IS_INBOUND,
--										TRIPS_ON_JOURNEY,TRIPS_ON_TOUR,ORIG_IS_TOUR_ORIG,
--										ORIG_IS_TOUR_DEST,DEST_IS_TOUR_DEST,DEST_IS_TOUR_ORIG,AUTO_OCC
--	e)WriteTotalTourFile			-	Required fields are written to TotalTourFile 
--	f)WriteTotalTripFile			-	Required fields are written to TotalTripFile	                      				                                                              
---------------------------------------------------------------------------------------------------------------

---------------------------------------
--INPUT - LinkedTripTable FROM Part-3
----------------------------------------

PRINT 'Part - 4 is being executed and start time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)
SET NOCOUNT ON;	
--------------------------------------------------------------------------------------
--START - Initial Setup before running the script to generate trips and tour files    
--------------------------------------------------------------------------------------
--Removing any existing table FROM previous runs or if the code
-- was run manually by the user creating several tables for checking purposes
IF OBJECT_ID('dbo.templace') IS NOT NULL 
	DROP TABLE templace;
TRUNCATE TABLE TotalTourFile;
TRUNCATE TABLE TotalTripFile;
TRUNCATE TABLE ReqdTable;
--------------------------------------------------------------------------------------
--END - Initial Setup before running the script to generate trips and tour files 
--------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------
--START - Initial Modifications in the Input Table ("templace")
--------------------------------------------------------------------------------------
SELECT * INTO templace FROM LinkedTripTable	
--Add extra columns in the templace for analysis
EXEC AddColumns
GO
--Create templace indices
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_plano
		ON dbo.templace (hhseq,PERNO,PLANO);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno
		ON dbo.templace (hhseq,PERNO);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_rogen
		ON dbo.templace(hhseq,PERNO,Ro_gen);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_rogen_begtournum
		ON dbo.templace (hhseq,PERNO,Ro_gen,BEGTOURNUM);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_tourrecord
		ON dbo.templace (hhseq,PERNO,TourRecord);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_begtournum
		ON dbo.templace (hhseq,PERNO,BEGTOURNUM);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_begtournum_begworksubtournum
		ON dbo.templace (hhseq,PERNO,BEGTOURNUM,BEGWORKSUBTOURNUM);	
--Create ReqdTable indices (a table structure has been created in Part-1)
CREATE NONCLUSTERED INDEX ix_ReqdTable_tourrecord_worktourrecord
		ON dbo.ReqdTable (TourRecord,WorkTourRecord);
CREATE NONCLUSTERED INDEX ix_ReqdTable_tourRecord
		ON dbo.ReqdTable (TourRecord);
CREATE NONCLUSTERED INDEX ix_ReqdTable_begtourindex
		ON dbo.ReqdTable (BegTourIndex);
CREATE NONCLUSTERED INDEX ix_ReqdTable_hhseq_perno_begtourindex
		ON dbo.ReqdTable (hhseq,PERNO,BegTourIndex);
CREATE NONCLUSTERED INDEX ix_ReqdTable_hhseq_perno_begtournum
		ON dbo.ReqdTable (hhseq,PERNO,BEGTOURNUM);
CREATE NONCLUSTERED INDEX ix_ReqdTable_hhseq_perno_begtournum_begworksubtournum
		ON dbo.ReqdTable (hhseq,PERNO,BEGTOURNUM,BEGWORKSUBTOURNUM);
--------------------------------------------------------------------------------------
--END - Initial Modifications in the Input Table ("templace")
--------------------------------------------------------------------------------------

------------------------------
--START - Main Code Area
------------------------------
--Run Identify tours and identifyWorkSubTour stored procedures to identify main tours and  
--at-work sub-tours for all persons and households
EXEC IdentifyTours;
EXEC identifyWorkSubTour
ALTER TABLE templace DROP COLUMN ENDTOURNUM,HomeIdentifier, ENDWORKSUBTOURNUM, WorkIdentifier;
GO

--Declaring the household counter hh so all households can be traced
DECLARE @hh AS int, @hhmax AS int;
SET @hh = 1;			--initial value
--Getting the count of households for which loop will run
SELECT @hhmax = MAX(hhseq) FROM templace;
--Loop through sampns and do calculations
WHILE @hh <= @hhmax
	BEGIN
		--Printing the current household number 
		PRINT 'The Current Household is : ' + CAST(@hh AS VARCHAR) + ' and current time is : ' + CAST(SYSDATETIME() AS VARCHAR);
		--Declaring the person counter @per within a household @hh to trace all the persons of the household @hh
		DECLARE @per AS int, @permax AS int;
		SET @per = 1;			--initial value
		--Getting the count of persons in a given household for which loop will run
		SELECT @permax = MAX(PERNO) FROM templace WHERE hhseq = @hh; 
		--Loop through persons for a given household
		WHILE @per <= @permax
			BEGIN
				--Declaring the tour counter "tour" to trace all the tours for person @per in household @hh
				DECLARE @tour AS int,@tourmax AS int,@tourid AS int;
				SELECT @tour = 1,@tourmax = MAX(BEGTOURNUM),@tourid = 1 FROM templace WHERE hhseq = @hh AND PERNO = @per
	
				--Loop through all the tours of person @per in a household @hh (if any positive value is found for BEGTOURNUM)				
				WHILE @tour <= @tourmax
					BEGIN
						--Populate ReqdTable with values from templace for @tour
						INSERT INTO ReqdTable SELECT * FROM templace WHERE hhseq = @hh AND PERNO = @per AND BEGTOURNUM = @tour			
						UPDATE ReqdTable SET Tour_ID = @tourid;	
						--Run AnchorPrimaryDestination stored procedure to determine the primary Destination Purpose of the tour
						EXEC AnchorPrimaryDestination
						--Run StopActivityFields stored procedure to generate the fields corresponding to outbound and inbound stops on the tour
						EXEC StopActivityFields
						--Run TotalTourFileCreation and TotalTripFileCreation stored procedures to generate required fields that are needed to 
						--write in the tour and trip file
						EXEC TotalTourFileCreation							
						EXEC TotalTripFileCreation
						--Run WriteTotalTripFile and WriteTotalTourFile stored procedures to write outputs to TotalTripFile and TotalTourFile
						EXEC WriteTotalTripFile
						EXEC WriteTotalTourFile
						--Truncate table ReqdTable so that it can be populated again in the next loop
						TRUNCATE TABLE dbo.ReqdTable
						SET @tourid = @tourid + 1;
						DECLARE @worktour AS int, @worktourmax AS int;
						SELECT @worktour = 1, @worktourmax = MAX(BEGWORKSUBTOURNUM) 
							FROM templace WHERE hhseq = @hh AND PERNO = @per AND BEGTOURNUM = @tour
						--Loop through all at-work subtours with the main tour (@tour) of person @per in a household @hh 
						--(if any positive value is found for BEGWORKSUBTOURNUM)
						WHILE @worktour <= @worktourmax
							BEGIN
								--Populate ReqdTable with values from templace for at-work sub-tour(@worktour) within the main tour (@tour)
								INSERT INTO ReqdTable SELECT * FROM templace 
									WHERE hhseq = @hh AND PERNO = @per AND BEGTOURNUM = @tour AND BEGWORKSUBTOURNUM = @worktour
								
								UPDATE ReqdTable SET Tour_ID =	@tourid;					
								--Run AnchorPrimaryDestination stored procedure to determine the primary Destination Purpose of the at-work subtour
								EXEC AnchorPrimaryDestination
								
								--Run StopActivityFields stored procedure to generate the fields corresponding to outbound and inbound stops on
								--the at-work subtour
								EXEC StopActivityFields
						        
								--Run TotalTourFileCreation and TotalTripFileCreation stored procedures to generate required fields that are needed to 
								--write in the tour and trip file
								EXEC TotalTourFileCreation							
								EXEC TotalTripFileCreation
								
								--Run WriteTotalTripFile and WriteTotalTourFile stored procedures to write outputs to TotalTripFile and TotalTourFile
								EXEC WriteTotalTripFile
								EXEC WriteTotalTourFile
								
								--Truncate table ReqdTable so that it can be populated again in the next loop (either this loop or loop just above)
								TRUNCATE TABLE dbo.ReqdTable
								SET @tourid = @tourid + 1;
								SET @worktour = @worktour + 1
							END
						SET @tour = @tour + 1	
					END	
				SET @per = @per + 1		
			END
		SET @hh  = @hh + 1
	END	
--------------------------
--END - Main Code Area
--------------------------
IF OBJECT_ID('dbo.templace') IS NOT NULL 
	DROP TABLE templace;

-----------------------------------------------------------------------------------------------------
--Creating New fields and changing several fields generated in both TotalTourFile and TotalTripFile
-----------------------------------------------------------------------------------------------------
--Tour duration adjustments for individuals returning next day(above script returns negative value
--Tour and trip duration hours)
UPDATE TotalTourFile SET TOUR_DUR_HR = 24 + TOUR_DUR_HR WHERE TOUR_DUR_HR < 0
UPDATE TotalTripFile SET TRIP_DUR_HR = 24 + TRIP_DUR_HR WHERE TRIP_DUR_HR < 0

---------------
--Separating School/University Tour and Trip Purposes to School and University purposes
--Also changing tourpurpose to Discretionary for misleading results (those who are not students 
--but making tours to school/university)
UPDATE TotalTourFile SET TOURPURP = 'School'
	FROM TotalTourFile,Person
	WHERE TotalTourFile.TOURPURP = 'School/University' AND Person.SCHOL IN (1,2,3,4) 
	AND TotalTourFile.HH_ID = Person.SAMPN AND TotalTourFile.PER_ID = Person.PERNO AND Person.Student = 1
UPDATE TotalTourFile SET TOURPURP = 'University'
	FROM TotalTourFile,Person
	WHERE TotalTourFile.TOURPURP = 'School/University' AND Person.SCHOL IN (5,6,7,8) 
	AND TotalTourFile.HH_ID = Person.SAMPN AND TotalTourFile.PER_ID = Person.PERNO AND Person.Student = 1
UPDATE TotalTourFile SET TOURPURP = 'Discretionary'
	FROM TotalTourFile,Person
	WHERE TotalTourFile.TOURPURP = 'School/University' AND Person.Student = 0
	AND TotalTourFile.HH_ID = Person.SAMPN AND TotalTourFile.PER_ID = Person.PERNO 

UPDATE TotalTripFile SET TOURPURP = 'School'
	FROM TotalTripFile,Person
	WHERE TotalTripFile.TOURPURP = 'School/University' AND Person.SCHOL IN (1,2,3,4) 
	AND TotalTripFile.HH_ID = Person.SAMPN AND TotalTripFile.PER_ID = Person.PERNO AND Person.Student = 1
UPDATE TotalTripFile SET TOURPURP = 'University'
	FROM TotalTripFile,Person
	WHERE TotalTripFile.TOURPURP = 'School/University' AND Person.SCHOL IN (5,6,7,8) 
	AND TotalTripFile.HH_ID = Person.SAMPN AND TotalTripFile.PER_ID = Person.PERNO AND Person.Student = 1
UPDATE TotalTripFile SET TOURPURP = 'Discretionary'
	FROM TotalTripFile,Person
	WHERE TotalTripFile.TOURPURP = 'School/University' AND Person.Student = 0
	AND TotalTripFile.HH_ID = Person.SAMPN AND TotalTripFile.PER_ID = Person.PERNO 
------------------
--Add Person Type fields to TotalTourFile and TotalTourFile
ALTER TABLE TotalTourFile ADD PERSONTYPE VARCHAR(5);
ALTER TABLE TotalTripFile ADD PERSONTYPE VARCHAR(5);
GO
UPDATE TotalTourFile SET PERSONTYPE = Person.PerType
	FROM Person WHERE Person.SAMPN = TotalTourFile.HH_ID AND Person.PERNO = TotalTourFile.PER_ID
UPDATE TotalTripFile SET PERSONTYPE = Person.PerType
	FROM Person WHERE Person.SAMPN = TotalTripFile.HH_ID AND Person.PERNO = TotalTripFile.PER_ID
------------------
--Add Expansion Factors to TotalTourFile and TotalTourFile
ALTER TABLE TotalTourFile ADD PEREXPFACT real,HHEXPFACT real;
ALTER TABLE TotalTripFile ADD PEREXPFACT real,HHEXPFACT real;
GO
UPDATE TotalTourFile SET PEREXPFACT = Person.EXPPWGT
	FROM Person WHERE Person.SAMPN = TotalTourFile.HH_ID AND Person.PERNO = TotalTourFile.PER_ID
UPDATE TotalTripFile SET PEREXPFACT = Person.EXPPWGT
	FROM Person WHERE Person.SAMPN = TotalTripFile.HH_ID AND Person.PERNO = TotalTripFile.PER_ID
	
UPDATE TotalTourFile SET HHEXPFACT = HH.EXPHHWGT
	FROM HH WHERE HH.SAMPN = TotalTourFile.HH_ID
UPDATE TotalTripFile SET HHEXPFACT = HH.EXPHHWGT
	FROM HH WHERE HH.SAMPN = TotalTripFile.HH_ID
-------------------------------------------------------------------------------
--OUTPUT - TotalTourFile  and TotalTripFile tables (in SQL Server database)
-------------------------------------------------------------------------------

PRINT 'Part - 4 execution is over and end time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)