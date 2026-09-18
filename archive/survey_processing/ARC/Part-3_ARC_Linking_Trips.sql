--ARC Project on coding tours and trips using Regional Household Travel Survey Data
--Part-3: Link Trips(Linkage required for Change Mode Actiivities before coding tours and trips)
--Palvinder Singh			10/24/2012			singhp@pbworld.com
----------------------------------------------------------------------------------------

---------------------------------------------
--INPUT - Modified Place table FROM Part-2
---------------------------------------------

PRINT 'Part - 3 is being executed and start time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)
SET NOCOUNT ON;	
-----------------------------------------------------------------------------------------------------------------
--START - Create an input table (for calculations) and initialize setup before running the script to link trips                   
-----------------------------------------------------------------------------------------------------------------
--Removing any existing table FROM previous runs or if the code was run manually by the user creating several
--tables for checking purposes
IF OBJECT_ID('dbo.templace') IS NOT NULL 
	DROP TABLE dbo.templace;
IF OBJECT_ID('dbo.LinkedTripTable') IS NOT NULL 
	DROP TABLE dbo.LinkedTripTable;	
--Creating a temporary table FROM place table for doing all the calculations.
--Also a household sequence is added to the table 
SELECT DENSE_RANK () OVER (ORDER BY SAMPN) AS hhseq, * INTO dbo.templace FROM dbo.Place;

-----------------------------------------------------------------------------------------------------------------
--END - Create an input table (for calculations) and initialize setup before running the script to link trips               
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--START - Initial modifications in the input table ("templace") and copying the structure of templace into 
--the output file ('LinkedTripTable')
-----------------------------------------------------------------------------------------------------------------
--Add columns required while generating the linked trips table
ALTER TABLE dbo.templace ADD
	ChMoIdentifier int,BegChMode int,EndChMode int,Ro_gen int,Mode1 VARCHAR(200),
	TOLL1 VARCHAR(25),TOTTR1 VARCHAR(25),MODECODE int,BOARDING_PLACENO int,
	BOARDING_PNAME VARCHAR(100),BOARDING_X real,BOARDING_Y real,ALIGHTING_PLACENO int,
	ALIGHTING_PNAME VARCHAR(100),ALIGHTING_X real,ALIGHTING_Y real,PARKING_PLACENO int,
	PARKING_PNAME VARCHAR(100),PARKING_X real,PARKING_Y real;	
GO
--Replacing null values of toll and tottr to 0
--(needed while linking trips and generating to toll1 and tottr1 respectively)
UPDATE dbo.templace SET TOLL = 999 WHERE TOLL IS NULL;
UPDATE dbo.templace SET TOTTR = 0 WHERE TOTTR IS NULL;

--Copying the values of columns mode,toll and tottr into mode1,toll1 and tottr 
--Also,these columns will be modified in further calculations
UPDATE dbo.templace SET MODE1 = MODENAME;
UPDATE dbo.templace SET TOLL1 = TOLL;
UPDATE dbo.templace SET TOTTR1 = TOTTR;
--Copying the structure of templace into output table structure
SELECT * INTO dbo.LinkedTripTable
FROM dbo.templace
WHERE 1 = 2
--Create indexes on templace and LinkedTripTable
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno
		ON dbo.templace (hhseq,PERNO);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_plano
		ON dbo.templace (hhseq,PERNO,PLANO);
CREATE NONCLUSTERED INDEX ix_templace_hhseq_perno_rogen
		ON dbo.templace (hhseq,PERNO,Ro_gen);
CREATE NONCLUSTERED INDEX ix_LinkedTripTable_hhseq_perno_plano
		ON dbo.LinkedTripTable (hhseq,PERNO,PLANO)
-----------------------------------------------------------------------------------------------------------------
--START - Initial modifications in the input table ("templace") and copying the structure of templace into 
--the output file ('LinkedTripTable')
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--START - Main Code Area
-----------------------------------------------------------------------------------------------------------------
--Identify the rows where TPURP = 4 ('ChangeMode' activity)	
UPDATE dbo.templace SET ChMoIdentifier = (CASE
											WHEN AGGACT = 'ChangeMode' THEN 1
											ELSE 0
										  END);
--Creating a sequence for further operations
UPDATE dbo.templace SET Ro_gen = ff.Ro 
	FROM (SELECT ROW_NUMBER() OVER (PARTITION BY hhseq,PERNO ORDER BY PLANO) AS Ro,* FROM dbo.templace) AS ff
	WHERE dbo.templace.hhseq = ff.hhseq AND dbo.templace.PERNO = ff.PERNO AND dbo.templace.PLANO = ff.PLANO;
			
UPDATE dbo.templace SET ChMoIdentifier = NULL WHERE Ro_gen = 1;

--Declaring the household counter hh so all households can be traced
DECLARE @hh AS int,@hhmax AS int;
SET @hh = 1; 			--initial value
--Getting the count of households for which loop will run
SELECT @hhmax = MAX(hhseq) FROM templace;
--Loop through sampns and do calculations
WHILE @hh <= @hhmax
	BEGIN		
		PRINT 'The Current Household is : ' + CAST(@hh AS VARCHAR) + ' and current time is : ' + CAST(SYSDATETIME() AS VARCHAR)

		--Declaring the person counter @per within a household @hh to trace all the persons of the household @hh
		DECLARE @per AS int,@permax AS int;
		SET @per = 1;			--initial value
		--Getting the count of persons in a given household for which loop will run
		SELECT @permax = MAX(PERNO) FROM dbo.templace WHERE hhseq = @hh;
		--loop through all persons of a given household
		WHILE @per <= @permax
			BEGIN
				--Following WHILE loop runs only if person @per of household @hh reports any change mode activity 
				IF EXISTS(SELECT * FROM dbo.templace WHERE ChMoIdentifier = 1 AND hhseq = @hh AND PERNO = @per)
					BEGIN
						DECLARE @i AS int, @k as int;
						SELECT @i = 1, @k = 1;
						--Loops through all the places for person @per of household @hh
						WHILE @i <= (SELECT MAX(Ro_gen) FROM dbo.templace WHERE hhseq = @hh AND PERNO = @per)
							BEGIN
							--Update only the rows where ChMoIdentifier = 0(For example; If the pattern is : 
							--1.Home(NULL) - 2.Chmode(1) - 3.Chmode(1)- 4.Discretionary(0)- 5.Chmode(1) - 6.Work(0)
							-- 7.Chmode(1) - 8.Chmode(1) - 9.Home(0)-- gives Home(NULL) - Discretionary(0) - Work(0) - Home(0))
								IF EXISTS(SELECT ChMoIdentifier FROM dbo.templace WHERE ChMoIdentifier = 0 AND hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
									BEGIN
										--Identify PLANO where ChMoIdentifier = 1 starts between two zeroes)
										UPDATE dbo.templace SET BegChMode = (SELECT Link_BEGCHMODE FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i;
										--Identify PLANO where ChMoIdentifier = 1 ends between two zeroes
										UPDATE dbo.templace SET EndChMode = (SELECT Link_ENDCHMODE FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i;		
										--Creating Boarding related fields
										UPDATE dbo.templace SET BOARDING_PLACENO = (SELECT Link_BOARDING_PLACENO FROM  LinkTripsFields(@i,@k,@hh,@per)) 
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET BOARDING_PNAME = (SELECT Link_BOARDING_PNAME FROM  LinkTripsFields(@i,@k,@hh,@per)) 
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;											
										UPDATE dbo.templace SET BOARDING_X = (SELECT Link_BOARDING_X FROM  LinkTripsFields(@i,@k,@hh,@per)) 
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET BOARDING_Y = (SELECT Link_BOARDING_Y FROM  LinkTripsFields(@i,@k,@hh,@per)) 
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										--Creating Alighting related fields
										UPDATE dbo.templace SET ALIGHTING_PLACENO = (SELECT Link_ALIGHTING_PLACENO FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET ALIGHTING_PNAME = (SELECT Link_ALIGHTING_PNAME FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;										
										UPDATE dbo.templace SET ALIGHTING_X = (SELECT Link_ALIGHTING_X FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET ALIGHTING_Y = (SELECT Link_ALIGHTING_Y FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										--Creating Parking related fields
										UPDATE dbo.templace SET PARKING_PLACENO = (SELECT Link_PARKING_PLACENO FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;										
										UPDATE dbo.templace SET PARKING_PNAME = (SELECT Link_PARKING_PNAME FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;										
										UPDATE dbo.templace SET PARKING_X = (SELECT Link_PARKING_X FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;										
										UPDATE dbo.templace SET PARKING_Y = (SELECT Link_PARKING_Y FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;										
										--Creating some fields for viewing and checking purpose (to see whehter linkage is correct or not) 
										DECLARE @Modes VARCHAR(200) 
										SELECT @Modes = COALESCE(@Modes + '-', '') + MODENAME FROM dbo.templace
											WHERE MODENAME IS NOT NULL AND PLANO >= (SELECT BegChMode FROM dbo.templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
												AND PLANO <= (SELECT (EndChMode + 1) FROM dbo.templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
												AND hhseq = @hh AND PERNO = @per;
									
										DECLARE @toll VARCHAR(100) 
										SELECT @toll = COALESCE(@toll + '-', '') + CAST(TOLL AS VARCHAR(50)) FROM dbo.templace
											WHERE PLANO >= (SELECT BegChMode FROM dbo.templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
												AND PLANO <= (SELECT (EndChMode + 1) FROM dbo.templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
												AND hhseq = @hh AND PERNO = @per;
										
										DECLARE @tottr VARCHAR(100) 
										SELECT @tottr = COALESCE(@tottr + '-', '') + CAST(TOTTR AS VARCHAR(50))  FROM dbo.templace
											WHERE PLANO >= (SELECT BegChMode FROM dbo.templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
												AND PLANO <= (SELECT (EndChMode + 1) FROM dbo.templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
												AND hhseq = @hh AND PERNO = @per;										
											
										UPDATE dbo.templace SET MODE1 = (SELECT @Modes) WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET TOLL1 = (SELECT @toll) WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET TOTTR1 = (SELECT @tottr) WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										
										--Recode Toll field for linkedtrips where No toll('Free') is prefered over toll ('Pay')
										UPDATE dbo.templace SET TOLL = (SELECT Link_TOLL FROM LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										--Recode TOTTR field for linkedtrips to maximum of tottr on linkedtrip										
										UPDATE dbo.templace SET TOTTR = (SELECT Link_TOTTR FROM LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;										
										--Recode fields where Origin of trip will change
										UPDATE dbo.templace SET OTAZ = (SELECT Link_OTAZ FROM LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET ORIGIN_LONG = (SELECT Link_ORIGIN_LONG FROM  LinkTripsFields(@i,@k,@hh,@per)) 
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET ORIGIN_LAT = (SELECT Link_ORIGIN_LAT FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET OPNAME = (SELECT Link_OPNAME FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET ORIGIN_DEPTIME = (SELECT Link_ORIGIN_DEPTIME FROM  LinkTripsFields(@i,@k,@hh,@per)) 
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET TRPDUR =(SELECT Link_TRPDUR FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										UPDATE dbo.templace SET TRIPDIST = (SELECT Link_TRIPDIST FROM  LinkTripsFields(@i,@k,@hh,@per))
											WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i AND BegChMode IS NOT NULL;
										SELECT @Modes = NULL, @toll = NULL, @tottr = NULL
										SET @k = @i;
									END
								SET @i = @i + 1
							END
					END
				ELSE
					BEGIN
					--Creating Parking related fields
					IF EXISTS(SELECT * FROM dbo.templace WHERE MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS') AND hhseq = @hh AND PERNO = @per)
						BEGIN
							--Required while generating the fields related to Parking
							DECLARE @park1 AS int, @park2 AS int;
							SET @park1 = (SELECT TOP(1) Ro_gen - 1 FROM dbo.templace
								WHERE MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS','PASSENGER') AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);
											
							SET @park2 = (SELECT TOP(1) Ro_gen  FROM dbo.templace
								WHERE MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS','PASSENGER') AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen desc);
		                
							UPDATE dbo.templace SET PARKING_X = XCORD
								WHERE (Ro_gen = @park2 AND hhseq = @hh AND PERNO = @per) OR (Ro_gen = @park2 - 1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR ((Ro_gen = @park1 + 1 AND MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS') AND hhseq = @hh AND PERNO = @per) 
										AND (SELECT MODENAME FROM dbo.templace WHERE Ro_gen = @park1 + 2 AND hhseq = @hh AND PERNO = @per) 
												NOT IN ('RAIL','LOCALBUS','EXPRESSBUS'))
							
							UPDATE dbo.templace SET PARKING_Y = YCORD
								WHERE (Ro_gen = @park2 AND hhseq = @hh AND PERNO = @per) OR (Ro_gen = @park2 - 1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR ((Ro_gen = @park1 + 1 AND MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS') AND hhseq = @hh AND PERNO = @per) 
										AND (SELECT MODENAME FROM dbo.templace WHERE Ro_gen = @park1 + 2 AND hhseq = @hh AND PERNO = @per)
											NOT IN ('RAIL','LOCALBUS','EXPRESSBUS'))
							
							UPDATE dbo.templace SET PARKING_PLACENO = PLANO
								WHERE (Ro_gen = @park2 AND hhseq = @hh AND PERNO = @per) OR (Ro_gen = @park2 - 1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR ((Ro_gen = @park1 + 1 AND MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS') AND hhseq = @hh AND PERNO = @per)
										AND (SELECT MODENAME FROM dbo.templace WHERE Ro_gen = @park1 + 2 AND hhseq = @hh AND PERNO = @per)
											NOT IN ('RAIL','LOCALBUS','EXPRESSBUS'))		
							
							UPDATE dbo.templace SET PARKING_PNAME = PNAME
								WHERE (Ro_gen = @park2 AND hhseq = @hh AND PERNO = @per) OR (Ro_gen = @park2 - 1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per) 
									OR ((Ro_gen = @park1 + 1 AND MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS') AND hhseq = @hh AND PERNO = @per) 
										AND (SELECT MODENAME FROM dbo.templace WHERE Ro_gen = @park1 + 2 AND hhseq = @hh AND PERNO = @per)
											NOT IN ('RAIL','LOCALBUS','EXPRESSBUS'))									
						END
					END
				--Recoding the ChMoIdentifier of Top(1) row so that it does not get missed while copying data to table 'LinkedTripTable' 
				UPDATE dbo.templace SET ChMoIdentifier = 0 WHERE Ro_gen = 1 AND hhseq = @hh AND PERNO = @per;

				SET @per = @per + 1		
			END
		SET @hh = @hh + 1
	END		
--Insert records into LinkedTripTable 
INSERT INTO LinkedTripTable SELECT * FROM dbo.templace WHERE ChMoIdentifier = 0
--Updating the records where changemode is missing but transit is used (for example: rail is used as transit but no access or egress mode)
		UPDATE LinkedTripTable SET BOARDING_PLACENO = PLANO - 1 WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');
		UPDATE LinkedTripTable SET BOARDING_PNAME = OPNAME WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');
		UPDATE LinkedTripTable SET BOARDING_X = ORIGIN_LONG WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');
		UPDATE LinkedTripTable SET BOARDING_Y = ORIGIN_LAT WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');
		UPDATE LinkedTripTable SET ALIGHTING_PLACENO = PLANO WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');
		UPDATE LinkedTripTable SET ALIGHTING_PNAME = DPNAME WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');		
		UPDATE LinkedTripTable SET ALIGHTING_X = DEST_LONG WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');
		UPDATE LinkedTripTable SET ALIGHTING_Y = DEST_LAT WHERE MODE1 IN ('LOCALBUS','RAIL','EXPRESSBUS');

-----------------------------------------------------------------------------------------------------------------
--END - Main Code Area
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--START - Create MODECODE Column (required for creating tourmode and tripmode in totaltourfile and totaltripfile)
-----------------------------------------------------------------------------------------------------------------	
UPDATE LinkedTripTable SET MODECODE = -1;
UPDATE LinkedTripTable SET MODECODE = (CASE 
	--If Schoolbus is found in the string (SCHOOLBUS wins over all other modes) then MODE = 'SCHOOLBUS' or 18
	WHEN CHARINDEX('SCHOOLBUS', MODE1) > 0 THEN 18
	
	--Making sure that if taxi and school bus are present in any observation 
	--(Taxi wins over other modes but Schoolbus), then mode still remains schoolbus	
	WHEN MODE1 = 'TAXI' OR (CHARINDEX('TAXI', MODE1) > 0 AND CHARINDEX('SCHOOLBUS', MODE1) = 0)		THEN 19
	
	--If either mode1 is driver or (no local(or express) bus/rail is found but driver is found) 
	--and no. of persons on trip = 1 and toll free then MODE = 'AUTOSOV (FREE)' or 1
	WHEN TOTTR = 1 AND TOLL IN (2,9) AND  MODECODE = -1 
		AND (MODE1 = 'DRIVER' OR (CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('LOCALBUS', MODE1) = 0
			AND CHARINDEX('EXPRESSBUS', MODE1) = 0 AND CHARINDEX('RAIL', MODE1) = 0))	THEN 1
	--If either mode1 is driver or (no local(or express) bus/rail is found but driver is found)
	--and no. of persons on trip = 1 and toll pay then MODE = 'AUTOSOV (PAY)' or 2
	WHEN TOTTR = 1 AND TOLL = 1 AND MODECODE = -1 
		AND (MODE1 = 'DRIVER' OR (CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('LOCALBUS', MODE1) = 0
			AND CHARINDEX('EXPRESSBUS', MODE1) = 0 AND CHARINDEX('RAIL', MODE1) = 0))	THEN 2
	
	--If either mode1 is driver or passenger or (no local(or express) bus/rail is found but driver (or passenger)
	-- is found) and no. of persons on trip = 2 and toll free then MODE = 'AUTO 2 Person(FREE)' or 3
	WHEN TOTTR = 2 AND TOLL IN (2,9) AND MODECODE = -1
		AND (MODE1 = 'DRIVER' OR MODE1 = 'PASSENGER' OR 
		((CHARINDEX('DRIVER', MODE1) > 0 OR CHARINDEX('PASSENGER', MODE1) > 0) AND CHARINDEX('LOCALBUS', MODE1) = 0
			AND CHARINDEX('EXPRESSBUS', MODE1) = 0 AND CHARINDEX('RAIL', MODE1) = 0))	THEN 3
	--If either mode1 is driver or passenger or (no local(or express) bus/rail is found but driver (or passenger) 
	--is found) and no. of persons on trip = 2 and toll pay then MODE = 'AUTO 2 Person(PAY)' or 4
	WHEN TOTTR = 2 AND TOLL = 1 AND MODECODE = -1  
		AND (MODE1 = 'DRIVER' OR MODE1 = 'PASSENGER' OR 
		((CHARINDEX('DRIVER', MODE1) > 0 OR CHARINDEX('PASSENGER', MODE1) > 0) AND CHARINDEX('LOCALBUS', MODE1) = 0
			AND CHARINDEX('EXPRESSBUS', MODE1) = 0 AND CHARINDEX('RAIL', MODE1) = 0))	THEN 4
	
	--If either mode1 is driver or passenger or (no local(or express) bus/rail is found but driver (or passenger)
	--is found) and no. of persons on trip >=3 and toll free then MODE = 'AUTO 3+ Person(FREE)' or 5
	WHEN TOTTR >= 3 AND TOLL IN (2,9) AND MODECODE = -1  
		AND (MODE1 = 'DRIVER' OR MODE1 = 'PASSENGER' OR 
		((CHARINDEX('DRIVER', MODE1) > 0 OR CHARINDEX('PASSENGER', MODE1) > 0) AND CHARINDEX('LOCALBUS', MODE1) = 0
			AND CHARINDEX('EXPRESSBUS', MODE1) = 0 AND CHARINDEX('RAIL', MODE1) = 0))	THEN 5
	--If either mode1 is driver or passenger or (no local(or express) bus/rail is found but driver (or passenger) 
	--is found) and no. of persons on trip >=3 and toll pay then MODE = 'AUTO 3+ Person(PAY)' or 6
	WHEN TOTTR >= 3 AND TOLL = 1 AND MODECODE = -1 
		AND (MODE1 = 'DRIVER' OR MODE1 = 'PASSENGER' OR 
		((CHARINDEX('DRIVER', MODE1) > 0 OR CHARINDEX('PASSENGER', MODE1) > 0) AND CHARINDEX('LOCALBUS', MODE1) = 0
			AND CHARINDEX('EXPRESSBUS', MODE1) = 0 AND CHARINDEX('RAIL', MODE1) = 0))	THEN 6					
	
	--If mode1 is walk then mode = 'Walk' or 7 
	WHEN (MODE1 = 'WALK' OR (CHARINDEX('DRIVER', MODE1) = 0 AND CHARINDEX('PASSENGER', MODE1) = 0 
			AND CHARINDEX('LOCALBUS', MODE1) = 0 AND CHARINDEX('EXPRESSBUS', MODE1) = 0 AND CHARINDEX('RAIL', MODE1) = 0)) 
		AND MODECODE = -1 	THEN 7
	
	--If mode1 is bike/moped then mode = 'Bike/Moped' or 8
	WHEN MODE1 = 'BIKE/MOPED' AND MODECODE = -1 	THEN 8
	
	--If rail is found, walk may/may not be found in the string but no driver or passenger
	--is found in the string then MODE = 'WALK-RAIL' or 11
	WHEN (CHARINDEX('RAIL', MODE1) > 0) AND (CHARINDEX('DRIVER', MODE1) = 0 
												AND CHARINDEX('PASSENGER', MODE1) = 0) AND MODECODE = -1 	THEN 11
	--If express bus is found, walk may/may not be found in the string but no driver or passenger
	--is found in the string then MODE = 'WALK-EXPRESS' or 10
	WHEN (CHARINDEX('EXPRESSBUS', MODE1) > 0) AND (CHARINDEX('DRIVER', MODE1) = 0 
													AND CHARINDEX('PASSENGER', MODE1) = 0) AND MODECODE = -1	THEN 10
	--If local bus is found, walk may/may not be found in the string but no driver or passenger 
	--is found in the string then MODE = 'WALK-LOCAL' or 9
	WHEN (CHARINDEX('LOCALBUS', MODE1) > 0) AND (CHARINDEX('DRIVER', MODE1) = 0 
													AND CHARINDEX('PASSENGER', MODE1) = 0) AND MODECODE = -1 	THEN 9
	
	--If rail and driver are found, walk may/may not be found in the string but no passenger 
	--is found in the string then MODE = 'PNR-RAIL' or 14
	WHEN (CHARINDEX('RAIL', MODE1) > 0) AND ((CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('PASSENGER', MODE1) = 0) 
												OR (CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('PASSENGER', MODE1) > 0))
		AND MODECODE = -1 	THEN 14
	--If express bus and driver are found, walk may/may not be found in the string but no passenger
	--is found in the string then MODE = 'PNR-EXPRESS' or 13
	WHEN (CHARINDEX('EXPRESSBUS', MODE1) > 0) AND ((CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('PASSENGER', MODE1) = 0) 
												OR (CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('PASSENGER', MODE1) > 0)) 
		AND MODECODE = -1 	THEN 13
	--If local bus and driver are found, walk may/may not be found in the string but no passenger
	--is found in the string then MODE = 'PNR-LOCAL' or 12
	WHEN (CHARINDEX('LOCALBUS', MODE1) > 0) AND ((CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('PASSENGER', MODE1) = 0) 
												OR (CHARINDEX('DRIVER', MODE1) > 0 AND CHARINDEX('PASSENGER', MODE1) > 0))
		AND MODECODE = -1	 THEN 12

	--If rail and passenger are found, walk may/may not be found in the string but no driver
	--is found in the string then MODE = 'KNR-RAIL' or 17
	WHEN (CHARINDEX('RAIL', MODE1) > 0) AND (CHARINDEX('DRIVER', MODE1) = 0 AND CHARINDEX('PASSENGER', MODE1) > 0) 
		AND MODECODE = -1	 THEN 17
	--If express bus and passenger are found, walk may/may not be found in the string but no driver
	--is found in the string then MODE = 'KNR-EXPRESS' or 16
	WHEN (CHARINDEX('EXPRESSBUS', MODE1) > 0) AND (CHARINDEX('DRIVER', MODE1) = 0 AND CHARINDEX('PASSENGER', MODE1) > 0)
		AND MODECODE = -1 	THEN 16	
	--If local bus and passenger are found, walk may/may not be found in the string but no driver
	--is found in the string then MODE = 'KNR-LOCAL' or 15
	WHEN (CHARINDEX('LOCALBUS', MODE1) > 0) AND (CHARINDEX('DRIVER', MODE1) = 0 AND CHARINDEX('PASSENGER', MODE1) > 0)
		AND MODECODE = -1 	THEN 15
	--IF no match with above categories is found, then modename is 'OTHER'
	WHEN (MODE1 = 'OTHER' OR (CHARINDEX('OTHER', MODE1) > 0 AND 
								(CHARINDEX('DRIVER', MODE1) = 0 AND CHARINDEX('PASSENGER', MODE1) = 0))) 
		AND MODECODE = -1 THEN 20 
							END)
--Remove columns from LinkedTripTabe that are not required in further scripts and also drop table templace from database 							
ALTER TABLE LinkedTripTable DROP COLUMN MODE,MODE1,TOLL,TOLL1,TOTTR1,BegChMode,EndChMode,ChMoIdentifier,PerType
IF OBJECT_ID('dbo.templace') IS NOT NULL 
	DROP TABLE templace;
-----------------------------------------------------------------------------------------------------------------
--END - Create MODECODE Column (required for creating tourmode and tripmode in totaltourfile and totaltripfile)
------------------------------------------------------------------------------------------------------------------	

------------------------------------------------------------------------------------------------------------------
--OUTPUT -  LinkedTripTable table (in SQL Server database)
------------------------------------------------------------------------------------------------------------------

PRINT 'Part - 3 execution is over and end time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)