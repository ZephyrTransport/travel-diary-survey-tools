# Household Travel Survey Trip Mode Standardizaion   

## PSRC HTS Trip Mode and Data Processing Work 
<details>

<summary> **CLICK HERE** to see details of PSRC work </summary>

### PSRC trip mode question on the survey

How did \<you/Name\> travel on this trip?

If \<you/Name\> used multiple modes or changed modes during your trip, please select all modes used.

-   Walk (or jog/wheelchair)
-   Bicycle or e-bicycle
-   Scooter, moped, skateboard
-   Bus, shuttle, or vanpool (public transit, private service, school bus, or shuttles for older adults and people with disabilities)
-   Rail (e.g., train, subway)
-   Ferry or water taxi
-   Household vehicle (or motorcycle)
-   Other vehicle (e.g., friend’s car, rental, carshare, work car)
-   Uber/Lyft, taxi, or car service
-   Other

#### follow up questions for trip mode 

Chapter 14.8 through 14.19 in the full [PSRC HTS questionnaire](https://www.psrc.org/media/8733)

  

### PSRC trip linking scripts and data preprocessing

- [trip linking SQL script](https://github.com/psrc/travel-survey-QC-Clean/blob/3b53454a82c3949232491af976e796144ba4667a/survey_data_cleaning/rulesy_link_trips.sql)

- PSRC mode variables in final linked trips

  | variable names | Description        |
  |----------------|--------------------|
  | mode_1         | Primary mode       |
  | mode_2         | Second mode chosen |
  | mode_3         | Third mode chosen  |
  | mode_4         | Fourth mode chosen |

  \* *mode_1\~mode_4 don't imply any chronological order*

- Final HTS tables use labeled values instead of numbered codes
   - to make the data available for a wider audience, PSRC decided to convert numbered codes into labeled values for all columns in our final HTS tables
   - HTS tables over the survey years (2017, 2019, 2021, 2023) are matched and combined into multi-year tables through extensive data processing
   - our variable grouping and recoding work (including mode standardization process) happen after this value labeling stage

   \* example data in the final table:
   
  | trip_id       | mode_1                            | mode_2                                       | mode_3 | mode_4 |
  |---------------|-----------------------------------|----------------------------------------------|--------|--------|     
  | 1710024801008 | Walk (or jog/wheelchair)	        | NULL                                         | NULL   | NULL   |
  | 1710024801009 | Walk (or jog/wheelchair)	        | NULL                                         | NULL   | NULL   |
  | 1710024801010 | Bicycle or e-bike (rSurvey only)	| NULL                                         | NULL   | NULL   |
  | 1710024801011 | Bicycle or e-bike (rSurvey only)	| Urban Rail (e.g., Link light rail, monorail) | NULL   | NULL   |

</details> 
   
## Mode Standardization Process with Example

The process organizes information in mode_1~mode_4 and assigns standardized trip modes to trip records 


### Step 1: Characterize value labels in mode_1~mode_4 into mode types

- this step simplifies the detailed mode labels into 10 mode types
- all value labels in mode_1~mode_4 are matched with a mode type in the following table
- a list of all existing value labels over survey years

  | mode type | Drive                                                 | Transit                                                 | Walk                     | Bike                                                |
  |:----------|:------------------------------------------------------|:--------------------------------------------------------|:-------------------------|:----------------------------------------------------|
  |           | Household vehicle 1                                   | Bus (public transit)                                    | Walk (or jog/wheelchair) | Bicycle or e-bike (rSurvey only)                    |
  |           | Household vehicle 2                                   | Ferry or water taxi                                     |                          | Other rented bicycle                                |
  |           | Household vehicle 3                                   | Commuter rail (Sounder, Amtrak)                         |                          | Borrowed bicycle (e.g., a friend's)                 |
  |           | Household vehicle 4                                   | Other rail                                              |                          | Standard bicycle (my household's)                   |
  |           | Household vehicle 5                                   | Other rail (e.g., streetcar)                            |                          | Bike-share - electric bicycle                       |
  |           | Household vehicle 6                                   | Urban Rail (e.g., Link light rail, monorail, streetcar) |                          | Bike-share - standard bicycle                       |
  |           | Household vehicle 7                                   | Urban Rail (e.g., Link light rail, monorail)            |                          | Bicycle owned by my household (rMove only)          |
  |           | Household vehicle 8                                   | Vehicle ferry (took vehicle on board)                   |                          | Borrowed bicycle (e.g., from a friend) (rMove only) |
  |           | Household vehicle 9                                   |                                                         |                          | Bike-share bicycle (rMove only)                     |
  |           | Household vehicle 10                                  |                                                         |                          | Other rented bicycle (rMove only)                   |
  |           | Other vehicle in household                            |                                                         |                          | Electric bicycle (my household's)                   |
  |           | Rental car                                            |                                                         |                          |                                                     |
  |           | Carshare service (e.g., Turo, Zipcar, Getaround, GIG) |                                                         |                          |                                                     |
  |           | Carshare service (e.g., Turo, Zipcar, ReachNow)       |                                                         |                          |                                                     |
  |           | Vanpool                                               |                                                         |                          |                                                     |
  |           | Other non-household vehicle                           |                                                         |                          |                                                     |
  |           | Car from work                                         |                                                         |                          |                                                     |
  |           | Friend/colleague's car                                |                                                         |                          |                                                     |
  |           | Other motorcycle/moped/scooter                        |                                                         |                          |                                                     |
  |           | Other motorcycle in household                         |                                                         |                          |                                                     |
  |           | Other motorcycle (not my household's)                 |                                                         |                          |                                                     |
  |           | Other motorcycle/moped                                |                                                         |                          |                                                     |
  |           | Personal scooter or moped (not shared)                |                                                         |                          |                                                     |
  
  
  
  | mode type | Micromobility                                  | Ride Hail                                                             | School Bus | Airplane or helicopter | Other                                                 | Missing Response |
  |:----------|:-----------------------------------------------|:----------------------------------------------------------------------|:-----------|:-----------------------|:------------------------------------------------------|:-----------------|
  |           | Skateboard or rollerblade                      | Taxi (e.g., Yellow Cab)                                               | School bus | Airplane or helicopter | Private bus or shuttle                                | Missing Response |
  |           | Scooter or e-scooter (e.g., Lime, Bird, Razor) | Other hired service (Uber, Lyft, or other smartphone-app car service) |            |                        | Paratransit                                           |
  |           | Segway or Onewheel/electric unicycle           | Other hired car service (e.g., black car, limo)                       |            |                        | Other bus (rMove only)                                |
  |           | Scooter-share (e.g., Bird, Lime)               |                                                                       |            |                        | Other scooter, moped, skateboard                      |
  |           |                                                |                                                                       |            |                        | Other mode (e.g., skateboard, kayak, motorhome, etc.) |

### Step 2: assign detailed trip modes (`mode_class`) with hierarchy logic

- this step applies a hierarchical logic to assign trip modes to trip records (our PSRC trip mode variable is called mode_class)
- the trip records go through each level of the hierarchy from 1 to 10. If any mode type in mode_1~mode_4 of a trip record finds a match in the hierarchy, the corresponding trip mode is assigned to the record
- full list of value labels in mode_class

  | `mode_class`           |
  |:-----------------------|
  | Drive SOV/HOV 2/HOV 3+ |
  | Transit                |
  | Walk                   |
  | Bike                   |
  | Micromobility          |
  | School Bus             |
  | Ride hail              |
  | Other                  |
  | Missing Response       |

- hierarchy logic

  | level | mode type (if exist in any of mode_1~mode_4) | `mode_class` assignment    |
  |:------|:---------------------------------------------|:---------------------------|
  | 1     | Airplane or helicopter                       | Other                      |
  | 2     | School Bus                                   | School Bus                 |
  | 3     | Transit                                      | Transit                    |
  | 4     | Ride hail                                    | Ride hail                  |
  | 5     | Drive                                        | Drive SOV/HOV 2/HOV 3+ (*) |
  | 6     | Other                                        | Other                      |
  | 7     | Micromobility                                | Micromobility              |
  | 8     | Bike                                         | Bike                       |
  | 9     | Walk                                         | Walk                       |
  | 10    | Missing Response                             | Missing Response           |

  \* *Drive SOV/HOV 2/HOV 3+ is assigned by considering travelers_total:*
  
  \ \ \- *If travelers_total==1, assign Drive SOV*
  
  \ \ \- *If travelers_total==2, assign Drive HOV 2*
  
  \ \ \- *If travelers_total>=3, assign Drive HOV 3+*
  
  \ \ \- *(All Drive SOV made by children under 16 has been replaced with Drive HOV2)*

### Step 3: assign 5 standardized trip modes (`mode_class_5`)	

-	this step assign standardized trip modes to trip records. The standardized trip modes are widely used in analyses, presentations and reports

- list of value labels grouping mode_class to mode_class_5	

  | `mode_class_5` | Drive       | Transit | Walk | Bike/Micromobility | Other      | Missing Response |
  |:---------------|:------------|:--------|:-----|:-------------------|:-----------|:-----------------|
  |                | Drive SOV   | Transit | Walk | Bike               | Ride hail  | Missing Response |
  |                | Drive HOV2  |         |      | Micromobility      | School Bus |
  |                | Drive HOV3+ |         |      |                    | Other      |

### Example

1. original value labels in mode_1~mode_4

  | trip_ID | mode_1                           | mode_2                                       | mode_3 | mode_4 |
  |:--------|:---------------------------------|:---------------------------------------------|:-------|:-------|
  | 1       | Household vehicle 1              | NULL                                         | NULL   | NULL   |
  | 2       | Walk (or jog/wheelchair)         | NULL                                         | NULL   | NULL   |
  | 3       | Walk (or jog/wheelchair)         | Bicycle or e-bike (rSurvey only)             | NULL   | NULL   |
  | 4       | Bicycle or e-bike (rSurvey only) | Urban Rail (e.g., Link light rail, monorail) | NULL   | NULL   |
				
2. assign corresponding mode type

  | trip_ID | mode_1 type     | mode_2 type     | mode_3 type | mode_4 type |
  |:--------|:----------------|:----------------|:------------|:------------|
  | 1       | **Drive**       | NULL            | NULL        | NULL        |
  | 2       | **Walk**        | NULL            | NULL        | NULL        |
  | 3       | **Walk**        | **Bike**        | NULL        | NULL        |
  | 4       | **Bike**        | **Transit**     | NULL        | NULL        |

3. apply hierarchical logic to mode types

  | trip_ID | mode_1 type | mode_2 type | mode_3 type | mode_4 type | `mode_class`               |                                                                                                       |
  |:--------|:------------|:------------|:------------|:------------|:---------------------------|:------------------------------------------------------------------------------------------------------|
  | 1       | Drive       | NULL        | NULL        | NULL        | **Drive SOV/HOV 2/HOV 3+** | \* *matched at level 5: Drive (SOV/HOV 2/HOV3+ is determined by the number of travelers in the trip)* |
  | 2       | Walk        | NULL        | NULL        | NULL        | **Walk**                   | \* *matched at level 9: Walk*                                                                         |
  | 3       | Walk        | Bike        | NULL        | NULL        | **Bike**                   | \* *matched at level 8: Bike*                                                                         |
  | 4       | Bike        | Transit     | NULL        | NULL        | **Transit**                | \* *matched at level 3: Transit*                                                                      |

4. group detailed trip modes (`mode_class`) into standardized trip modes (`mode_class_5`)	

  | trip_ID | `mode_class`           | `mode_class_5`         |
  |:--------|:-----------------------|:-----------------------|
  | 1       | Drive SOV/HOV 2/HOV 3+ | **Drive**              |
  | 2       | Walk                   | **Walk**               |
  | 3       | Bike                   | **Bike/Micromobility** |
  | 4       | Transit                | **Transit**            |



