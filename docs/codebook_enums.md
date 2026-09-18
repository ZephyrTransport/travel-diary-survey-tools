# Enums

Quick lookup of categorical values and labels for codebook enum fields.

## AccessEgressMode

| Value | Label |
| --- | --- |
| 1 | Walked (or jogged/wheelchair) |
| 2 | Bicycle |
| 3 | Transferred from another bus |
| 4 | Micromobility (e.g., scooter, moped, skateboard) |
| 5 | Transferred from other transit (e.g., rail, air) |
| 6 | Uber/Lyft, taxi, or car service |
| 7 | Drove and parked my own household's vehicle (or motorcycle) |
| 8 | Drove and parked another vehicle (or motorcycle) |
| 9 | Got dropped off in my own household's vehicle (or motorcycle) |
| 10 | Got dropped off in another vehicle (or motorcycle) |
| 995 | Missing Response |
| 997 | Other |

## AgeCategory

**Field name:** `age`

| Value | Label |
| --- | --- |
| 1 | Under 5 |
| 2 | 5 to 15 |
| 3 | 16 to 17 |
| 4 | 18 to 24 |
| 5 | 25 to 34 |
| 6 | 35 to 44 |
| 7 | 45 to 54 |
| 8 | 55 to 64 |
| 9 | 65 to 74 |
| 10 | 75 to 84 |
| 11 | 85 and up |

## AttendSchool

**Description:** Whether the person attends school on the survey day

| Value | Label |
| --- | --- |
| 1 | Yes, attend school at usual location |
| 2 | Yes, attend school at another location |
| 3 | No, do not attend school |
| 998 | Don't know |
| 999 | Prefer not to answer |
| 995 | Missing Response |

## BeginEndDay

**Description:** Location at the beginning or end of the day

| Value | Label |
| --- | --- |
| 1 | Home |
| 2 | Someone else's home |
| 3 | Work |
| 4 | Your/Their other home (e.g., other parent, second home) |
| 5 | Traveling (e.g., red-eye flight) |
| 7 | Temporary lodging (e.g., hotel, vacation rental) |
| 995 | Missing Response |
| 997 | Other |

## BicycleType

**Field name:** `bicycle_type`

| Value | Label |
| --- | --- |
| 1 | Standard |
| 2 | Electric |
| 3 | Other |
| 995 | Missing Response |

## CommuteFreq

| Value | Label |
| --- | --- |
| 1 | 6-7 days a week |
| 2 | 5 days a week |
| 3 | 4 days a week |
| 4 | 3 days a week |
| 5 | 2 days a week |
| 6 | 1 day a week |
| 7 | 1-3 days a month |
| 8 | Less than monthly |
| 995 | Missing Response |
| 996 | Never |

## Delivery

**Description:** Type of delivery received

| Value | Label |
| --- | --- |
| 1 | Take-out / prepared food delivery |
| 2 | Someone came to provide a service (e.g., cleaning, repair) |
| 3 | Groceries / other goods delivery |
| 4 | Postal package delivery (e.g., USPS, FedEx, UPS) |
| 5 | Postal package delivery other location (e.g., Amazon locker) |
| 6 | Postal package delivery work location |
| 7 | Other item delivery (e.g., furniture, appliance) |
| 8 | Other item delivery work location |
| 9 | None of the above |

## Driver

| Value | Label |
| --- | --- |
| 1 | Driver |
| 2 | Passenger |
| 3 | Both (switched drivers during trip) |
| 995 | Missing Response |

## Education

**Field name:** `education`

| Value | Label |
| --- | --- |
| 1 | Less than high school |
| 2 | High school graduate/GED |
| 3 | Some college, no degree |
| 4 | Vocational/technical training |
| 5 | Associate degree |
| 6 | Bachelor's degree |
| 7 | Graduate/post-graduate degree |
| 995 | Missing Response |
| 999 | Prefer not to answer |

## Employment

**Field name:** `employment`

| Value | Label |
| --- | --- |
| 1 | Employed full-time (paid) |
| 2 | Employed part-time (paid) |
| 3 | Self-employed |
| 5 | Not employed and not looking for work (e.g., retired, stay-at-home parent, student) |
| 6 | Unemployed and looking for work |
| 7 | Unpaid volunteer or intern |
| 8 | Employed, but not currently working (e.g., on leave, furloughed 100%) |
| 995 | Missing Response |

## Ethnicity

**Field name:** `ethnicity`

| Value | Label |
| --- | --- |
| 1 | Not Hispanic or Latino |
| 2 | Mexican, Mexican American, Chicano |
| 3 | Puerto Rican |
| 4 | Cuban |
| 5 | Other Hispanic or Latino |
| 995 | Missing Response |
| 999 | Prefer not to answer |

## FuelType

**Field name:** `fuel_type`

| Value | Label |
| --- | --- |
| 1 | Gas |
| 2 | Hybrid (HEV) |
| 3 | Plug-in hybrid (PHEV) |
| 4 | Electric (EV) |
| 5 | Diesel |
| 6 | Flex fuel (FFV) |
| 997 | Other (e.g., natural gas, bio-diesel) |

## Gender

**Field name:** `gender`

| Value | Label |
| --- | --- |
| 1 | Female |
| 2 | Male |
| 3 | Transgender |
| 4 | Non-binary |
| 995 | Missing Response |
| 997 | Other/prefer to self-describe |
| 999 | Prefer not to answer |

## HomeInRegion

**Field name:** `home_in_region`

| Value | Label |
| --- | --- |
| 0 | No |
| 1 | Yes |

## IncomeBroad

**Field name:** `income_bin`

| Value | Label |
| --- | --- |
| 1 | Under $25,000 |
| 2 | $25,000-$49,999 |
| 3 | $50,000-$74,999 |
| 4 | $75,000-$99,999 |
| 5 | $100,000-$199,999 |
| 6 | $200,000 or more |
| 995 | Missing Response |
| 999 | Prefer not to answer |

## IncomeDetailed

**Field name:** `income_detailed`

| Value | Label |
| --- | --- |
| 1 | Under $15,000 |
| 2 | $15,000-$24,999 |
| 3 | $25,000-$34,999 |
| 4 | $35,000-$49,999 |
| 5 | $50,000-$74,999 |
| 6 | $75,000-$99,999 |
| 7 | $100,000-$149,999 |
| 8 | $150,000-$199,999 |
| 9 | $200,000-$249,999 |
| 10 | $250,000 or more |
| 999 | Prefer not to answer |

## IncomeFollowup

**Field name:** `income_followup`

| Value | Label |
| --- | --- |
| 1 | Under $25,000 |
| 2 | $25,000-$49,999 |
| 3 | $50,000-$74,999 |
| 4 | $75,000-$99,999 |
| 5 | $100,000-$199,999 |
| 6 | $200,000 or more |
| 995 | Missing Response |
| 999 | Prefer not to answer |

## Industry

**Field name:** `industry`

| Value | Label |
| --- | --- |
| 1 | Agriculture, Forestry, Fishing, and Hunting |
| 2 | Mining, Quarrying, and Oil and Gas Extraction |
| 3 | Utilities |
| 4 | Construction |
| 5 | Manufacturing |
| 6 | Wholesale Trade |
| 7 | Retail Trade |
| 8 | Transportation and Warehousing |
| 9 | Information |
| 10 | Finance and Insurance |
| 11 | Real Estate and Rental and Leasing |
| 12 | Professional, Scientific, and Technical Services |
| 13 | Management of Companies and Enterprises |
| 14 | Administrative and Support and Waste Management and Remediation Services |
| 15 | Educational Services |
| 16 | Health Care and Social Assistance |
| 17 | Arts, Entertainment, and Recreation |
| 18 | Accommodation and Food Services |
| 19 | Other Services (except Public Administration) |
| 20 | Public Administration |
| 995 | Missing Response |
| 997 | Other, please specify |

## JobType

**Field name:** `job_type`

| Value | Label |
| --- | --- |
| 1 | Go to one work location ONLY (outside of home) |
| 2 | Work location regularly varies (different offices/jobsites) |
| 3 | Work ONLY from home or remotely (telework, self-employed) |
| 4 | Drive/bike/travel for work (driver, sales, deliveries) |
| 5 | Work remotely some days and travel to a work location some days |
| 995 | Missing Response |

## MadeTravel

**Description:** Whether the person made trips on the survey day

| Value | Label |
| --- | --- |
| 1 | Yes, made trips |
| 2 | No, did not go anywhere or make trips |
| 995 | Missing Response |
| 998 | Don't know |
| 999 | Prefer not to answer |

## Mode

| Value | Label |
| --- | --- |
| 1 | Walk/jog/wheelchair |
| 2 | Standard bicycle (household) |
| 3 | Borrowed bicycle |
| 4 | Other rented bicycle |
| 5 | Other |
| 6 | Household vehicle 1 |
| 7 | Household vehicle 2 |
| 8 | Household vehicle 3 |
| 9 | Household vehicle 4 |
| 10 | Household vehicle 5 |
| 11 | Household vehicle 6 |
| 12 | Household vehicle 7 |
| 13 | Household vehicle 8 |
| 14 | Household vehicle 9 |
| 15 | Household vehicle 10 |
| 16 | Other vehicle (household) |
| 17 | Rental car |
| 18 | Carshare (Zipcar, etc.) |
| 21 | Vanpool |
| 22 | Other vehicle (non-household) |
| 23 | Local public bus |
| 24 | School bus |
| 25 | Intercity bus (Greyhound, etc.) |
| 26 | Private shuttle/bus |
| 28 | Other bus |
| 27 | Paratransit/Dial-A-Ride |
| 30 | BART |
| 31 | Airplane/helicopter |
| 32 | Boat/ferry/water taxi |
| 33 | Work car |
| 34 | Friend/relative/colleague car |
| 36 | Regular taxi |
| 38 | University/college shuttle |
| 39 | Light rail |
| 41 | Intercity/commuter rail (ACE, Amtrak, Caltrain) |
| 42 | Other rail |
| 43 | Skateboard/rollerblade |
| 44 | Golf cart |
| 45 | ATV |
| 46 | Local public bus |
| 47 | Motorcycle (household) |
| 49 | Rideshare (Uber, Lyft, etc.) |
| 53 | MUNI Metro |
| 54 | Motorcycle (non-household) |
| 55 | Express/Transbay bus |
| 59 | Peer-to-peer rental (Turo, etc.) |
| 60 | Hired car (black car, limo) |
| 61 | Rapid transit bus (BRT) |
| 62 | Employer shuttle/bus |
| 63 | Medical transportation |
| 64 | Uber |
| 65 | Lyft |
| 66 | Other rideshare (not Uber/Lyft) |
| 67 | Local private bus |
| 68 | Cable car/streetcar |
| 69 | Bike-share (standard) |
| 70 | Bike-share (electric) |
| 71 | Scooter-share |
| 73 | Moped-share (Scoot, etc.) |
| 74 | Segway |
| 75 | Other |
| 76 | Carpool match (Waze, etc.) |
| 77 | Personal scooter/moped |
| 78 | Ferry/water taxi |
| 80 | Other boat (kayak, etc.) |
| 82 | Electric bicycle (household) |
| 83 | Scooter-share (Bird, Lime, etc.) |
| 100 | Household vehicle/motorcycle |
| 101 | Other vehicle (rental, carshare, etc.) |
| 102 | Bus/shuttle/vanpool |
| 103 | Bicycle |
| 104 | Other |
| 105 | Rail (train, BART, MUNI, etc.) |
| 106 | Uber/Lyft/taxi/car service |
| 107 | Micromobility (scooter, moped, etc.) |
| 995 | Missing Response |
| 997 | Other/Unknown |

## ModeType

| Value | Label |
| --- | --- |
| 1 | Walk |
| 2 | Bike |
| 3 | Bikeshare |
| 4 | Scootershare |
| 5 | Taxi |
| 6 | TNC |
| 7 | Other |
| 8 | Car |
| 9 | Carshare |
| 10 | School bus |
| 11 | Shuttle/vanpool |
| 12 | Ferry |
| 13 | Transit |
| 14 | Long distance passenger |
| 995 | Missing Response |

## NoSchoolReason

**Description:** Reason for not attending school

| Value | Label |
| --- | --- |
| 1 | Sick |
| 2 | Online / at home |
| 3 | Online / at other location |
| 4 | Vacation |
| 5 | Scheduled school closure (e.g., holiday) |
| 6 | Unscheduled school closure (e.g., weather) |
| 7 | Other |
| 995 | Missing Response |
| 998 | Don't know |
| 999 | Prefer not to answer |

## NoTravelReason

**Description:** Reason for not making trips

| Value | Label |
| --- | --- |
| 0 | I did make trips |
| 1 | No work/school, took day off |
| 2 | Worked from home (telework) |
| 3 | Just hung out at home |
| 4 | Scheduled school/work holiday |
| 5 | No transportation available |
| 6 | Sick or caring for sick household member |
| 7 | Waiting for a delivery or service at home |
| 8 | Remote learning / homeschooling |
| 9 | Bad weather (e.g., snowstorm) |
| 998 | Person made trips but don't know when or where |
| 997 | Other |
| 995 | Missing Response |
| 999 | Prefer not to answer |

## Occupation

**Field name:** `occupation`

| Value | Label |
| --- | --- |
| 1 | Management |
| 2 | Business and Financial Operations |
| 3 | Computer and Mathematical |
| 4 | Architecture and Engineering |
| 5 | Life, Physical, and Social Science |
| 6 | Community and Social Service |
| 7 | Legal |
| 8 | Educational Instruction and Library |
| 9 | Arts, Design, Entertainment, Sports, and Media |
| 10 | Healthcare Practitioners and Technical |
| 11 | Healthcare Support |
| 12 | Protective Service |
| 13 | Food Preparation and Serving Related |
| 14 | Building and Grounds Cleaning and Maintenance |
| 15 | Personal Care and Service |
| 16 | Sales and Related |
| 17 | Office and Administrative Support |
| 18 | Farming, Fishing, and Forestry |
| 19 | Construction and Extraction |
| 20 | Installation, Maintenance, and Repair |
| 21 | Production |
| 22 | Transportation and Material Moving |
| 23 | Military Specific |
| 995 | Missing Response |
| 997 | Other, please specify |

## ParticipationGroup

**Field name:** `participation_group`

**Description:** Indicates the survey mode used for signup and diary completion

| Value | Label |
| --- | --- |
| 1 | Signup via browserMove, Diary via browserMove |
| 2 | Signup via browserMove, Diary via call center |
| 3 | Signup via browserMove, Diary via rMove |
| 4 | Signup via call center, Diary via browserMove |
| 5 | Signup via call center, Diary via call center |
| 6 | Signup via call center, Diary via rMove |
| 7 | Signup via rMove, Diary via browserMove |
| 8 | Signup via rMove, Diary via call center |
| 9 | Signup via rMove, Diary via rMove |

## Purpose

| Value | Label |
| --- | --- |
| 1 | Went home |
| 2 | Went to work, work-related, volunteer-related |
| 3 | Attended school/class |
| 4 | Appointment, shopping, or errands (e.g., gas) |
| 5 | Dropped off, picked up, or accompanied another person |
| 7 | Social, leisure, religious, entertainment activity |
| 10 | Went to primary workplace |
| 11 | Went to work-related activity (e.g., meeting, delivery, worksite) |
| 12 | Traveling for work (e.g., business trip) |
| 13 | Volunteering |
| 14 | Other work-related |
| 21 | Attend K-12 school |
| 22 | Attend college/university |
| 23 | Attend other type of class (e.g., cooking class) |
| 24 | Attend other education-related activity (e.g., field trip) |
| 25 | Attend vocational education class |
| 26 | Attend daycare or preschool |
| 30 | Grocery shopping |
| 31 | Got gas |
| 32 | Other routine shopping (e.g., pharmacy) |
| 33 | Errand without appointment (e.g., post office) |
| 34 | Medical visit (e.g., doctor, dentist) |
| 36 | Shopping for major item (e.g., furniture, car) |
| 37 | Errand with appointment (e.g., haircut) |
| 40 | To/from childcare or preschool |
| 41 | To/from K-12 school or college |
| 42 | To/from other person's work or volunteer activity |
| 43 | To/from other person's scheduled activity (e.g., lesson, appointment) |
| 44 | Other activity only (e.g., attend meeting, pick-up or drop-off item) |
| 45 | Pick someone up |
| 46 | Drop someone off |
| 47 | Accompany someone only (e.g., go along for the ride) |
| 48 | BOTH pick up AND drop off |
| 50 | Dined out, got coffee, or take-out |
| 51 | Exercise or recreation (e.g., gym, jog, bike, walk dog) |
| 52 | Social activity (e.g., visit friends/relatives) |
| 53 | Leisure/entertainment/cultural (e.g., cinema, museum, park) |
| 54 | Religious/civic/volunteer activity |
| 55 | Vacation or leisure trip |
| 56 | Family activity (e.g., watch child's game) |
| 60 | Changed or transferred mode (e.g., waited for bus or exited bus) |
| 61 | Other errand |
| 62 | Other social |
| 99 | Other reason |
| 101 | Split/loop trip |
| 150 | Went to another residence (e.g., someone else's home, second home) |
| 152 | Went to temporary lodging (e.g., hotel, vacation rental) |
| 995 | Missing Response |
| 999 | Prefer not to answer |
| 996 | Not imputable |

## PurposeCategory

| Value | Label |
| --- | --- |
| 1 | Home |
| 2 | Work |
| 3 | Work related |
| 4 | School |
| 5 | School related |
| 6 | Escort |
| 7 | Shop |
| 8 | Meal |
| 9 | Social or recreational |
| 10 | Errand |
| 11 | Change mode |
| 12 | Overnight |
| 13 | Other |
| 995 | Missing Response |
| 999 | Prefer not to answer |
| 996 | Not imputable |

## Race

**Field name:** `race`

**Description:** Grouped race for the respondent

| Value | Label |
| --- | --- |
| 1 | African American or Black |
| 2 | American Indian or Alaska Native |
| 3 | Asian |
| 4 | Native Hawaiian or Other Pacific Islander |
| 5 | White |
| 6 | Some other race |
| 7 | Multiple races |
| 995 | Missing Response |
| 999 | Prefer not to answer |

## Relationship

**Field name:** `relationship`

**Description:** Indicates the relationship of the person to the primary respondent

| Value | Label |
| --- | --- |
| 0 | Self |
| 1 | Spouse, partner |
| 2 | Child or child-in-law |
| 3 | Parent or parent-in-law |
| 4 | Sibling or sibling-in-law |
| 5 | Other relative (grandchild, cousin) |
| 6 | Nonrelative (friend, roommate, household help) |

## RemoteClassFreq

**Field name:** `remote_class_freq`

| Value | Label |
| --- | --- |
| 1 | 6-7 days a week |
| 2 | 5 days a week |
| 3 | 4 days a week |
| 4 | 3 days a week |
| 5 | 2 days a week |
| 6 | 1 day a week |
| 7 | 1-3 days a month |
| 8 | Less than monthly |
| 995 | Missing Response |
| 996 | Never |

## ResidenceRentOwn

**Field name:** `residence_rent_own`

| Value | Label |
| --- | --- |
| 1 | Own/buying (paying a mortgage) |
| 2 | Rent |
| 3 | Housing provided by job or military |
| 4 | Provided by family or friend without payment or rent |
| 995 | Missing Response |
| 997 | Other |
| 999 | Prefer not to answer |

## ResidenceType

**Field name:** `residence_type`

| Value | Label |
| --- | --- |
| 1 | Single-family house (detached house) |
| 2 | Single-family house attached to one or more houses (rowhouse or townhouse) |
| 3 | Building with 2-4 units (duplexes, triplexes, quads) |
| 4 | Building with 5-49 apartments/condos |
| 5 | Building with 50 or more apartments/condos |
| 6 | Senior or age-restricted apartments/condos |
| 7 | Manufactured home/mobile home/trailer |
| 9 | Dorm, group quarters, or institutional housing |
| 995 | Missing Response |
| 997 | Other (e.g., boat, RV, van) |

## SchoolFreq

**Field name:** `school_freq`

| Value | Label |
| --- | --- |
| 1 | 6-7 days a week |
| 2 | 5 days a week |
| 3 | 4 days a week |
| 4 | 3 days a week |
| 5 | 2 days a week |
| 6 | 1 day a week |
| 7 | 1-3 days a month |
| 8 | Less than monthly |
| 995 | Missing Response |
| 996 | Never |

## SchoolType

**Field name:** `school_type`

| Value | Label |
| --- | --- |
| 1 | Cared for at home |
| 2 | Daycare outside home |
| 3 | Preschool |
| 4 | Home school |
| 5 | Elementary school (public, private, charter) |
| 6 | Middle school (public, private, charter) |
| 7 | High school (public, private, charter) |
| 10 | Vocational/technical school |
| 11 | 2-year college |
| 12 | 4-year college |
| 13 | Graduate or professional school |
| 995 | Missing Response |
| 999 | Prefer not to answer |
| 997 | Other |

## Student

**Field name:** `student`

| Value | Label |
| --- | --- |
| 0 | Full-time student, currently attending some or all classes in-person |
| 1 | Part-time student, currently attending some or all classes in-person |
| 2 | Not a student |
| 3 | Part-time student, ONLY online classes |
| 4 | Full-time student, ONLY online classes |
| 995 | Missing Response |

## TNCType

| Value | Label |
| --- | --- |
| 1 | Pooled/Shared Ridehail |
| 2 | Regular |
| 3 | Premium |
| 995 | Missing Response |
| 998 | Unknown TNC Service |

## TourCategory

| Value | Label |
| --- | --- |
| 1 | Start at anchor, end at anchor |
| 2 | Start at anchor, end away from anchor |
| 3 | Start away from anchor, end at anchor |
| 4 | Start away from anchor, end away from anchor |

## TourDataQuality

| Value | Label |
| --- | --- |
| 0 | Valid tour |
| 1 | Open end is another home known for this person |
| 2 | Chain resumes at the same place on the next diary day |
| 3 | First or last trip in the diary; its open end is not a known home |
| 4 | No activity to anchor the tour on: returns to the anchor without stopping, or every stop was a mode change |
| 5 | Missing leg, inside the tour or at its boundary |

## TourDirection

| Value | Label |
| --- | --- |
| 1 | Outbound half-tour |
| 2 | Inbound half-tour |

## TourType

| Value | Label |
| --- | --- |
| 1 | Home-based tour |
| 2 | Work-based tour (at-work subtour) |
| 3 | School-based tour (at-school subtour) |

## TravelDow

**Description:** Day of the week enumeration

| Value | Label |
| --- | --- |
| 1 | Monday |
| 2 | Tuesday |
| 3 | Wednesday |
| 4 | Thursday |
| 5 | Friday |
| 6 | Saturday |
| 7 | Sunday |

## Vehicle

**Field name:** `vehicle`

**Description:** Indicates the vehicle the person primarily drives

| Value | Label |
| --- | --- |
| 6 | Household vehicle 1 |
| 7 | Household vehicle 2 |
| 8 | Household vehicle 3 |
| 9 | Household vehicle 4 |
| 10 | Household vehicle 5 |
| 11 | Household vehicle 6 |
| 12 | Household vehicle 7 |
| 18 | A carshare vehicle (e.g., ZipCar) |
| 995 | Missing Response |
| 996 | None (I do not drive a vehicle) |
| 997 | Other vehicle |

## WorkParking

**Field name:** `work_park`

| Value | Label |
| --- | --- |
| 1 | Parking is always free at/near work, at park & ride, etc. |
| 2 | Employer pays ALL parking costs (for me) |
| 3 | Employer offers discounted parking (I pay some) |
| 4 | I personally pay some or all parking costs (employer pays none) |
| 995 | Missing Response |
| 996 | Not applicable (I never drive to work) |
| 998 | Don't know |
