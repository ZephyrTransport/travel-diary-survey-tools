"""Codebook enumerations for person table."""

from data_canon.core.labeled_enum import LabeledEnum


class AgeCategory(LabeledEnum):
    """age value labels."""

    canonical_field_name = "age"

    AGE_UNDER_5 = (1, "Under 5")
    AGE_5_TO_15 = (2, "5 to 15")
    AGE_16_TO_17 = (3, "16 to 17")
    AGE_18_TO_24 = (4, "18 to 24")
    AGE_25_TO_34 = (5, "25 to 34")
    AGE_35_TO_44 = (6, "35 to 44")
    AGE_45_TO_54 = (7, "45 to 54")
    AGE_55_TO_64 = (8, "55 to 64")
    AGE_65_TO_74 = (9, "65 to 74")
    AGE_75_TO_84 = (10, "75 to 84")
    AGE_85_AND_UP = (11, "85 and up")


class CommuteSubsidy(LabeledEnum):
    """commute_subsidy value labels, parent class to be referenced by specific subsidy types."""  # noqa: E501

    canonical_field_name = "commute_subsidy"

    FREE_PARK = (1, "Free parking provided by employer")
    DISCOUNT_PARKING = (
        2,
        "Discounted (partially subsidized) parking provided by employer",
    )
    TRANSIT = (3, "Free/discounted transit fare provided by employer")
    VANPOOL = (4, "Free/discounted vanpool service provided by employer")
    CASH_IN_LIEU = (5, "Cash in lieu for carpooling, biking, or walking")
    TNC = (
        6,
        "Free/discounted rideshare / TNC (e.g., Uber, Lyft) "
        "provided by employer",
    )
    CARSHARE = (
        7,
        "Free/discounted carshare membership provided by employer "
        "(e.g., Zipcar, Car2Go)",
    )
    SHUTTLE = (
        8,
        "Free/discounted shuttle service to/from work provided by employer",
    )
    BIKESHARE = (9, "Free/discounted bikeshare membership provided by employer")
    BIKE_MAINTENANCE = (
        10,
        "Free/discounted bike maintenance or bike parking provided by employer",
    )
    OTHER = (11, "Other commute subsidy provided by employer")
    NONE = (12, "No commute subsidies provided by employer")
    DONT_KNOW = (13, "Don't know")


class Education(LabeledEnum):
    """education value labels."""

    canonical_field_name = "education"

    LESS_HIGH_SCHOOL = (1, "Less than high school")
    HIGHSCHOOL = (2, "High school graduate/GED")
    SOME_COLLEGE = (3, "Some college, no degree")
    VOCATIONAL = (4, "Vocational/technical training")
    ASSOCIATE = (5, "Associate degree")
    BACHELORS = (6, "Bachelor's degree")
    GRAD = (7, "Graduate/post-graduate degree")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")


class Employment(LabeledEnum):
    """employment value labels."""

    canonical_field_name = "employment"

    EMPLOYED_FULLTIME = (1, "Employed full-time (paid)")
    EMPLOYED_PARTTIME = (2, "Employed part-time (paid)")
    EMPLOYED_SELF = (3, "Self-employed")
    UNEMPLOYED_NOT_LOOKING = (
        5,
        "Not employed and not looking for work "
        "(e.g., retired, stay-at-home parent, student)",
    )
    UNEMPLOYED_LOOKING = (6, "Unemployed and looking for work")
    EMPLOYED_UNPAID = (7, "Unpaid volunteer or intern")
    # NOTE This should include some number of hours per week
    EMPLOYED_FURLOUGHED = (
        8,
        "Employed, but not currently working (e.g., on leave, furloughed 100%)",
    )
    MISSING = (995, "Missing Response")
    # NOTE: This should be broken out into multiple categories if possible
    # UNEMPLOYED_PARENT = (6, "Not employed and not looking, full-time parent")
    # UNEMPLOYED_STUDENT = (7, "Not employed and not looking, enrolled as full-time student")  # noqa: E501
    # UNEMPLOYED_RETIRED = (8, "Not employed and not working, retired")


class Ethnicity(LabeledEnum):
    """ethnicity value labels."""

    canonical_field_name = "ethnicity"

    NOT_HISPANIC = (1, "Not Hispanic or Latino")
    MEXICAN = (2, "Mexican, Mexican American, Chicano")
    PUERTO_RICAN = (3, "Puerto Rican")
    CUBAN = (4, "Cuban")
    OTHER = (5, "Other Hispanic or Latino")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")


class Gender(LabeledEnum):
    """gender value labels."""

    canonical_field_name = "gender"

    FEMALE = (1, "Female")
    MALE = (2, "Male")
    NON_BINARY = (4, "Non-binary")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other/prefer to self-describe")
    PNTA = (999, "Prefer not to answer")


class Industry(LabeledEnum):
    """industry value labels."""

    canonical_field_name = "industry"

    AGRICULTURE = (1, "Agriculture, Forestry, Fishing, and Hunting")
    MINING = (2, "Mining, Quarrying, and Oil and Gas Extraction")
    UTILITIES = (3, "Utilities")
    CONSTRUCTION = (4, "Construction")
    MANUFACTURING = (5, "Manufacturing")
    WHOLESALE_TRADE = (6, "Wholesale Trade")
    RETAIL_TRADE = (7, "Retail Trade")
    TRANSPORTATION = (8, "Transportation and Warehousing")
    INFORMATION = (9, "Information")
    FINANCE_AND_INSURANCE = (10, "Finance and Insurance")
    REALESTATE = (11, "Real Estate and Rental and Leasing")
    PROFESSIONAL = (12, "Professional, Scientific, and Technical Services")
    MANAGEMENT = (13, "Management of Companies and Enteprises")
    ADMINISTRATIVE = (
        14,
        "Administrative and Support and Waste Management "
        "and Remediation Services",
    )
    EDUCATIONAL = (15, "Educational Services")
    HEALTH_AND_SOCIAL = (16, "Health Care and Social Assistance")
    ARTS_AND_RECREATION = (17, "Arts, Entertainment, and Recreation")
    ACCOMMODATION = (18, "Accommodation and Food Services")
    OTHER = (19, "Other Services (except Public Administration)")
    PUBLIC_ADMINISTRATION = (20, "Public Administration")
    MISSING = (995, "Missing Response")
    OTHER_SPECIFY = (997, "Other, please specify")


class JobCommuteType(LabeledEnum):
    """job_commute_type value labels."""

    canonical_field_name = "job_commute_type"

    FIXED = (1, "Go to one work location ONLY (outside of home)")
    VARIES = (2, "Work location regularly varies (different offices/jobsites)")
    WFH = (3, "Work ONLY from home or remotely (telework, self-employed)")
    DELIVERY = (4, "Drive/bike/travel for work (driver, sales, deliveries)")
    HYBRID = (
        5,
        "Work remotely some days and travel to a work location some days",
    )
    MISSING = (995, "Missing Response")


class Occupation(LabeledEnum):
    """occupation value labels."""

    canonical_field_name = "occupation"

    MANAGEMENT = (1, "Management")
    BUSINESS_FINANCE = (2, "Business and Financial Operations")
    COMPUTER_MATH = (3, "Computer and Mathematical")
    ARCH_ENG = (4, "Architecture and Engineering")
    SCIENCE = (5, "Life, Physical, and Social Science")
    COMMUNITY_SOCIAL = (6, "Community and Social Service")
    LEGAL = (7, "Legal")
    EDUCATION = (8, "Educational Instruction and Library")
    ARTS_MEDIA = (9, "Arts, Design, Entertainment, Sports, and Media")
    HEALTHCARE_PROFESSIONAL = (10, "Healthcare Practitioners and Technical")
    HEALTHCARE_SUPPORT = (11, "Healthcare Support")
    PROTECTIVE = (12, "Protective Service")
    FOOD_SERVICE = (13, "Food Preparation and Serving Related")
    CLEANING_MAINTENANCE = (14, "Building and Grounds Cleaning and Maintenance")
    PERSONAL_CARE = (15, "Personal Care and Service")
    SALES = (16, "Sales and Related")
    OFFICE_ADMIN = (17, "Office and Administrative Support")
    FARMING_FISHING = (18, "Farming, Fishing, and Forestry")
    CONSTRUCTION = (19, "Construction and Extraction")
    INSTALLATION_REPAIR = (20, "Installation, Maintenance, and Repair")
    PRODUCTION = (21, "Production")
    TRANSPORTATION = (22, "Transportation and Material Moving")
    MILITARY = (23, "Military Specific")
    MISSING = (995, "Missing Response")
    OTHER_PLEASE_SPECIFY = (997, "Other, please specify")


class Race(LabeledEnum):
    """race value labels."""

    canonical_field_name = "race"
    field_description = "Grouped race for the respondent"

    AFAM = (1, "African American or Black")
    NATIVE = (2, "American Indian or Alaska Native")
    ASIAN = (3, "Asian")
    PACIFIC = (4, "Native Hawaiian or Other Pacific Islander")
    WHITE = (5, "White")
    OTHER = (6, "Some other race")
    MULTI = (7, "Multiple races")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")


class Relationship(LabeledEnum):
    """relationship value labels."""

    canonical_field_name = "relationship"
    field_description = (
        "Indicates the relationship of the person to the primary respondent"
    )

    SELF = (0, "Self")
    SPOUSE_PARTNER = (1, "Spouse, partner")
    CHILD = (2, "Child or child-in-law")
    PARENT = (3, "Parent or parent-in-law")
    SIBLING = (4, "Sibling or sibling-in-law")
    OTHER_RELATIVE = (5, "Other relative (grandchild, cousin)")
    NONRELATIVE = (6, "Nonrelative (friend, roommate, household help)")


class RemoteClassFreq(LabeledEnum):
    """remote_class_freq value labels."""

    canonical_field_name = "remote_class_freq"

    REMOTESCHOOL_6_7_DAYS = (1, "6-7 days a week")
    REMOTESCHOOL_5_DAYS = (2, "5 days a week")
    REMOTESCHOOL_4_DAYS = (3, "4 days a week")
    REMOTESCHOOL_3_DAYS = (4, "3 days a week")
    REMOTESCHOOL_2_DAYS = (5, "2 days a week")
    REMOTESCHOOL_1_DAY = (6, "1 day a week")
    REMOTESCHOOL_1_3_PER_MONTH = (7, "1-3 days a month")
    LESS_THAN_MONTHLY = (8, "Less than monthly")
    MISSING = (995, "Missing Response")
    NEVER = (996, "Never")


class SchoolFreq(LabeledEnum):
    """school_freq value labels."""

    canonical_field_name = "school_freq"

    SCHOOL_6_7_DAYS = (1, "6-7 days a week")
    SCHOOL_5_DAYS = (2, "5 days a week")
    SCHOOL_4_DAYS = (3, "4 days a week")
    SCHOOL_3_DAYS = (4, "3 days a week")
    SCHOOL_2_DAYS = (5, "2 days a week")
    SCHOOL_1_DAY = (6, "1 day a week")
    SCHOOL_1_3_PER_MONTH = (7, "1-3 days a month")
    LESS_THAN_MONTHLY = (8, "Less than monthly")
    MISSING = (995, "Missing Response")
    NEVER = (996, "Never")


class SchoolType(LabeledEnum):
    """school_type value labels."""

    canonical_field_name = "school_type"

    ATHOME = (1, "Cared for at home")
    DAYCARE = (2, "Daycare outside home")
    PRESCHOOL = (3, "Preschool")
    HOME_SCHOOL = (4, "Home school")
    ELEMENTARY = (5, "Elementary school (public, private, charter)")
    MIDDLE_SCHOOL = (6, "Middle school (public, private, charter)")
    HIGH_SCHOOL = (7, "High school (public, private, charter)")
    VOCATIONAL = (10, "Vocational/technical school")
    COLLEGE_2YEAR = (11, "2-year college")
    COLLEGE_4YEAR = (12, "4-year college")
    GRADUATE_SCHOOL = (13, "Graduate or professional school")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")
    OTHER = (997, "Other")


class Student(LabeledEnum):
    """student value labels."""

    canonical_field_name = "student"

    FULLTIME_INPERSON = (
        0,
        "Full-time student, currently attending some or all classes in-person",
    )
    PARTTIME_INPERSON = (
        1,
        "Part-time student, currently attending some or all classes in-person",
    )
    NONSTUDENT = (2, "Not a student")
    PARTTIME_ONLINE = (3, "Part-time student, ONLY online classes")
    FULLTIME_ONLINE = (4, "Full-time student, ONLY online classes")
    MISSING = (995, "Missing Response")


class CommuteFreq(LabeledEnum):
    """commute and telework frequency value labels."""

    DAYS_6_7 = (1, "6-7 days a week")
    DAYS_5 = (2, "5 days a week")
    DAYS_4 = (3, "4 days a week")
    DAYS_3 = (4, "3 days a week")
    DAYS_2 = (5, "2 days a week")
    DAY_1 = (6, "1 day a week")
    DAYS_1_3_PER_MONTH = (7, "1-3 days a month")
    LESS_THAN_MONTHLY = (8, "Less than monthly")
    MISSING = (995, "Missing Response")
    NEVER = (996, "Never")


class Vehicle(LabeledEnum):
    """vehicle value labels."""

    canonical_field_name = "vehicle"
    field_description = "Indicates the vehicle the person primarily drives"

    HOUSEHOLD_VEHICLE_1 = (6, "Household vehicle 1")
    HOUSEHOLD_VEHICLE_2 = (7, "Household vehicle 2")
    HOUSEHOLD_VEHICLE_3 = (8, "Household vehicle 3")
    HOUSEHOLD_VEHICLE_4 = (9, "Household vehicle 4")
    HOUSEHOLD_VEHICLE_5 = (10, "Household vehicle 5")
    HOUSEHOLD_VEHICLE_6 = (11, "Household vehicle 6")
    HOUSEHOLD_VEHICLE_7 = (12, "Household vehicle 7")
    CARSHARE = (18, "A carshare vehicle (e.g., ZipCar)")
    MISSING = (995, "Missing Response")
    NONE = (996, "None (I do not drive a vehicle)")
    OTHER_VEHICLE = (997, "Other vehicle")


class PersonType(LabeledEnum):
    """Derived person type from employment status, student status, and age."""

    canonical_field_name = "person_type"
    field_description = (
        "Person type derived from employment, student status, and age"
    )

    FULL_TIME_WORKER = (1, "Full-time worker")
    PART_TIME_WORKER = (2, "Part-time worker")
    RETIRED = (3, "Non-working adult 65+")
    NON_WORKER = (4, "Non-working adult < 65")
    UNIVERSITY_STUDENT = (5, "University student")
    HIGH_SCHOOL_STUDENT = (6, "High school student 16+")
    CHILD_5_15 = (7, "Child 5-15")
    CHILD_UNDER_5 = (8, "Child 0-4")


class WorkParking(LabeledEnum):
    """work_park value labels."""

    canonical_field_name = "work_park"

    FREE = (1, "Parking is always free at/near work, at park & ride, etc.")
    EMPLOYER_PAYS_ALL = (2, "Employer pays ALL parking costs (for me)")
    EMPLOYER_DISCOUNT = (3, "Employer offers discounted parking (I pay some)")
    PERSONAL_PAY = (
        4,
        "I personally pay some or all parking costs (employer pays none)",
    )
    MISSING = (995, "Missing Response")
    NOT_APPLICABLE = (996, "Not applicable (I never drive to work)")
    DONT_KNOW = (998, "Don't know")
