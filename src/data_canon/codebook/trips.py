"""Codebook enumerations for trip table."""

from data_canon.core.labeled_enum import LabeledEnum


class Purpose(LabeledEnum):
    """Base class for purpose value labels."""

    HOME = (1, "Went home")
    WORK_VOLUNTEER = (2, "Went to work, work-related, volunteer-related")
    SCHOOL = (3, "Attended school/class")
    SHOPPING_ERRANDS = (4, "Appointment, shopping, or errands (e.g., gas)")
    ESCORT = (5, "Dropped off, picked up, or accompanied another person")
    SOCIAL_LEISURE = (7, "Social, leisure, religious, entertainment activity")
    PRIMARY_WORKPLACE = (10, "Went to primary workplace")
    WORK_ACTIVITY = (
        11,
        "Went to work-related activity (e.g., meeting, delivery, worksite)",
    )
    VOLUNTEERING = (13, "Volunteering")
    OTHER_WORK = (14, "Other work-related")
    K12_SCHOOL = (21, "Attend K-12 school")
    COLLEGE = (22, "Attend college/university")
    OTHER_CLASS = (23, "Attend other type of class (e.g., cooking class)")
    OTHER_EDUCATION = (
        24,
        "Attend other education-related activity (e.g., field trip)",
    )
    VOCATIONAL = (25, "Attend vocational education class")
    DAYCARE = (26, "Attend daycare or preschool")
    GROCERY = (30, "Grocery shopping")
    GAS = (31, "Got gas")
    ROUTINE_SHOPPING = (32, "Other routine shopping (e.g., pharmacy)")
    ERRAND_NO_APPT = (33, "Errand without appointment (e.g., post office)")
    MEDICAL = (34, "Medical visit (e.g., doctor, dentist)")
    MAJOR_SHOPPING = (36, "Shopping for major item (e.g., furniture, car)")
    ERRAND_WITH_APPT = (37, "Errand with appointment (e.g., haircut)")
    OTHER_ACTIVITY = (
        44,
        "Other activity only (e.g., attend meeting, pick-up or drop-off item)",
    )
    PICK_UP = (45, "Pick someone up")
    DROP_OFF = (46, "Drop someone off")
    ACCOMPANY = (47, "Accompany someone only (e.g., go along for the ride)")
    PICK_UP_AND_DROP_OFF = (48, "BOTH pick up AND drop off")
    DINING = (50, "Dined out, got coffee, or take-out")
    EXERCISE = (51, "Exercise or recreation (e.g., gym, jog, bike, walk dog)")
    SOCIAL = (52, "Social activity (e.g., visit friends/relatives)")
    ENTERTAINMENT = (
        53,
        "Leisure/entertainment/cultural (e.g., cinema, museum, park)",
    )
    RELIGIOUS_CIVIC = (54, "Religious/civic/volunteer activity")
    FAMILY_ACTIVITY = (56, "Family activity (e.g., watch child's game)")
    MODE_CHANGE = (
        60,
        "Changed or transferred mode (e.g., waited for bus or exited bus)",
    )
    OTHER_ERRAND = (61, "Other errand")
    OTHER_SOCIAL = (62, "Other social")
    OTHER = (99, "Other reason")
    OTHER_RESIDENCE = (
        150,
        "Went to another residence (e.g., someone else's home, second home)",
    )
    TEMP_LODGING = (
        152,
        "Went to temporary lodging (e.g., hotel, vacation rental)",
    )
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")
    NOT_IMPUTABLE = (996, "Not imputable")


class PurposeCategory(LabeledEnum):
    """d_purpose_category value labels."""

    HOME = (1, "Home")
    WORK = (2, "Work")
    WORK_RELATED = (3, "Work related")
    SCHOOL = (4, "School")
    SCHOOL_RELATED = (5, "School related")
    ESCORT = (6, "Escort")
    SHOP = (7, "Shop")
    MEAL = (8, "Meal")
    SOCIALREC = (9, "Social or recreational")
    ERRAND = (10, "Errand")
    CHANGE_MODE = (11, "Change mode")
    OVERNIGHT = (12, "Overnight")
    OTHER = (13, "Other")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")
    NOT_IMPUTABLE = (996, "Not imputable")


class PurposeToCategoryMap:
    """Mapping from detailed purpose codes to purpose categories."""

    # Need to populate this...


class Driver(LabeledEnum):
    """driver value labels."""

    DRIVER = (1, "Driver")
    PASSENGER = (2, "Passenger")
    BOTH = (3, "Both (switched drivers during trip)")
    MISSING = (995, "Missing Response")


class Mode(LabeledEnum):
    """mode value labels."""

    # NOTE: This is absolute hot chaos... MUST FIX!!!
    # Plan for 2+ level hierarchy: mode groups (x) > detailed modes (xx) > regionally specific (xxx)  # noqa: E501
    # Goal is no orphaned modes so they all map to a group
    # e.g.,:
    # 1. Transit
    #   |-- 11. Local Bus
    #   |-- 12. Express Bus
    #   |-- 13. Light Rail
    #   |     |-- 131. Streetcar/Cable Car
    #   |     |-- 132. MUNI Metro
    #   |     |-- 133. MBTA T
    #   |-- 14. Urban Rail
    #   |      |-- 141. BART
    #   |      |-- 142. NYC Subway
    #   |      |-- 143. DC Metro
    #   |-- 15. Commuter Rail
    #   |      |-- 151. Caltrain
    #   |      |-- 152. MBTA Commuter Rail
    #   |      |-- 153. Caltrain
    #   |      |-- 154. Metro North
    #   |-- 16. Intercity Rail
    #   |      |-- 161. Capitol Corridor
    #   |      |-- 162. Amtrak Northeast Regional
    #   |-- 17. Ferry
    #   |      |-- 171. Richmond Ferry
    #   |      |-- 172. Staten Island Ferry
    #   |-- 18. Other Transit

    WALK = (1, "Walk/jog/wheelchair")
    BIKE = (2, "Standard bicycle (household)")
    BIKE_BORROWED = (3, "Borrowed bicycle")
    BIKE_RENTED = (4, "Other rented bicycle")
    OTHER = (5, "Other")
    HOUSEHOLD_VEHICLE_1 = (6, "Household vehicle 1")
    HOUSEHOLD_VEHICLE_2 = (7, "Household vehicle 2")
    HOUSEHOLD_VEHICLE_3 = (8, "Household vehicle 3")
    HOUSEHOLD_VEHICLE_4 = (9, "Household vehicle 4")
    HOUSEHOLD_VEHICLE_5 = (10, "Household vehicle 5")
    HOUSEHOLD_VEHICLE_6 = (11, "Household vehicle 6")
    HOUSEHOLD_VEHICLE_7 = (12, "Household vehicle 7")
    HOUSEHOLD_VEHICLE_8 = (13, "Household vehicle 8")
    HOUSEHOLD_VEHICLE_9 = (14, "Household vehicle 9")
    HOUSEHOLD_VEHICLE_10 = (15, "Household vehicle 10")
    HOUSEHOLD_VEHICLE_OTHER = (16, "Other vehicle (household)")
    CAR_RENTAL = (17, "Rental car")
    CAR_SHARE = (18, "Carshare (Zipcar, etc.)")
    VANPOOL = (21, "Vanpool")
    OTHER_VEHICLE = (22, "Other vehicle (non-household)")
    BUS_LOCAL = (23, "Local public bus")
    BUS_SCHOOL = (24, "School bus")
    BUS_INTERCITY = (25, "Intercity bus (Greyhound, etc.)")
    BUS_PRIVATE = (26, "Private shuttle/bus")
    BUS_OTHER = (28, "Other bus")
    PARATRANSIT = (27, "Paratransit/Dial-A-Ride")
    BART = (30, "BART")
    AIR = (31, "Airplane/helicopter")
    CAR_WORK = (33, "Work car")
    CAR_FRIEND = (34, "Friend/relative/colleague car")
    TAXI = (36, "Regular taxi")
    BUS_UNIVERSITY = (38, "University/college shuttle")
    RAIL_INTERCITY = (41, "Intercity/commuter rail (ACE, Amtrak, Caltrain)")
    RAIL_OTHER = (42, "Other rail")
    SKATE = (43, "Skateboard/rollerblade")
    GOLF_CART = (44, "Golf cart")
    ATV = (45, "ATV")
    MOTORCYCLE = (47, "Motorcycle (household)")
    TNC = (49, "Rideshare (Uber, Lyft, etc.)")
    MUNI_METRO = (53, "MUNI Metro")
    MOTORCYCLE_OTHER = (54, "Motorcycle (non-household)")
    BUS_EXPRESS = (55, "Express/Transbay bus")
    CAR_RENTAL_P2P = (59, "Peer-to-peer rental (Turo, etc.)")
    TOWNCAR = (60, "Hired car (black car, limo)")
    BUS_BRT = (61, "Rapid transit bus (BRT)")
    BUS_WORK = (62, "Employer shuttle/bus")
    MEDICAL = (63, "Medical transportation")
    BUS_PRIVATE_LOCAL = (67, "Local private bus")
    STREETCAR = (68, "Cable car/streetcar")
    BIKE_SHARE = (69, "Bike-share (standard)")
    BIKE_SHARE_ELECTRIC = (70, "Bike-share (electric)")
    MOPED_SHARE = (73, "Moped-share (Scoot, etc.)")
    SEGWAY = (74, "Segway")
    OTHER_ALT = (75, "Other")
    CARPOOL_SERVICE = (76, "Carpool match (Waze, etc.)")
    MOPED = (77, "Personal scooter/moped")
    FERRY = (78, "Ferry/water taxi")
    BOAT = (80, "Other boat (kayak, etc.)")
    BIKE_ELECTRIC = (82, "Electric bicycle (household)")
    SCOOTER_SHARE = (83, "Scooter-share (Bird, Lime, etc.)")
    HOUSEHOLD_VEHICLE = (100, "Household vehicle/motorcycle")
    CAR_OTHER = (101, "Other vehicle (rental, carshare, etc.)")
    SHUTTLE = (102, "Bus/shuttle/vanpool")
    BICYCLE = (103, "Bicycle")
    OTHER_OTHER = (104, "Other")
    RAIL = (105, "Rail (train, BART, MUNI, etc.)")
    TNC_OTHER = (106, "Uber/Lyft/taxi/car service")
    MICROMOBILITY = (107, "Micromobility (scooter, moped, etc.)")
    MISSING = (995, "Missing Response")


class ModeType(LabeledEnum):
    """mode_type value labels."""

    WALK = (1, "Walk")
    BIKE = (2, "Bike")
    BIKESHARE = (3, "Bikeshare")
    SCOOTERSHARE = (4, "Scootershare")
    TAXI = (5, "Taxi")
    TNC = (6, "TNC")
    OTHER = (7, "Other")
    CAR = (8, "Car")
    CARSHARE = (9, "Carshare")
    SCHOOL_BUS = (10, "School bus")
    SHUTTLE = (11, "Shuttle/vanpool")
    FERRY = (12, "Ferry")
    TRANSIT = (13, "Transit")
    LONG_DISTANCE = (14, "Long distance passenger")
    MISSING = (995, "Missing Response")

    @classmethod
    def from_mode(cls) -> dict["Mode", "ModeType"]:
        """Get mapping from detailed Mode to ModeType.

        Returns:
            Dictionary mapping Mode enum values to ModeType enum values
        """
        return {
            # Walk
            Mode.WALK: cls.WALK,
            # Bike
            Mode.BIKE: cls.BIKE,
            Mode.BIKE_BORROWED: cls.BIKE,
            Mode.BIKE_RENTED: cls.BIKE,
            Mode.BIKE_ELECTRIC: cls.BIKE,
            Mode.BICYCLE: cls.BIKE,
            # Bikeshare
            Mode.BIKE_SHARE: cls.BIKESHARE,
            Mode.BIKE_SHARE_ELECTRIC: cls.BIKESHARE,
            # Scootershare
            Mode.SCOOTER_SHARE: cls.SCOOTERSHARE,
            Mode.MOPED_SHARE: cls.SCOOTERSHARE,
            Mode.MICROMOBILITY: cls.SCOOTERSHARE,
            Mode.SKATE: cls.SCOOTERSHARE,
            Mode.SEGWAY: cls.SCOOTERSHARE,
            Mode.MOPED: cls.SCOOTERSHARE,
            # Taxi
            Mode.TAXI: cls.TAXI,
            Mode.TOWNCAR: cls.TAXI,
            # TNC
            Mode.TNC: cls.TNC,
            Mode.TNC_OTHER: cls.TNC,
            # Car
            Mode.HOUSEHOLD_VEHICLE_1: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_2: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_3: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_4: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_5: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_6: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_7: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_8: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_9: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_10: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE_OTHER: cls.CAR,
            Mode.HOUSEHOLD_VEHICLE: cls.CAR,
            Mode.CAR_WORK: cls.CAR,
            Mode.CAR_FRIEND: cls.CAR,
            Mode.OTHER_VEHICLE: cls.CAR,
            Mode.CAR_OTHER: cls.CAR,
            Mode.MOTORCYCLE: cls.CAR,
            Mode.MOTORCYCLE_OTHER: cls.CAR,
            Mode.GOLF_CART: cls.CAR,
            Mode.ATV: cls.CAR,
            # Carshare
            Mode.CAR_RENTAL: cls.CARSHARE,
            Mode.CAR_SHARE: cls.CARSHARE,
            Mode.CAR_RENTAL_P2P: cls.CARSHARE,
            Mode.CARPOOL_SERVICE: cls.CARSHARE,
            # School bus
            Mode.BUS_SCHOOL: cls.SCHOOL_BUS,
            # Shuttle/vanpool
            Mode.VANPOOL: cls.SHUTTLE,
            Mode.BUS_PRIVATE: cls.SHUTTLE,
            Mode.BUS_UNIVERSITY: cls.SHUTTLE,
            Mode.BUS_WORK: cls.SHUTTLE,
            Mode.BUS_PRIVATE_LOCAL: cls.SHUTTLE,
            Mode.SHUTTLE: cls.SHUTTLE,
            Mode.PARATRANSIT: cls.SHUTTLE,
            Mode.MEDICAL: cls.SHUTTLE,
            # Ferry
            Mode.FERRY: cls.FERRY,
            Mode.BOAT: cls.FERRY,
            # Transit
            Mode.BUS_LOCAL: cls.TRANSIT,
            Mode.BUS_EXPRESS: cls.TRANSIT,
            Mode.BUS_BRT: cls.TRANSIT,
            Mode.BUS_OTHER: cls.TRANSIT,
            Mode.BART: cls.TRANSIT,
            Mode.MUNI_METRO: cls.TRANSIT,
            Mode.STREETCAR: cls.TRANSIT,
            Mode.RAIL_OTHER: cls.TRANSIT,
            Mode.RAIL: cls.TRANSIT,
            # Long distance
            Mode.BUS_INTERCITY: cls.LONG_DISTANCE,
            Mode.RAIL_INTERCITY: cls.LONG_DISTANCE,
            Mode.AIR: cls.LONG_DISTANCE,
            # Other
            Mode.OTHER: cls.OTHER,
            Mode.OTHER_ALT: cls.OTHER,
            Mode.OTHER_OTHER: cls.OTHER,
            Mode.MISSING: cls.MISSING,
        }


class AccessEgressMode(LabeledEnum):
    """transit_access value labels."""

    # NOTE: Why is this not just inherited from Mode???

    WALK = (1, "Walked (or jogged/wheelchair)")
    BICYCLE = (2, "Bicycle")
    TRANSFER_BUS = (3, "Transferred from another bus")
    MICROMOBILITY = (4, "Micromobility (e.g., scooter, moped, skateboard)")
    TRANSFER_OTHER = (5, "Transferred from other transit (e.g., rail, air)")
    TNC = (6, "Uber/Lyft, taxi, or car service")
    CAR_HOUSEHOLD = (
        7,
        "Drove and parked my own household's vehicle (or motorcycle)",
    )
    CAR_OTHER = (8, "Drove and parked another vehicle (or motorcycle)")
    DROPOFF_HOUSEHOLD = (
        9,
        "Got dropped off in my own household's vehicle (or motorcycle)",
    )
    DROPOFF_OTHER = (10, "Got dropped off in another vehicle (or motorcycle)")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other")
