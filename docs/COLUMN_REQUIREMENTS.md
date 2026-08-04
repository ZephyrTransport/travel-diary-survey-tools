---
body_class: col-matrix
hide:
  - toc
---

# Column Requirement Matrix

Generated automatically by `scripts/generate_column_matrix.py`.

***Do not edit this markdown file directly.***

Each tab shows the fields for one canonical table. Only steps that reference the table are shown.

| Symbol | Meaning |
| :----: | ------- |
| ✓ | Required as input |
| + | Created / produced |

=== "households"

    | Field | Type | Constraints | extract_tours | add_zone_ids | format_daysim |
    | --- | --- | --- | --- | --- | --- |
    | `hh_id` | int | ≥ 1, UNIQUE | ✓ | ✓ |  |
    | `home_lat` | float | ≥ -90, ≤ 90 | ✓ | ✓ |  |
    | `home_lon` | float | ≥ -180, ≤ 180 | ✓ | ✓ |  |
    | `residence_rent_own` | ResidenceRentOwn |  |  |  | ✓ |
    | `residence_type` | ResidenceType |  |  |  | ✓ |
    | `income` | int or None | ≥ 0 |  |  |  |
    | `income_bin` | IncomeBroad |  |  |  |  |
    | `hh_weight` | float or None | ≥ 0 |  |  |  |
    | `num_vehicles` | int | ≥ 0 |  |  |  |
    | `complete` | bool |  |  |  |  |
    | `model_usable` | bool or None |  |  |  |  |

=== "persons"

    | Field | Type | Constraints | extract_tours | add_zone_ids | format_ctramp | format_daysim |
    | --- | --- | --- | --- | --- | --- | --- |
    | `person_id` | int | ≥ 1, UNIQUE | ✓ | ✓ |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id`, REQ_CHILD |  |  |  |  |
    | `person_num` | int | ≥ 1 |  |  | ✓ | ✓ |
    | `age` | AgeCategory |  | ✓ |  |  |  |
    | `gender` | Gender |  |  |  | ✓ |  |
    | `work_lat` | float or None | ≥ -90, ≤ 90 | ✓ | ✓ |  |  |
    | `work_lon` | float or None | ≥ -180, ≤ 180 | ✓ | ✓ |  |  |
    | `school_lat` | float or None | ≥ -90, ≤ 90 | ✓ | ✓ |  |  |
    | `school_lon` | float or None | ≥ -180, ≤ 180 | ✓ | ✓ |  |  |
    | `industry` | Industry or None |  |  |  |  |  |
    | `occupation` | Occupation or None |  |  |  |  |  |
    | `job_type` | JobType or None |  |  |  | ✓ |  |
    | `employment` | Employment |  | ✓ |  |  |  |
    | `student` | Student |  | ✓ |  |  |  |
    | `school_type` | SchoolType or None |  | ✓ |  |  |  |
    | `work_park` | WorkParking or None |  |  |  |  | ✓ |
    | `work_mode` | Mode or None |  |  |  |  | ✓ |
    | `race` | Race or None |  |  |  |  |  |
    | `ethnicity` | Ethnicity or None |  |  |  |  |  |
    | `telework_freq` | CommuteFreq or None |  |  |  |  |  |
    | `commute_freq` | CommuteFreq or None |  |  |  |  |  |
    | `commute_subsidy_provide_free_parking` | BooleanYesNo or None |  |  |  | ✓ |  |
    | `commute_subsidy_provide_discounted_parking` | BooleanYesNo or None |  |  |  | ✓ |  |
    | `commute_subsidy_use_free_parking` | BooleanYesNo or None |  |  |  | ✓ |  |
    | `commute_subsidy_use_discounted_parking` | BooleanYesNo or None |  |  |  | ✓ |  |
    | `is_proxy` | bool or None |  |  |  |  | ✓ |
    | `surveyable` | bool or None |  |  |  |  |  |
    | `num_days_complete` | int | ≥ 0 |  |  |  |  |
    | `complete` | bool or None |  |  |  |  |  |
    | `model_usable` | bool or None |  |  |  |  |  |
    | `person_weight` | float or None | ≥ 0 |  |  |  |  |

=== "days"

    | Field | Type | Constraints | cascade_completeness | format_ctramp | format_daysim |
    | --- | --- | --- | --- | --- | --- |
    | `person_id` | int | ≥ 1, FK → `persons.person_id`, REQ_CHILD |  | ✓ |  |
    | `day_id` | int | ≥ 1, UNIQUE | ✓ | ✓ |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` | ✓ | ✓ |  |
    | `travel_date` | datetime |  | ✓ |  |  |
    | `travel_dow` | TravelDow |  |  |  | ✓ |
    | `complete` | bool or None |  | ✓ |  |  |
    | `hh_day_complete` | bool or None |  |  |  |  |
    | `hh_day_usable` | bool or None |  |  |  |  |
    | `model_usable` | bool or None |  |  |  |  |
    | `day_weight` | float or None | ≥ 0 |  |  |  |

=== "unlinked_trips"

    | Field | Type | Constraints | link_trips | extract_tours | add_zone_ids | format_daysim |
    | --- | --- | --- | --- | --- | --- | --- |
    | `unlinked_trip_id` | int | ≥ 1, UNIQUE |  |  | ✓ |  |
    | `day_id` | int | ≥ 1, FK → `days.day_id` | ✓ | ✓ |  |  |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` |  |  |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` |  |  |  |  |
    | `linked_trip_id` | int | ≥ 1, FK → `linked_trips.linked_trip_id` |  | ✓ |  |  |
    | `tour_id` | int or None | ≥ 1, FK → `tours.tour_id` |  |  |  | ✓ |
    | `o_lon` | float | ≥ -180, ≤ 180 | ✓ |  | ✓ |  |
    | `o_lat` | float | ≥ -90, ≤ 90 | ✓ |  | ✓ |  |
    | `d_lon` | float | ≥ -180, ≤ 180 | ✓ |  | ✓ |  |
    | `d_lat` | float | ≥ -90, ≤ 90 | ✓ |  | ✓ |  |
    | `o_purpose` | Purpose |  |  |  |  |  |
    | `d_purpose` | Purpose |  |  |  |  |  |
    | `o_purpose_category` | PurposeCategory |  | ✓ |  |  |  |
    | `d_purpose_category` | PurposeCategory |  | ✓ |  |  |  |
    | `mode_type` | ModeType |  | ✓ |  |  |  |
    | `mode_1` | Mode or None |  |  |  |  |  |
    | `mode_2` | Mode or None |  |  |  |  |  |
    | `mode_3` | Mode or None |  |  |  |  |  |
    | `mode_4` | Mode or None |  |  |  |  |  |
    | `tnc_type` | TNCType or None |  |  |  |  |  |
    | `duration_minutes` | float | ≥ 0 |  |  |  |  |
    | `distance_meters` | float | ≥ 0 |  |  |  |  |
    | `depart_time` | datetime or None |  | ✓ | ✓ |  |  |
    | `arrive_time` | datetime or None |  | ✓ | ✓ |  |  |
    | `num_travelers` | int | ≥ 1 |  |  |  |  |
    | `complete` | bool or None |  |  |  |  |  |
    | `model_usable` | bool or None |  |  |  |  |  |
    | `unlinked_trip_weight` | float or None | ≥ 0 |  |  |  |  |

=== "linked_trips"

    | Field | Type | Constraints | detect_joint_trips | extract_tours | add_zone_ids | format_ctramp | format_daysim |
    | --- | --- | --- | --- | --- | --- | --- | --- |
    | `day_id` | int | ≥ 1, FK → `days.day_id` |  | ✓ |  |  |  |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` |  |  |  |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` |  |  |  |  |  |
    | `linked_trip_id` | int | ≥ 1, UNIQUE |  |  | ✓ |  |  |
    | `joint_trip_id` | int or None | ≥ 1, FK → `joint_trips.joint_trip_id` |  | ✓ |  |  |  |
    | `tour_id` | int | ≥ 1, FK → `tours.tour_id` |  |  |  |  | ✓ |
    | `travel_dow` | TravelDow |  |  |  |  |  |  |
    | `o_purpose` | Purpose |  |  | ✓ |  | ✓ |  |
    | `o_purpose_category` | PurposeCategory |  |  | ✓ |  |  |  |
    | `o_lat` | float | ≥ -90, ≤ 90 | ✓ |  | ✓ |  |  |
    | `o_lon` | float | ≥ -180, ≤ 180 | ✓ |  | ✓ |  |  |
    | `d_purpose` | Purpose |  |  | ✓ |  | ✓ |  |
    | `d_purpose_category` | PurposeCategory |  |  | ✓ |  |  |  |
    | `d_lat` | float | ≥ -90, ≤ 90 | ✓ |  | ✓ |  |  |
    | `d_lon` | float | ≥ -180, ≤ 180 | ✓ |  | ✓ |  |  |
    | `mode_type` | ModeType |  |  | ✓ |  |  |  |
    | `driver` | Driver |  |  |  |  |  | ✓ |
    | `num_travelers` | int | ≥ 1 |  |  |  |  |  |
    | `access_mode` | AccessEgressMode or None |  |  |  |  | ✓ | ✓ |
    | `egress_mode` | AccessEgressMode or None |  |  |  |  | ✓ | ✓ |
    | `duration_minutes` | float | ≥ 0 |  |  |  |  |  |
    | `distance_meters` | float | ≥ 0 |  |  |  |  |  |
    | `depart_time` | datetime |  | ✓ |  |  |  |  |
    | `arrive_time` | datetime |  | ✓ |  |  |  |  |
    | `d_activity_duration` | int |  |  | ✓ |  |  |  |
    | `tour_direction` | TourDirection |  |  |  |  |  | ✓ |
    | `o_location_type` | LocationType |  |  |  |  |  |  |
    | `d_location_type` | LocationType |  |  |  |  |  |  |
    | `complete` | bool or None |  |  |  |  |  |  |
    | `model_usable` | bool or None |  |  |  |  |  |  |
    | `linked_trip_weight` | float or None | ≥ 0 |  |  |  |  |  |

=== "tours"

    | Field | Type | Constraints | cascade_completeness | add_zone_ids | format_ctramp | format_daysim |
    | --- | --- | --- | --- | --- | --- | --- |
    | `tour_id` | int | ≥ 1, UNIQUE | ✓ | ✓ |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` |  |  |  |  |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` |  |  |  |  |
    | `day_id` | int | ≥ 1, FK → `days.day_id` | ✓ |  |  |  |
    | `tour_num` | int | ≥ 1 |  |  |  |  |
    | `subtour_num` | int | ≥ 0 |  |  |  |  |
    | `parent_tour_id` | int | ≥ 1, FK → `tours.tour_id` |  |  |  |  |
    | `joint_tour_id` | int or None | ≥ 1 |  |  |  |  |
    | `tour_purpose` | PurposeCategory or None |  |  |  |  |  |
    | `tour_type` | TourType |  |  |  |  |  |
    | `tour_category` | TourCategory |  |  |  |  |  |
    | `single_trip_tour` | bool |  |  |  |  |  |
    | `origin_depart_time` | datetime |  |  |  |  |  |
    | `origin_arrive_time` | datetime |  |  |  |  |  |
    | `dest_arrive_time` | datetime or None |  |  |  |  |  |
    | `dest_depart_time` | datetime or None |  |  |  |  |  |
    | `origin_linked_trip_id` | int | ≥ 1, FK → `linked_trips.linked_trip_id` |  |  |  | ✓ |
    | `dest_linked_trip_id` | int or None | ≥ 1, FK → `linked_trips.linked_trip_id` |  |  |  | ✓ |
    | `o_lat` | float | ≥ -90, ≤ 90 |  | ✓ |  |  |
    | `o_lon` | float | ≥ -180, ≤ 180 |  | ✓ |  |  |
    | `d_lat` | float | ≥ -90, ≤ 90 |  | ✓ |  |  |
    | `d_lon` | float | ≥ -180, ≤ 180 |  | ✓ |  |  |
    | `o_location_type` | LocationType |  |  |  |  |  |
    | `d_location_type` | LocationType |  |  |  |  |  |
    | `tour_mode` | ModeType |  |  |  |  |  |
    | `outbound_mode` | ModeType or None |  |  |  |  |  |
    | `inbound_mode` | ModeType or None |  |  |  |  |  |
    | `num_travelers` | int | ≥ 1 |  |  | ✓ |  |
    | `complete` | bool or None |  | ✓ |  |  |  |
    | `model_usable` | bool or None |  |  |  |  |  |
    | `tour_weight` | float or None | ≥ 0 |  |  |  |  |
