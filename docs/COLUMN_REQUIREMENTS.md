---
body_class: col-matrix
hide:
  - toc
---

# Column Requirement Matrix

Generated automatically by `scripts/generate_column_matrix.py`.

***Do not edit this markdown file directly.***

Each tab shows the fields for one canonical table. Only steps that reference the table are shown. Rows such as `usable_<profile>` are columns the config names, one per profile or zone.

| Symbol | Meaning |
| :----: | ------- |
| ✓ | Required as input |
| + | Created / produced |
| ✓+ | Required, then updated |

=== "households"

    | Field | Type | Constraints | imputation | detect_joint_trips | add_zone_ids | cascade_usability | add_existing_weights | compute_weights | format_ctramp | format_daysim | final_check |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | `hh_id` | int | ≥ 1, UNIQUE |  |  | ✓ | ✓ | ✓ | ✓ | ✓ |  |  |
    | `home_lat` | float | ≥ -90, ≤ 90 |  |  | ✓ |  |  | ✓ | ✓ |  |  |
    | `home_lon` | float | ≥ -180, ≤ 180 |  |  | ✓ |  |  | ✓ | ✓ |  |  |
    | `residence_rent_own` | ResidenceRentOwn |  |  |  |  |  |  |  |  | ✓ |  |
    | `residence_type` | ResidenceType |  |  |  |  |  |  |  |  | ✓ |  |
    | `income` | int or None | ≥ 0 |  |  |  |  |  |  |  |  |  |
    | `income_bin` | IncomeBroad |  |  |  |  |  |  | ✓ | ✓ |  |  |
    | `num_vehicles` | int | ≥ 0 |  |  |  |  |  | ✓ | ✓ |  |  |
    | `exclusion_reason` | str or None |  |  |  |  |  |  |  |  |  |  |
    | `survey_complete` | bool |  |  |  |  | + |  | ✓ |  |  |  |
    | `<column>_preimputed` | named by config |  | + |  |  |  |  |  |  |  |  |
    | `hh_weight_<profile>` | named by config |  |  |  |  |  | + | + |  |  |  |
    | `home_<zone>` | named by config |  |  |  | + |  |  |  |  |  |  |
    | `usable_<profile>` | named by config |  |  |  |  | + | ✓ | ✓ |  |  |  |

=== "persons"

    | Field | Type | Constraints | imputation | extract_tours | add_zone_ids | cascade_usability | add_existing_weights | compute_weights | format_ctramp | format_daysim | final_check |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | `person_id` | int | ≥ 1, UNIQUE |  | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id`, REQ_CHILD |  |  |  | ✓ | ✓ | ✓ | ✓ |  |  |
    | `person_num` | int | ≥ 1 |  |  |  |  |  |  | ✓ | ✓ |  |
    | `age` | AgeCategory |  |  | ✓ |  |  |  | ✓ | ✓ |  |  |
    | `gender` | Gender |  |  |  |  |  |  | ✓ | ✓ |  |  |
    | `work_lat` | float or None | ≥ -90, ≤ 90 |  |  | ✓ |  |  |  | ✓ |  |  |
    | `work_lon` | float or None | ≥ -180, ≤ 180 |  |  | ✓ |  |  |  | ✓ |  |  |
    | `school_lat` | float or None | ≥ -90, ≤ 90 |  |  | ✓ |  |  |  | ✓ |  |  |
    | `school_lon` | float or None | ≥ -180, ≤ 180 |  |  | ✓ |  |  |  | ✓ |  |  |
    | `industry` | Industry or None |  |  |  |  |  |  |  |  |  |  |
    | `occupation` | Occupation or None |  |  |  |  |  |  |  |  |  |  |
    | `job_type` | JobType or None |  |  |  |  |  |  | ✓ |  |  |  |
    | `employment` | Employment |  |  | ✓ |  |  |  | ✓ | ✓ |  |  |
    | `student` | Student |  |  | ✓ |  |  |  | ✓ | ✓ |  |  |
    | `school_type` | SchoolType or None |  |  |  |  |  |  | ✓ | ✓ |  |  |
    | `work_park` | WorkParking or None |  |  |  |  |  |  |  |  | ✓ |  |
    | `work_mode` | Mode or None |  |  |  |  |  |  | ✓ |  | ✓ |  |
    | `race` | Race or None |  |  |  |  |  |  | ✓ |  |  |  |
    | `ethnicity` | Ethnicity or None |  |  |  |  |  |  | ✓ |  |  |  |
    | `telework_freq` | CommuteFreq or None |  |  |  |  |  |  | ✓ |  |  |  |
    | `commute_freq` | CommuteFreq or None |  |  |  |  |  |  | ✓ |  |  |  |
    | `education` | Education or None |  |  |  |  |  |  | ✓ |  |  |  |
    | `commute_subsidy_provide_free_parking` | BooleanYesNo or None |  |  |  |  |  |  |  | ✓ |  |  |
    | `commute_subsidy_provide_discounted_parking` | BooleanYesNo or None |  |  |  |  |  |  |  | ✓ |  |  |
    | `commute_subsidy_use_free_parking` | BooleanYesNo or None |  |  |  |  |  |  |  | ✓ |  |  |
    | `commute_subsidy_use_discounted_parking` | BooleanYesNo or None |  |  |  |  |  |  |  | ✓ |  |  |
    | `is_proxy` | bool or None |  |  |  |  |  |  |  |  | ✓ |  |
    | `surveyable` | bool or None |  |  |  |  |  |  |  |  |  |  |
    | `survey_tech` | SurveyTech |  |  |  |  |  |  |  |  |  |  |
    | `exclusion_reason` | str or None |  |  |  |  |  |  |  |  |  |  |
    | `num_days_complete` | int | ≥ 0 |  |  |  |  |  |  |  |  |  |
    | `survey_complete` | bool or None |  |  |  |  | + |  |  |  |  |  |
    | `<column>_preimputed` | named by config |  | + |  |  |  |  |  |  |  |  |
    | `person_weight_<profile>` | named by config |  |  |  |  |  | + | + |  |  |  |
    | `school_<zone>` | named by config |  |  |  | + |  |  |  |  |  |  |
    | `usable_<profile>` | named by config |  |  |  |  | + |  |  |  |  |  |
    | `work_<zone>` | named by config |  |  |  | + |  |  |  |  |  |  |

=== "household_days"

    | Field | Type | Constraints | cascade_usability |
    | --- | --- | --- | --- |
    | `hh_day_id` | int | ≥ 1, UNIQUE | ✓ |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` | ✓ |
    | `travel_date` | datetime |  |  |
    | `survey_complete` | bool or None |  | + |
    | `usable_<profile>` | named by config |  | + |

=== "person_days"

    | Field | Type | Constraints | imputation | detect_habitual_locations | detect_joint_trips | extract_tours | cascade_usability | add_existing_weights | compute_weights | format_ctramp | format_daysim | final_check |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | `person_id` | int | ≥ 1, FK → `persons.person_id`, REQ_CHILD |  |  |  |  | ✓ | ✓ | ✓ | ✓ |  |  |
    | `person_day_id` | int | ≥ 1, UNIQUE |  | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` |  |  |  |  | ✓ |  |  | ✓ |  |  |
    | `hh_day_id` | int | ≥ 1, FK → `household_days.hh_day_id` |  |  | ✓ | ✓ | ✓ |  |  | ✓ |  |  |
    | `travel_date` | datetime |  |  |  | ✓ | ✓ |  |  |  |  |  |  |
    | `travel_dow` | TravelDow |  |  |  |  |  | ✓ |  |  |  | ✓ |  |
    | `begin_day` | BeginEndDay or None |  |  | ✓ |  | ✓ |  |  |  |  |  |  |
    | `end_day` | BeginEndDay or None |  |  | ✓ |  | ✓ |  |  |  |  |  |  |
    | `exclusion_reason` | str or None |  |  |  |  |  |  |  |  |  |  |  |
    | `survey_complete` | bool or None |  |  |  |  |  | ✓ |  |  |  |  |  |
    | `<column>_preimputed` | named by config |  | + |  |  |  |  |  |  |  |  |  |
    | `person_day_weight_<profile>` | named by config |  |  |  |  |  |  | + | + |  |  |  |
    | `usable_<profile>` | named by config |  |  |  |  |  | + |  |  |  |  |  |

=== "unlinked_trips"

    | Field | Type | Constraints | imputation | link_trips | extract_tours | add_zone_ids | cascade_usability | add_existing_weights | compute_weights | format_ctramp | format_daysim | final_check |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | `unlinked_trip_id` | int | ≥ 1, UNIQUE |  |  |  | ✓ |  | ✓ | ✓ |  |  |  |
    | `person_day_id` | int | ≥ 1, FK → `person_days.person_day_id` |  | ✓ | ✓ |  | ✓ | ✓ | ✓ |  |  |  |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` |  |  |  |  |  |  |  |  |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` |  |  |  |  |  |  |  |  |  |  |
    | `linked_trip_id` | int | ≥ 1, FK → `linked_trips.linked_trip_id` |  | + | ✓ |  |  | ✓ | ✓ |  |  |  |
    | `linked_trip_num` | int or None | ≥ 1 |  |  |  |  |  |  |  |  |  |  |
    | `tour_id` | int or None | ≥ 1, FK → `tours.tour_id` |  |  | + |  | ✓ |  |  |  | ✓ |  |
    | `joint_tour_id` | int or None | ≥ 1, FK → `joint_tours.joint_tour_id` |  |  | + |  |  |  |  |  |  |  |
    | `o_lon` | float | ≥ -180, ≤ 180 |  | ✓ |  | ✓ |  |  |  |  |  |  |
    | `o_lat` | float | ≥ -90, ≤ 90 |  | ✓ |  | ✓ |  |  |  |  |  |  |
    | `d_lon` | float | ≥ -180, ≤ 180 |  | ✓ |  | ✓ |  |  |  |  |  |  |
    | `d_lat` | float | ≥ -90, ≤ 90 |  | ✓ |  | ✓ |  |  |  |  |  |  |
    | `o_purpose` | Purpose |  |  |  |  |  |  |  |  |  |  |  |
    | `d_purpose` | Purpose |  |  |  |  |  |  |  |  |  |  |  |
    | `o_purpose_category` | PurposeCategory |  |  | ✓ |  |  |  |  |  |  |  |  |
    | `d_purpose_category` | PurposeCategory |  |  | ✓ |  |  |  |  |  |  |  |  |
    | `mode_type` | ModeType |  |  | ✓ |  |  |  |  |  |  |  |  |
    | `mode_1` | Mode or None |  |  |  |  |  |  |  |  |  |  |  |
    | `mode_2` | Mode or None |  |  |  |  |  |  |  |  |  |  |  |
    | `mode_3` | Mode or None |  |  |  |  |  |  |  |  |  |  |  |
    | `mode_4` | Mode or None |  |  |  |  |  |  |  |  |  |  |  |
    | `tnc_type` | TNCType or None |  |  |  |  |  |  |  |  |  |  |  |
    | `duration_minutes` | float | ≥ 0 |  |  |  |  |  |  |  |  |  |  |
    | `distance_meters` | float | ≥ 0 |  |  |  |  |  |  |  |  |  |  |
    | `depart_time` | datetime or None |  |  | ✓ | ✓ |  |  |  |  |  |  |  |
    | `arrive_time` | datetime or None |  |  | ✓ | ✓ |  |  |  |  |  |  |  |
    | `num_travelers` | int | ≥ 1 |  |  |  |  |  |  |  |  |  |  |
    | `survey_complete` | bool or None |  |  |  |  |  | ✓+ |  |  |  |  |  |
    | `<column>_preimputed` | named by config |  | + |  |  |  |  |  |  |  |  |  |
    | `d_<zone>` | named by config |  |  |  |  | + |  |  |  |  |  |  |
    | `joint_tour_id_<profile>` | named by config |  |  |  |  |  | + |  |  |  |  |  |
    | `o_<zone>` | named by config |  |  |  |  | + |  |  |  |  |  |  |
    | `unlinked_trip_weight_<profile>` | named by config |  |  |  |  |  |  | + | + |  |  |  |
    | `usable_<profile>` | named by config |  |  |  |  |  | + |  |  |  |  |  |

=== "linked_trips"

    | Field | Type | Constraints | imputation | link_trips | detect_habitual_locations | detect_joint_trips | extract_tours | add_zone_ids | cascade_usability | add_existing_weights | compute_weights | format_ctramp | format_daysim | final_check |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | `person_day_id` | int | ≥ 1, FK → `person_days.person_day_id` |  | + | ✓ | ✓ | ✓ |  | ✓ |  |  |  |  |  |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` |  | + | ✓ |  |  |  |  |  |  |  |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` |  | + |  |  |  |  |  |  |  |  |  |  |
    | `linked_trip_id` | int | ≥ 1, UNIQUE |  | + | ✓ |  |  | ✓ |  | ✓ | ✓ | ✓ |  |  |
    | `joint_trip_id` | int or None | ≥ 1, FK → `joint_trips.joint_trip_id` |  |  |  | + | ✓ |  | ✓ |  | ✓ |  |  |  |
    | `tour_id` | int | ≥ 1, FK → `tours.tour_id` |  |  |  |  | + |  | ✓ | ✓ | ✓ | ✓ | ✓ |  |
    | `travel_dow` | TravelDow |  |  |  |  |  |  |  |  |  |  |  |  |  |
    | `o_purpose` | Purpose |  |  |  | ✓ |  | ✓ |  |  |  |  |  |  |  |
    | `o_purpose_category` | PurposeCategory |  |  |  | ✓ |  | ✓ |  |  |  |  | ✓ |  |  |
    | `o_lat` | float | ≥ -90, ≤ 90 |  |  | ✓ | ✓ |  | ✓ |  |  |  | ✓ |  |  |
    | `o_lon` | float | ≥ -180, ≤ 180 |  |  | ✓ | ✓ |  | ✓ |  |  |  | ✓ |  |  |
    | `d_purpose` | Purpose |  |  |  | ✓ |  | ✓ |  |  |  |  |  |  |  |
    | `d_purpose_category` | PurposeCategory |  |  |  | ✓ |  | ✓ |  |  |  |  | ✓ |  |  |
    | `d_lat` | float | ≥ -90, ≤ 90 |  |  | ✓ | ✓ |  | ✓ |  |  |  | ✓ |  |  |
    | `d_lon` | float | ≥ -180, ≤ 180 |  |  | ✓ | ✓ |  | ✓ |  |  |  | ✓ |  |  |
    | `mode_type` | ModeType |  |  |  |  |  | ✓ |  |  |  |  | ✓ |  |  |
    | `driver` | Driver |  |  | + |  |  |  |  |  |  |  |  | ✓ |  |
    | `num_travelers` | int | ≥ 1 |  |  |  |  |  |  |  |  |  | ✓ |  |  |
    | `access_mode` | AccessEgressMode or None |  |  |  |  |  |  |  |  |  |  | ✓ | ✓ |  |
    | `egress_mode` | AccessEgressMode or None |  |  |  |  |  |  |  |  |  |  | ✓ | ✓ |  |
    | `duration_minutes` | float | ≥ 0 |  |  |  |  |  |  |  |  |  |  |  |  |
    | `distance_meters` | float | ≥ 0 |  |  |  |  |  |  |  |  |  | ✓ |  |  |
    | `depart_time` | datetime |  |  |  | ✓ | ✓ |  |  |  |  |  | ✓ |  |  |
    | `arrive_time` | datetime |  |  |  | ✓ | ✓ |  |  |  |  |  | ✓ |  |  |
    | `d_activity_duration` | int or None |  |  |  | ✓ |  | ✓ |  |  |  |  |  |  |  |
    | `tour_direction` | TourDirection |  |  |  |  |  |  |  |  |  |  | ✓ | ✓ |  |
    | `tour_num` | int or None | ≥ 1 |  |  |  |  |  |  |  |  |  |  |  |  |
    | `subtour_num` | int or None | ≥ 0 |  |  |  |  |  |  |  |  |  |  |  |  |
    | `parent_tour_id` | int or None | ≥ 1, FK → `tours.tour_id` |  |  |  |  |  |  |  |  |  |  |  |  |
    | `tour_purpose` | PurposeCategory or None |  |  |  |  |  |  |  |  |  |  |  |  |  |
    | `joint_tour_id` | int or None | ≥ 1, FK → `joint_tours.joint_tour_id` |  |  |  |  | + |  |  |  |  |  |  |  |
    | `linked_trip_num` | int or None | ≥ 1 |  |  |  |  |  |  |  |  |  |  |  |  |
    | `num_segments` | int or None | ≥ 1 |  |  |  |  |  |  |  |  |  |  |  |  |
    | `travel_duration_minutes` | int or None | ≥ 0 |  |  |  |  |  |  |  |  |  |  |  |  |
    | `dwell_duration_minutes` | int or None | ≥ 0 |  |  |  |  |  |  |  |  |  |  |  |  |
    | `o_location_type` | LocationType |  |  |  |  |  |  |  |  |  |  |  |  |  |
    | `d_location_type` | LocationType |  |  |  |  |  |  |  |  |  |  |  |  |  |
    | `survey_complete` | bool or None |  |  |  |  |  |  |  | ✓+ |  |  |  |  |  |
    | `<column>_preimputed` | named by config |  | + |  |  |  |  |  |  |  |  |  |  |  |
    | `d_<zone>` | named by config |  |  |  |  |  |  | + |  |  |  |  |  |  |
    | `joint_tour_id_<profile>` | named by config |  |  |  |  |  |  |  | + |  |  |  |  |  |
    | `joint_trip_id_<profile>` | named by config |  |  |  |  |  |  |  | + |  |  |  |  |  |
    | `linked_trip_weight_<profile>` | named by config |  |  |  |  |  |  |  |  | + | + |  |  |  |
    | `o_<zone>` | named by config |  |  |  |  |  |  | + |  |  |  |  |  |  |
    | `usable_<profile>` | named by config |  |  |  |  |  |  |  | + |  |  |  |  |  |

=== "tours"

    | Field | Type | Constraints | imputation | extract_tours | add_zone_ids | cascade_usability | add_existing_weights | compute_weights | format_ctramp | format_daysim | final_check |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | `tour_id` | int | ≥ 1, UNIQUE |  | + | ✓ | ✓ | ✓ | ✓ | ✓ |  |  |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` |  |  |  |  |  |  |  |  |  |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` |  |  |  |  |  |  |  |  |  |
    | `person_day_id` | int | ≥ 1, FK → `person_days.person_day_id` |  |  |  | ✓ |  |  |  |  |  |
    | `tour_num` | int | ≥ 1 |  |  |  |  |  |  |  |  |  |
    | `subtour_num` | int | ≥ 0 |  |  |  |  |  |  | ✓ |  |  |
    | `parent_tour_id` | int | ≥ 1, FK → `tours.tour_id` |  |  |  | ✓ |  |  | ✓ |  |  |
    | `joint_tour_id` | int or None | ≥ 1, FK → `joint_tours.joint_tour_id` |  | + |  | ✓ |  | ✓ |  |  |  |
    | `tour_purpose` | PurposeCategory or None |  |  |  |  |  |  |  | ✓ |  |  |
    | `tour_type` | TourType |  |  |  |  |  |  |  |  |  |  |
    | `tour_category` | TourCategory |  |  |  |  | ✓ |  |  | ✓ |  |  |
    | `tour_data_quality` | TourDataQuality |  |  |  |  | ✓ |  |  |  |  |  |
    | `trip_count` | int | ≥ 1 |  |  |  |  |  |  |  |  |  |
    | `origin_depart_time` | datetime |  |  |  |  |  |  |  | ✓ |  |  |
    | `origin_arrive_time` | datetime |  |  |  |  |  |  |  | ✓ |  |  |
    | `dest_arrive_time` | datetime or None |  |  |  |  |  |  |  |  |  |  |
    | `dest_depart_time` | datetime or None |  |  |  |  |  |  |  |  |  |  |
    | `origin_linked_trip_id` | int | ≥ 1, FK → `linked_trips.linked_trip_id` |  |  |  |  |  |  |  | ✓ |  |
    | `dest_linked_trip_id` | int or None | ≥ 1, FK → `linked_trips.linked_trip_id` |  |  |  |  |  |  |  | ✓ |  |
    | `o_lat` | float | ≥ -90, ≤ 90 |  |  | ✓ |  |  |  |  |  |  |
    | `o_lon` | float | ≥ -180, ≤ 180 |  |  | ✓ |  |  |  |  |  |  |
    | `d_lat` | float | ≥ -90, ≤ 90 |  |  | ✓ |  |  |  |  |  |  |
    | `d_lon` | float | ≥ -180, ≤ 180 |  |  | ✓ |  |  |  |  |  |  |
    | `o_location_type` | LocationType |  |  |  |  |  |  |  |  |  |  |
    | `d_location_type` | LocationType |  |  |  |  |  |  |  |  |  |  |
    | `tour_mode` | ModeType |  |  |  |  |  |  |  | ✓ |  |  |
    | `outbound_mode` | ModeType or None |  |  |  |  |  |  |  |  |  |  |
    | `inbound_mode` | ModeType or None |  |  |  |  |  |  |  |  |  |  |
    | `num_travelers` | int | ≥ 1 |  |  |  |  |  |  |  |  |  |
    | `survey_complete` | bool or None |  |  |  |  | ✓+ |  |  |  |  |  |
    | `<column>_preimputed` | named by config |  | + |  |  |  |  |  |  |  |  |
    | `d_<zone>` | named by config |  |  |  | + |  |  |  |  |  |  |
    | `joint_tour_id_<profile>` | named by config |  |  |  |  | + |  |  |  |  |  |
    | `o_<zone>` | named by config |  |  |  | + |  |  |  |  |  |  |
    | `tour_weight_<profile>` | named by config |  |  |  |  |  | + | + |  |  |  |
    | `usable_<profile>` | named by config |  |  |  |  | + |  |  |  |  |  |

=== "joint_trips"

    | Field | Type | Constraints | detect_joint_trips | extract_tours | cascade_usability | add_existing_weights | compute_weights | format_ctramp |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | `joint_trip_id` | int | ≥ 1, UNIQUE | + |  | ✓ | ✓ | ✓ | ✓ |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` | + |  | ✓ |  |  |  |
    | `hh_day_id` | int | ≥ 1, FK → `household_days.hh_day_id` | + |  |  |  |  | ✓ |
    | `travel_date` | datetime |  | + |  |  |  |  |  |
    | `num_joint_travelers` | int | ≥ 2 |  |  |  |  |  |  |
    | `survey_complete` | bool or None |  |  |  |  |  |  |  |
    | `joint_trip_weight_<profile>` | named by config |  |  |  |  | + | + |  |
    | `usable_<profile>` | named by config |  |  |  | + |  |  |  |

=== "joint_tours"

    | Field | Type | Constraints | extract_tours | cascade_usability | add_existing_weights | compute_weights | format_ctramp |
    | --- | --- | --- | --- | --- | --- | --- | --- |
    | `joint_tour_id` | int | ≥ 1, UNIQUE | + | ✓ | ✓ | ✓ | ✓ |
    | `hh_id` | int | ≥ 1, FK → `households.hh_id` | + | ✓ |  |  |  |
    | `hh_day_id` | int | ≥ 1, FK → `household_days.hh_day_id` | + |  |  |  |  |
    | `travel_date` | datetime |  | + |  |  |  |  |
    | `num_participants` | int | ≥ 2 |  |  |  |  |  |
    | `survey_complete` | bool or None |  |  |  |  |  |  |
    | `joint_tour_weight_<profile>` | named by config |  |  |  | + | + |  |
    | `usable_<profile>` | named by config |  |  | + |  |  |  |

=== "habitual_locations"

    | Field | Type | Constraints | detect_habitual_locations | extract_tours | add_zone_ids | format_ctramp | format_daysim |
    | --- | --- | --- | --- | --- | --- | --- | --- |
    | `habitual_location_id` | int | ≥ 1, UNIQUE | ✓+ | ✓ | ✓ | ✓ | ✓ |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` | ✓ | ✓ |  | ✓ | ✓ |
    | `location_type` | LocationType |  | ✓ | ✓ |  | ✓ | ✓ |
    | `location_num` | int | ≥ 1 | ✓ |  |  | ✓ | ✓ |
    | `is_primary` | bool or None |  | ✓ | ✓ |  |  |  |
    | `lat` | float | ≥ -90, ≤ 90 | ✓ | ✓ | ✓ | ✓ | ✓ |
    | `lon` | float | ≥ -180, ≤ 180 | ✓ | ✓ | ✓ | ✓ | ✓ |
    | `source` | LocationSource |  | ✓ | ✓ |  | ✓ | ✓ |
    | `<zone>` | named by config |  |  |  | + |  |  |

=== "habitual_location_days"

    | Field | Type | Constraints | detect_habitual_locations | format_ctramp | format_daysim |
    | --- | --- | --- | --- | --- | --- |
    | `habitual_location_id` | int | ≥ 1, FK → `habitual_locations.habitual_location_id` | + | ✓ | ✓ |
    | `person_id` | int | ≥ 1, FK → `persons.person_id` |  |  |  |
    | `person_day_id` | int | ≥ 1, FK → `person_days.person_day_id` |  |  |  |
    | `location_type` | LocationType |  |  |  |  |
    | `dwell_minutes` | float or None | ≥ 0 |  | ✓ | ✓ |
    | `n_visits` | int | ≥ 1 |  |  |  |
    | `is_day_start` | bool |  |  |  |  |
    | `is_day_end` | bool |  |  |  |  |

=== "household_weights"

    | Field | Type | Constraints | compute_weights |
    | --- | --- | --- | --- |
    | `hh_id` | int | ≥ 1, UNIQUE, FK → `households.hh_id` | + |
    | `target_zone_id` | str or None |  | + |
    | `zone_group_id` | str or None |  | + |
    | `home_puma` | str or None |  | + |
    | `bg_geo_id` | str or None |  | + |
    | `base_weight_<profile>` | named by config |  | + |
    | `hh_weight_<profile>` | named by config |  | + |
