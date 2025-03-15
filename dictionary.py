class Dictionary:
    def lookup(self, type: str, code: str) -> str:
        if type == 'rdwy_part':
            return self.roadway_part(self, code)

        if type == 'rdwy_sys':
            return self.roadway_system(self, code)
        
        if type == 'direction':
            return self.direction(self, code)
        
        if type == 'street_suffix':
            return self.street_suffix(self, code)
        
        if type == 'unit_desc':
            return self.unit_description(self, code)
        
        if type == 'veh_color':
            return self.vehicle_color(self, code)
        
        if type == 'body_style':
            return self.body_style(self, code)
        
        if type == 'autonomous_unit':
            return self.autonomous_unit(self, code)
        
        if type == 'autonomous_level_engaged':
            return self.autonomous_level_engine(self, code)
        
        if type == 'dl_id_type':
            return self.driver_license(self, code)
        
        if type == 'dl_class':
            return self.driver_license_class(self, code)
        
        if type == 'person_type':
            return self.person_type(self, code)
        
        if type == 'person_seat_position':
            return self.seat_position(self, code)
        
        if type == 'injury_severity':
            return self.injury_severity(self, code)
        
        if type == 'ethnicity':
            return self.ethnicity(self, code)
        
        if type == 'sex':
            return self.sex(self, code)
        
        if type == 'ejected':
            return self.ejected(self, code)
        
        if type == 'restr':
            return self.restraint(self, code)
        
        if type == 'airbag':
            return self.airbag(self, code)
        
        if type == 'helmet':
            return self.helmet(self, code)
        
        if type == 'sol':
            return self.solicitation(self, code)
        
        if type == 'alc_spec':
            return self.alc_spec(self, code)
        
        if type == 'drug_spec':
            return self.drug_spec(self, code)
        
        if type == 'drug_result':
            return self.drug_result(self, code)
        
        if type == 'drug_category':
            return self.drug_category(self, code)
        
        if type == 'fin_resp_type':
            return self.fin_resp_type(self, code)
        
        if type == 'vehicle_damage_rating':
            return self.vehicle_damage_rate(self, code)
        
        if type == 'weather_cond':
            return self.weather_condition(self, code)
        
        if type == 'light_cond':
            return self.light_condition(self, code)
        
        if type == 'entering_roads':
            return self.entering_roads(self, code)
        
        if type == 'roadway_type':
            return self.roadway_type(self, code)
        
        if type == 'roadway_alignment':
            return self.roadway_alignment(self, code)
        
        if type == 'surface_condition':
            return self.surface_condition(self, code)
        
        if type == 'traffic_control':
            return self.traffic_conntrol(self, code)
        
        if type == 'unit_num_contributing' or type == 'contributing_contributing_factors':
            return self.factor_n_condition(self, code)

        return code
        
    def roadway_system(self, code: str) -> str:
        roadway_system_dictionary = {
            "IH": "Interstate",
            "US": "US Highway",
            "SH": "State Highway",
            "FM": "Farm to Market",
            "RR": "Ranch Road",
            "RM": "Ranch to Market",
            "BI": "Business Interstate",
            "BU": "Business US",
            "BS": "Business State",
            "BF": "Business FM",
            "SL": "State Loop",
            "TL": "Toll Road",
            "AL": "Alternate",
            "SP": "Spur",
            "CR": "County Road",
            "PR": "Park Road",
            "PV": "Private Road",
            "RC": "Recreational Road",
            "LR": "Local Road/Street"
        }

        return roadway_system_dictionary.get(code)
    
    def roadway_part(self, code: str) -> str:
        roadway_part_dictionary = {
            "1": "Main/Proper Lane",
            "2": "Service/Frontage Road",
            "3": "Entrance/On Ramp",
            "4": "Exit/Off Ramp",
            "5": "Connector/Flyover",
            "98": "Other (Explain in Narrative)"
        }

        return roadway_part_dictionary.get(code)
    
    def direction(self, code: str) -> str:
        direction_dictionary = {
            "N": "North",
            "E": "East",
            "S": "South",
            "W": "West",
            "NE": "Northeast",
            "SE": "Southeast",
            "SW": "Southwest",
            "NW": "Northwest"
        }

        return direction_dictionary.get(code)
    
    def street_suffix(self, code: str) -> str:
        street_suffix_dictionary = {
            "RD": "Road",
            "ST": "Street",
            "DR": "Drive",
            "AVE": "Avenue",
            "BLVD": "Boulevard",
            "PKWY": "Parkway",
            "LN": "Lane",
            "FWY": "Freeway",
            "HWY": "Highway",
            "WAY": "Way",
            "TRL": "Trail",
            "LOOP": "Loop",
            "EXPY": "Expressway",
            "CT": "Court",
            "CIR": "Circle",
            "PL": "Place",
            "PARK": "Park",
            "CV": "Cove",
            "PATH": "Path",
            "TRC": "Trace",
            "PT": "Point"
        }

        return street_suffix_dictionary.get(code)
    
    def unit_description(self, code: str) -> str:
        unit_description_dictionary = {
            "1": "Motor Vehicle",
            "2": "Train",
            "3": "Pedalcyclist",
            "4": "Pedestrian",
            "5": "Motorized Conveyance",
            "6": "Towed/Pushed/Trailer",
            "7": "Non-Contact",
            "98": "Other"
        }

        return unit_description_dictionary.get(code)
    
    def vehicle_color(self, code: str) -> str:
        vehicle_color_dictionary = {
            "BGE": "Beige",
            "BLK": "Black",
            "BLU": "Blue",
            "BRZ": "Bronze",
            "BRO": "Brown",
            "CAM": "Camouflage",
            "CPR": "Copper",
            "GLD": "Gold",
            "GRY": "Gray",
            "GRN": "Green",
            "MAR": "Maroon",
            "MUL": "Multicolored",
            "ONG": "Orange",
            "PNK": "Pink",
            "PLE": "Purple",
            "RED": "Red",
            "SIL": "Silver",
            "TAN": "Tan",
            "TEA": "Teal(green)",
            "TRQ": "Turquoise (blue)",
            "WHI": "White",
            "YEL": "Yellow",
            "98": "Other",
            "99": "Unknown"
        }

        return vehicle_color_dictionary.get(code)
    
    def body_style(self, code: str) -> str:
        body_style_dictionary = {
            "PC": "Police Car/Truck",
            "PM": "Police Motorcycle",
            "TL": "Trailer, Semi-Trailer, or Pole Trailer",
            "TR": "Truck",
            "TT": "Truck Tractor",
            "VN": "Van",
            "EV": "Neighborhood",
            "P2": "Passenger Car, 2-Door",
            "P4": "Passenger Car, 4-Door",
            "PK": "Pickup",
            "AM": "Ambulance",
            "BU": "Bus",
            "SB": "Yellow School Bus",
            "SBO": "School Bus Other",
            "FE": "Farm Equipment",
            "FT": "Fire Truck",
            "MC": "Motorcycle",
            "SV": "Sport Utility Vehicle",
            "98": "Other (Explain Vehicle in Narrative)",
            "99": "Unknown"
        }

        return body_style_dictionary.get(code)
    
    def autonomous_unit(self, code: str) -> str:
        autonomous_unit_dictionary = {
            "1": "Yes",
            "2": "No",
            "99": "Unknown"
        }

        return autonomous_unit_dictionary.get(code)
    
    def autonomous_level_engine(self, code: str) -> str:
        autonomous_level_dictionary = {
            "0": "No Automation",
            "1": "Driver Assistance",
            "2": "Partial Automation",
            "3": "Conditional Automation",
            "4": "High Automation",
            "5": "Full Automation",
            "6": "Automation Level Unknown",
            "99": "Unknown"
        }

        return autonomous_level_dictionary.get(code)
    
    def driver_license(self, code: str) -> str:
        driver_license_dictionary = {
            "1": "Driver License",
            "2": "Commercial Driver Lic.",
            "3": "Occupational",
            "4": "ID Card",
            "5": "Unlicensed",
            "95": "Autonomous",
            "98": "Other",
            "99": "Unknown"
        }

        return driver_license_dictionary.get(code)
    
    def driver_license_class(self, code: str) -> str:
        driver_license_class_dictionary = {
            "A": "Class A",
            "AM": "Class A and M",
            "B": "Class B",
            "BM": "Class B and M",
            "C": "Class C",
            "CM": "Class C and M",
            "M": "Class M",
            "5": "Unlicensed",
            "95": "Autonomous",
            "98": "Other/Out of State",
            "99": "Unknown"
        }

        return driver_license_class_dictionary.get(code)
    
    def commercial_driver_license_skip(self, code: str) -> str:
        commercial_license_dictionary = {
            "H": "Hazardous Materials",
            "N": "Tank Vehicle",
            "P": "Passenger",
            "S": "School Bus",
            "T": "Double/Triple Trailer",
            "X": "Tank Vehicle with Hazardous Materials",
            "5": "Unlicensed",
            "96": "None",
            "95": "Autonomous",
            "98": "Other/Out of State",
            "99": "Unknown"
        }

        return commercial_license_dictionary.get(code)
    
    def driver_license(self, code: str) -> str:
        driver_license_dictionary = {
            "A": "With corrective lenses",
            "B": "LOFS 21 or over",
            "C": "Daytime driving only",
            "D": "Speed not to exceed 45 mph",
            "E": "No manual transmission equipped CMV",
            "F": "Must hold valid learner lic. to MM/DD/YY",
            "G": "TRC 545.424 applies until MM/DD/YY",
            "H": "Vehicle not to exceed 26,000 lbs GVWR",
            "I": "MC not to exceed 250cc",
            "J": "Licensed MC operator 21 or over in sight",
            "K": "Intrastate only",
            "L": "No air brake equipped CMV",
            "M": "No Class A passenger vehicle",
            "N": "No Class A and B passenger vehicle",
            "O": "No tractor-trailer CMV",
            "Q": "LOFS 21 or over vehicle above Class B",
            "R": "LOFS 21 or over vehicle above Class C",
            "S": "Outside rearview mirror or hearing aid",
            "T": "Automatic transmission",
            "U": "Applicable prosthetic devices",
            "V": "Medical Variance",
            "W": "Power steering",
            "X": "No cargo in CMV tank vehicle",
            "Y": "Valid TX vision or limb waiver required",
            "Z": "No full air brake equipped CMV",
            "P1": "For Class M TRC 545.424 until MM/DD/YY",
            "P2": "To/from work/school",
            "P3": "To/from work",
            "P4": "To/from school",
            "P5": "To/from work/school or LOFS 21 or over",
            "P6": "To/from work or LOFS 21 or over",
            "P7": "To/from school or LOFS 21 or over",
            "P8": "With telescopic lens",
            "P9": "LOFS 21 or over bus only",
            "P10": "LOFS 21 or over school bus only",
            "P11": "Bus not to exceed 26,000 lbs GVWR",
            "P12": "Passenger CMVs restrict to Class C only",
            "P13": "LOFS 21 or over in veh equip w/airbrake",
            "P14": "Operation Class B exempt veh authorized",
            "P15": "Operation Class A exempt veh authorized",
            "P16": "If CMV, school buses interstate",
            "P17": "If CMV, government vehicles interstate",
            "P18": "If CMV, only trans personal prop interstate",
            "P19": "If CMV, trans corpse/sick/injured interstate",
            "P20": "If CMV, privately trans passengers interstate",
            "P21": "If CMV, fire/rescue interstate",
            "P22": "If CMV, intra-city zone drivers interstate",
            "P23": "If CMV, custom-harvesting interstate",
            "P24": "If CMV, transporting bees/hives interstate",
            "P25": "If CMV, use in oil/water well service/drill",
            "P26": "If CMV, for operation of mobile crane",
            "P27": "HME Expiration Date MM/DD/YY",
            "P28": "FRSI CDL valid MM/DD/YY to MM/DD/YY",
            "P29": "FRSI CDL MM/DD/YY - MM/DD/YY or exempt B veh",
            "P30": "FRSI CDL MM/DD/YY - MM/DD/YY or exempt A veh",
            "P31": "Class C only - no taxi/bus/emergency veh",
            "P32": "Other",
            "P33": "No passengers in CMV bus",
            "P34": "No express or highway driving",
            "P35": "Restricted to operation of three-wheeled MC",
            "P36": "Moped",
            "P37": "Occ/Essent need DL-no CMV-see court order",
            "P38": "Applicable vehicle devices",
            "P39": "Ignition Interlock required",
            "P40": "Vehicle not to exceed Class C",
            "5": "Unlicensed",
            "95": "Autonomous",
            "96": "None",
            "98": "Other/Out of State",
            "99": "Unknown"
        }

        return driver_license_dictionary.get(code)
    
    def person_type(self, code: str) -> str:
        person_type_dictionary = {
            "1": "Driver",
            "2": "Passenger/Occupant",
            "3": "Pedalcyclist",
            "4": "Pedestrian",
            "5": "Driver of Motorcycle Type Vehicle",
            "6": "Passenger/Occupant on Motorcycle Type Vehicle",
            "95": "Autonomous",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown"
        }

        return person_type_dictionary.get(code)
    
    def seat_position(self, code: str) -> str:
        seat_position_dictionary = {
            "1": "Front Left or Motorcycle Driver",
            "2": "Front Center or Motorcycle Sidecar Passenger",
            "3": "Front Right",
            "4": "Second Seat Left or Motorcycle Back Passenger",
            "5": "Second Seat Center",
            "6": "Second Seat Right",
            "7": "Third Seat Left",
            "8": "Third Seat Center",
            "9": "Third Seat Right",
            "10": "Cargo Area",
            "11": "Outside Vehicle",
            "13": "Other in Vehicle",
            "14": "Passenger in Bus",
            "16": "Pedestrian, Pedalcyclist, or Motorized Conveyance",
            "95": "Autonomous",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown"
        }

        return seat_position_dictionary.get(code)
    
    def injury_severity(self, code: str) -> str:
        injury_severity_dictionary = {
            "A": "Suspected Serious Injury",
            "B": "Suspected Minor Injury",
            "C": "Possible Injury",
            "K": "Fatal Injury",
            "N": "Not Injured",
            "95": "Autonomous",
            "99": "Unknown"
        }

        return injury_severity_dictionary.get(code)
    
    def ethnicity(self, code: str) -> str:
        ethnicity_dictionary = {
            "W": "White",
            "B": "Black",
            "H": "Hispanic",
            "A": "Asian",
            "I": "Amer. Indian/Alaskan Native",
            "95": "Autonomous",
            "98": "Other",
            "99": "Unknown"
        }

        return ethnicity_dictionary.get(code)
    
    def sex(self, code: str) -> str:
        sex_dictionary = {
            "1": "Male",
            "2": "Female",
            "95": "Autonomous",
            "99": "Unknown"
        }

        return sex_dictionary.get(code)
    
    def ejected(self, code: str) -> str:
        ejected_dictionary = {
            "1": "No",
            "2": "Yes",
            "3": "Yes, Partial",
            "97": "Not Applicable",
            "99": "Unknown"
        }

        return ejected_dictionary.get(code)
    
    def restraint(self, code: str) -> str:
        restraint_dictionary = {
            "1": "Shoulder and Lap Belt",
            "2": "Shoulder Belt Only",
            "3": "Lap Belt Only",
            "4": "Child Seat, Facing Forward",
            "5": "Child Seat, Facing Rear",
            "6": "Child Seat, Unknown",
            "7": "Child Booster Seat",
            "96": "None",
            "97": "Not Applicable",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown"
        }

        return restraint_dictionary.get(code)
    
    def airbag(self, code: str) -> str:
        airbag_dictionary = {
            "1": "Not Deployed",
            "2": "Deployed, Front",
            "3": "Deployed, Side",
            "4": "Deployed, Rear",
            "5": "Deployed, Multiple",
            "97": "Not Applicable",
            "99": "Unknown"
        }

        return airbag_dictionary.get(code)
    
    def helmet(self, code: str) -> str:
        helmet_dictionary = {
            "1": "Not Worn",
            "2": "Worn, Damaged",
            "3": "Worn, Not Damaged",
            "4": "Worn, Unk. Damage",
            "97": "Not Applicable",
            "99": "Unknown if Worn"
        }

        return helmet_dictionary.get(code)
    
    def solicitation(self, code: str) -> str:
        solicitation_dictionary = {
            "Y": "Solicit",
            "N": "No Solicit"
        }

        return solicitation_dictionary.get(code)
    
    def alc_spec(self, code: str) -> str:
        alc_spec_dictionary = {
            "1": "Breath",
            "2": "Blood",
            "3": "Urine",
            "4": "Refused",
            "96": "None",
            "98": "Other (Explain in Narrative)"
        }

        return alc_spec_dictionary.get(code)
    
    def drug_spec(self, code: str) -> str:
        drug_spec_dictionary = {
            "1": "Breath",
            "2": "Blood",
            "3": "Urine",
            "4": "Refused",
            "96": "None",
            "98": "Other (Explain in Narrative)"
        }

        return drug_spec_dictionary.get(code)
    
    def drug_result(self, code: str) -> str:
        drug_result_dictionnary = {
            "1": "Positive",
            "2": "Negative",
            "97": "Not Applicable",
            "99":"Unknown"
        }

        return drug_result_dictionnary.get(code)
    
    def drug_category(self, code: str) -> str:
        drug_category = {
            "1": "Liability Insurance Policy",
            "2": "CNS Depressants",
            "3": "CNS Stimulants",
            "4": "Hallucinogens",
            "6": "Narcotic Analgesics",
            "7": "Inhalants",
            "8": "Cannabis",
            "10": "Dissociative Anesthetics",
            "11": "Multiple Drugs (Explain in Narrative)",
            "97": "Not Applicable",
            "98": "Other Drugs (Explain in Narrative)",
            "99": "Unknown"
        }

        return drug_category.get(code)
    
    def fin_resp_type(self, code: str) -> str:
        fin_resp = {
            "1": "Liability Insurance Policy",
            "2": "Proof of Liability Insurance",
            "3": "Insurance Binder",
            "4": "Surety Bond",
            "5": "Certificate of Deposit with Comptroller",
            "6": "Certificate of Deposit with County Judge",
            "7": "Certificate of Self-Insurance"
        }

        return fin_resp.get(code)
    
    def vehicle_damage_rate(self, code: str) -> str:
        vehicle_damage = {
            "VB-1": "vehicle burned, NOT due to collision",
            "VB-7": "vehicle catches fire due to the collision",
            "TP-0": "top damage",
            "VX-0": "undercarriage damage",
            "MC-1": "motorcycle, moped, scooter, etc.",
            "NA": "Not Applicable (Farm Tractor, etc.)"
        }

        return vehicle_damage.get(code)
    
    def vehicle_operation_skip(self, code: str) -> str:
        vehicle_operation = {
            "1": "Interstate Commerce",
            "2": "Intrastate Commerce",
            "3": "Not in Commerce",
            "4": "Government",
            "5": "Personal"
        }

        return vehicle_operation.get(code)
    
    def carrier_id_type_skip(self, code: str) -> str:
        carrier_id_type = {
            "1": "US DOT",
            "2": "TxDOT",
            "3": "ICC/MC",
            "96": "None",
            "98": "Other (Explain in Narrative)"
        }

        return carrier_id_type.get(code)
    
    def vehicle_type_skip(self, code: str) -> str:
        vehicle_type = {
            "1": "Passenger Car",
            "2": "Light Truck",
            "3": "Bus (9-15)",
            "4": "Bus (>15)",
            "5": "Single Unit Truck 2 Axles 6 Tires",
            "6": "Single Unit Truck 3 or More Axles",
            "7": "Truck Trailer",
            "8": "Truck Tractor (Bobtail)",
            "9": "Tractor/Semi Trailer",
            "10": "Tractor/Double Trailer",
            "11": "Tractor/Triple Trailer",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown Heavy Truck"
        }

        return vehicle_type.get(code)
    
    def bus_type_skip(self, code: str) -> str:
        bus_type = {
            "0": "Not a Bus",
            "1": "School Bus (Public or Private)",
            "2": "Transit",
            "3": "Intercity",
            "4": "Charter",
            "5": "Other",
            "6": "Shuttle",
            "9": "Unknown"
        }

        return bus_type.get(code)
    
    def hazardous_material_class_skip(self, code: str) -> str:
        hazardous_material_class = {
            "1": "Explosives",
            "2": "Gases",
            "3": "Flammable Liquids",
            "4": "Flammable Solids",
            "5": "Oxidizers and Organic Peroxides",
            "6": "Toxic Materials and Infectious Substances",
            "7": "Radioactive Materials",
            "8": "Corrosive Materials",
            "9": "Miscellaneous Dangerous Goods"
        }

        return hazardous_material_class.get(code)
    
    def cargo_type_skip(self, code: str) -> str:
        cargo_type = {
            "1": "Bus (9-15)",
            "2": "Bus (>15)",
            "3": "Van/Enclosed Box",
            "4": "Cargo Tank",
            "5": "Flatbed",
            "6": "Dump",
            "7": "Concrete Mixer",
            "8": "Auto Transporter",
            "9": "Garbage",
            "10": "Grain Chips Gravel",
            "11": "Pole",
            "13": "Intermodal",
            "14": "Logging",
            "15": "Vehicle Towing Another Vehicle",
            "97": "Not Applicable",
            "98": "Other (Explain in Narrative)"
        }

        return cargo_type.get(code)
    
    def trailer_type_skip(self, code: str) -> str:
        trailer_type = {
            "1": "Full Trailer",
            "2": "Semi-Trailer",
            "3": "Pole Trailer"
        }

        return trailer_type.get(code)
    
    def trailer_type_skip(self, code: str) -> str:
        trailer_type = {
            "1": "Full Trailer",
            "2": "Semi-Trailer",
            "3": "Pole Trailer"
        }

        return trailer_type.get(code)

    def weather_condition(self, code: str) -> str:
        weather_condition = {
            "1": "Clear",
            "2": "Cloudy",
            "3": "Rain",
            "4": "Sleet/Hail",
            "5": "Snow",
            "6": "Fog",
            "7": "Blowing Sand/Snow",
            "8": "Severe Crosswinds",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown"
        }

        return weather_condition.get(code)
    
    def light_condition(self, code: str) -> str:
        light_condition = {
            "1": "Daylight",
            "2": "Dark, Not Lighted",
            "3": "Dark, Lighted",
            "4": "Dark, Unknown Lighting",
            "5": "Dawn",
            "6": "Dusk",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown"
        }

        return light_condition.get(code)
    
    def entering_roads(self, code: str) -> str:
        entering_roads = {
            "2": "Three Entering Roads – T",
            "3": "Three Entering Roads – Y",
            "4": "Four Entering Roads",
            "5": "Five Entering Roads",
            "6": "Six Entering Roads",
            "7": "Traffic Circle",
            "8": "Cloverleaf",
            "97": "Not Applicable",
            "98": "Other (Explain in Narrative)"
        }

        return entering_roads.get(code)

    def roadway_type(self, code: str) -> str:
        roadway_type = {
            "1": "Two-Way, Not Divided",
            "2": "Two-Way, Divided, Unprotected Median",
            "3": "Two-Way, Divided, Protected Median",
            "4": "One-Way",
            "98": "Other (Explain in Narrative)"
        }

        return roadway_type.get(code)

    def roadway_alignment(self, code: str) -> str:
        roadway_alignment = {
            "1": "Straight, Level",
            "2": "Straight, Grade",
            "3": "Straight, Hillcrest",
            "4": "Curve, Level",
            "5": "Curve, Grade",
            "6": "Curve, Hillcrest",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown"
        }

        return roadway_alignment.get(code)
    
    def surface_condition(self, code: str) -> str:
        surface_condition = {
            "1": "Dry",
            "2": "Wet",
            "3": "Standing Water",
            "4": "Snow",
            "5": "Slush",
            "6": "Ice",
            "7": "Sand, Mud, Dirt",
            "98": "Other (Explain in Narrative)",
            "99": "Unknown"
        }

        return surface_condition.get(code)

    def traffic_conntrol(self, code: str) -> str:
        traffic_conntrol = {
            "2": "Inoperative (Explain in Narrative)",
            "3": "Officer",
            "4": "Flagman",
            "5": "Signal Light",
            "6": "Flashing Red Light",
            "7": "Flashing Yellow Light",
            "8": "Stop Sign",
            "9": "Yield Sign",
            "10": "Warning Sign",
            "11": "Center Stripe/Divider",
            "12": "No Passing Zone",
            "13": "RR Gate/Signal",
            "15": "Crosswalk",
            "16": "Bike Lane",
            "17": "Marked Lanes",
            "18": "Signal Light With Red Light Running Camera",
            "96": "None",
            "98": "Other (Explain in Narrative)"
        }

        return traffic_conntrol.get(code)
    
    def factor_n_condition(self, code: str) -> str:
        factor_n_condition = {
            1: "Animal on Road - Domestic",
            2: "Animal on Road - Wild",
            3: "Backed without Safety",
            4: "Changed Lane when Unsafe",
            14: "Disabled in Traffic Lane",
            15: "Disregard Stop and Go Signal",
            16: "Disregard Stop Sign or Light",
            17: "Disregard Turn Marks at Intersection",
            18: "Disregard Warning Sign at Construction",
            19: "Distraction in Vehicle",
            20: "Driver Inattention",
            21: "Drove Without Headlights",
            22: "Failed to Control Speed",
            23: "Failed to Drive in Single Lane",
            24: "Failed to Give Half of Roadway",
            25: "Failed to Heed Warning Sign or Traffic Control Device",
            26: "Failed to Pass to Left Safely",
            27: "Failed to Pass to Right Safely",
            28: "Failed to Signal or Gave Wrong Signal",
            29: "Failed to Stop at Proper Place",
            30: "Failed to Stop for School Bus",
            31: "Failed to Stop for Train",
            32: "Failed to Yield ROW - Emergency Vehicle",
            33: "Failed to Yield ROW - Open Intersection",
            34: "Failed to Yield ROW - Private Drive",
            35: "Failed to Yield ROW - Stop Sign",
            36: "Failed to Yield ROW - To Pedestrian",
            37: "Failed to Yield ROW - Turning Left",
            38: "Failed to Yield ROW - Turn on Red",
            39: "Failed to Yield ROW - Yield Sign",
            40: "Fatigued or Asleep",
            41: "Faulty Evasive Action",
            42: "Fire in Vehicle",
            43: "Fleeing or Evading Police",
            44: "Followed Too Closely",
            45: "Had Been Drinking",
            46: "Handicapped Driver (Explain in Narrative)",
            47: "Ill (Explain in Narrative)",
            48: "Impaired Visibility (Explain in Narrative)",
            49: "Improper Start from a Stopped, Standing, or Parked Position",
            50: "Load Not Secured",
            51: "Opened Door Into Traffic Lane",
            52: "Oversized Vehicle or Load",
            53: "Overtake and Pass Insufficient Clearance",
            54: "Parked and Failed to Set Brakes",
            55: "Parked in Traffic Lane",
            56: "Parked without Lights",
            57: "Passed in No Passing Lane",
            58: "Passed on Shoulder",
            59: "Pedestrian FTYROW to Vehicle",
            60: "Unsafe Speed",
            61: "Speeding - (Over Limit)",
            62: "Taking Medication (Explain in Narrative)",
            63: "Turned Improperly - Cut Corner on Left",
            64: "Turned Improperly - Wide Right",
            65: "Turned Improperly - Wrong Lane",
            66: "Turned when Unsafe",
            67: "Intoxicated - Alcohol",
            68: "Intoxicated - Drug",
            69: "Wrong Side - Approach or Intersection",
            70: "Wrong Side - Not Passing",
            71: "Wrong Way - One Way Road",
            73: "Road Rage",
            74: "Cell/Mobile Device Use - Talking",
            75: "Cell/Mobile Device Use - Texting",
            76: "Cell/Mobile Device Use - Other",
            77: "Cell/Mobile Device Use - Unknown",
            78: "Failed to slow or move over for vehicles displaying emergency lights",
            79: "Drove on improved shoulder",
            98: "Other (Explain in Narrative)"
        }

        return factor_n_condition.get(code)











