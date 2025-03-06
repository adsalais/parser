use crate::configuration::DataType;

///
/// Definition of a srum table, with its topic name used by the ouput writers
/// https://github.com/libyal/esedb-kb/blob/main/documentation/System%20Resource%20Usage%20Monitor%20(SRUM).asciidoc
///
pub struct SrumTable {
    pub topic: &'static str,
    pub name: &'static str,
    pub fields: &'static [(&'static str, DataType)],
}

///
/// This sort field is used to generate the most significant bytes of the tuple id
/// and is used in clickhouse in the table definition
///
pub const SRUM_SORT_FIELD: &str = "TimeStamp";

///
/// retrieve the list of every SRUM tables
///
pub fn srum_tables() -> Vec<SrumTable> {
    vec![
        SrumTable {
            topic: APP_TIMELINE_PROVIDER_TOPIC,
            name: APP_TIMELINE_PROVIDER,
            fields: &APP_TIMELINE_PROVIDER_FIELDS,
        },
        SrumTable {
            topic: APPLICATION_RESOURCES_TOPIC,
            name: APPLICATION_RESOURCES,
            fields: &APPLICATION_RESOURCES_FIELDS,
        },
        SrumTable {
            topic: ENERGY_ESTIMATION_PROVIDER_TOPIC,
            name: ENERGY_ESTIMATION_PROVIDER,
            fields: &ENERGY_ESTIMATION_PROVIDER_FIELDS,
        },
        SrumTable {
            topic: ENERGY_USAGE_PROVIDER_TOPIC,
            name: ENERGY_USAGE_PROVIDER,
            fields: &ENERGY_USAGE_PROVIDER_FIELDS,
        },
        SrumTable {
            topic: ENERGY_USAGE_PROVIDER_LT_TOPIC,
            name: ENERGY_USAGE_PROVIDER_LT,
            fields: &ENERGY_USAGE_PROVIDER_LT_FIELDS,
        },
        SrumTable {
            topic: NETWORK_CONNECTIVITY_USAGE_TOPIC,
            name: NETWORK_CONNECTIVITY_USAGE,
            fields: &NETWORK_CONNECTIVITY_USAGE_FIELDS,
        },
        SrumTable {
            topic: NETWORK_DATA_USAGE_TOPIC,
            name: NETWORK_DATA_USAGE,
            fields: &NETWORK_DATA_USAGE_FIELDS,
        },
        SrumTable {
            topic: TAGGED_ENERGY_PROVIDER_TOPIC,
            name: TAGGED_ENERGY_PROVIDER,
            fields: &TAGGED_ENERGY_PROVIDER_FIELDS,
        },
        SrumTable {
            topic: VFU_PROVIDER_TOPIC,
            name: VFU_PROVIDER,
            fields: &VFU_PROVIDER_FIELDS,
        },
        SrumTable {
            topic: WPN_PROVIDER_TOPIC,
            name: WPN_PROVIDER,
            fields: &WPN_PROVIDER_FIELDS,
        },
    ]
}

///
/// name of the table that contains the string values
///
pub const ID_MAP_TABLE: &str = "SruDbIdMapTable";

pub const APP_TIMELINE_PROVIDER_TOPIC: &str = "srum_app_timeline";
pub const APP_TIMELINE_PROVIDER: &str = "{5C8CF1C7-7257-4F13-B223-970EF5939312}";
pub const APP_TIMELINE_PROVIDER_FIELDS: [(&str, DataType); 44] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("Flags", DataType::Int32),
    ("EndTime", DataType::Int64),
    ("DurationMS", DataType::Int32),
    ("SpanMS", DataType::Int32),
    ("TimelineEnd", DataType::Int32),
    ("InFocusTimeline", DataType::Int64),
    ("UserInputTimeline", DataType::Int64),
    ("CompRenderedTimeline", DataType::Int64),
    ("CompDirtiedTimeline", DataType::Int64),
    ("CompPropagatedTimeline", DataType::Int64),
    ("AudioInTimeline", DataType::Int64),
    ("AudioOutTimeline", DataType::Int64),
    ("CpuTimeline", DataType::Int64),
    ("DiskTimeline", DataType::Int64),
    ("NetworkTimeline", DataType::Int64),
    ("MBBTimeline", DataType::Int64),
    ("InFocusS", DataType::Int32),
    ("PSMForegroundS", DataType::Int32),
    ("UserInputS", DataType::Int32),
    ("CompRenderedS", DataType::Int32),
    ("CompDirtiedS", DataType::Int32),
    ("CompPropagatedS", DataType::Int32),
    ("AudioInS", DataType::Int32),
    ("AudioOutS", DataType::Int32),
    ("Cycles", DataType::Int64),
    ("CyclesBreakdown", DataType::Int64),
    ("CyclesAttr", DataType::Int64),
    ("CyclesAttrBreakdown", DataType::Int64),
    ("CyclesWOB", DataType::Int64),
    ("CyclesWOBBreakdown", DataType::Int64),
    ("DiskRaw", DataType::Int64),
    ("NetworkTailRaw", DataType::Int64),
    ("NetworkBytesRaw", DataType::Int64),
    ("MBBTailRaw", DataType::Int64),
    ("MBBBytesRaw", DataType::Int64),
    ("DisplayRequiredS", DataType::Int32),
    ("DisplayRequiredTimeline", DataType::Int64),
    ("KeyboardInputTimeline", DataType::Int64),
    ("KeyboardInputS", DataType::Int32),
    ("MouseInputS", DataType::Int32),
];

pub const APPLICATION_RESOURCES_TOPIC: &str = "srum_application_resources";
pub const APPLICATION_RESOURCES: &str = "{D10CA2FE-6FCF-4F6D-848E-B2E99266FA89}";
pub const APPLICATION_RESOURCES_FIELDS: [(&str, DataType); 19] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("ForegroundCycleTime", DataType::Int64),
    ("BackgroundCycleTime", DataType::Int64),
    ("FaceTime", DataType::Int64),
    ("ForegroundContextSwitches", DataType::Int32),
    ("BackgroundContextSwitches", DataType::Int32),
    ("ForegroundBytesRead", DataType::Int64),
    ("ForegroundBytesWritten", DataType::Int64),
    ("ForegroundNumReadOperations", DataType::Int32),
    ("ForegroundNumWriteOperations", DataType::Int32),
    ("ForegroundNumberOfFlushes", DataType::Int32),
    ("BackgroundBytesRead", DataType::Int64),
    ("BackgroundBytesWritten", DataType::Int64),
    ("BackgroundNumReadOperations", DataType::Int32),
    ("BackgroundNumWriteOperations", DataType::Int32),
    ("BackgroundNumberOfFlushes", DataType::Int32),
];

pub const ENERGY_ESTIMATION_PROVIDER_TOPIC: &str = "srum_energy_estimation";
pub const ENERGY_ESTIMATION_PROVIDER: &str = "{DA73FB89-2BEA-4DDC-86B8-6E048C6DA477}";
pub const ENERGY_ESTIMATION_PROVIDER_FIELDS: [(&str, DataType); 5] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("BinaryData", DataType::String), //binary
];

pub const ENERGY_USAGE_PROVIDER_TOPIC: &str = "srum_energy_usage";
pub const ENERGY_USAGE_PROVIDER: &str = "{FEE4E14F-02A9-4550-B5CE-5FA2DA202E37}";
pub const ENERGY_USAGE_PROVIDER_FIELDS: [(&str, DataType); 11] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("EventTimestamp", DataType::Int64),
    ("StateTransition", DataType::Int32),
    ("DesignedCapacity", DataType::Int32),
    ("FullChargedCapacity", DataType::Int32),
    ("ChargeLevel", DataType::Int32),
    ("CycleCount", DataType::Int32),
    ("ConfigurationHash", DataType::String),
];

pub const ENERGY_USAGE_PROVIDER_LT_TOPIC: &str = "srum_energy_usage_long_term";
pub const ENERGY_USAGE_PROVIDER_LT: &str = "{FEE4E14F-02A9-4550-B5CE-5FA2DA202E37}LT";
pub const ENERGY_USAGE_PROVIDER_LT_FIELDS: [(&str, DataType); 16] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("ActiveAcTime", DataType::Int32),
    ("CsAcTime", DataType::Int32),
    ("ActiveDcTime", DataType::Int32),
    ("CsDcTime", DataType::Int32),
    ("ActiveDischargeTime", DataType::Int32),
    ("CsDischargeTime", DataType::Int32),
    ("ActiveEnergy", DataType::Int32),
    ("CsEnergy", DataType::String),
    ("DesignedCapacity", DataType::String),
    ("FullChargedCapacity", DataType::Int32),
    ("CycleCount", DataType::Int32),
    ("ConfigurationHash", DataType::String),
];

pub const NETWORK_CONNECTIVITY_USAGE_TOPIC: &str = "srum_network_connectivity_usage";
pub const NETWORK_CONNECTIVITY_USAGE: &str = "{DD6636C4-8929-4683-974E-22C046A43763}";
pub const NETWORK_CONNECTIVITY_USAGE_FIELDS: [(&str, DataType); 9] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("InterfaceLuid", DataType::Int64),
    ("L2ProfileId", DataType::Int32),
    ("ConnectedTime", DataType::Int32),
    ("ConnectStartTime", DataType::Date),
    ("L2ProfileFlags", DataType::Int32),
];

pub const NETWORK_DATA_USAGE_TOPIC: &str = "srum_network_data_usage";
pub const NETWORK_DATA_USAGE: &str = "{973F5D5C-1D90-4944-BE8E-24B94231A174}";
pub const NETWORK_DATA_USAGE_FIELDS: [(&str, DataType); 9] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("InterfaceLuid", DataType::Int64),
    ("L2ProfileId", DataType::Int32),
    ("L2ProfileFlags", DataType::Int32),
    ("BytesSent", DataType::Int64),
    ("BytesRecvd", DataType::Int64),
];

pub const TAGGED_ENERGY_PROVIDER_TOPIC: &str = "srum_tagged_energy";
pub const TAGGED_ENERGY_PROVIDER: &str = "{B6D82AF1-F780-4E17-8077-6CB9AD8A6FC4}";
pub const TAGGED_ENERGY_PROVIDER_FIELDS: [(&str, DataType); 7] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("Metadata", DataType::Int64),
    ("EnergyData", DataType::String), //bytes
    ("Tag", DataType::String),
];

pub const VFU_PROVIDER_TOPIC: &str = "srum_vfuprov";
pub const VFU_PROVIDER: &str = "{7ACBBAA3-D029-4BE4-9A7A-0885927F1D8F}";
pub const VFU_PROVIDER_FIELDS: [(&str, DataType); 8] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("Flags", DataType::Int64),
    ("StartTime", DataType::Int64),
    ("EndTime", DataType::Int64),
    ("Usage", DataType::String), //bytes
];

pub const WPN_PROVIDER_TOPIC: &str = "srum_wpn_provider";
pub const WPN_PROVIDER: &str = "{D10CA2FE-6FCF-4F6D-848E-B2E99266FA86}";
pub const WPN_PROVIDER_FIELDS: [(&str, DataType); 7] = [
    ("AutoIncId", DataType::Int32),
    ("TimeStamp", DataType::Date),
    ("AppId", DataType::String),
    ("UserId", DataType::String),
    ("NotificationType", DataType::Int32),
    ("PayloadSize", DataType::Int32),
    ("NetworkType", DataType::Int32),
];
