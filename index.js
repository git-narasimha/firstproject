// Declarations
const sql = require('mssql');
const elasticsearch = require('elasticsearch');
const async = require('async');
const indexName = 'itops_all_custom_summary';
var elasticHost = process.env.elasticHost;

// DB Configurations
const config = {
    domain: 'ADHARMAN',
    user: 'SVC_DCAFSBI',
    password: 'u7dA}THV',
    server: '10.70.51.194',
    database: 'Summit_Prod_DN',
    pool: {
        max: 100,
        min: 0,
        idleTimeoutMillis: 30000
    },
    options: {
        encrypt: false // Use true if you're on Windows Azure
    }
};

var dataToElastic = {};

const monthNames = ["January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
];

var monthNumbers = [0];

// Fetching data
exports.handler = (event, context, callback) => {
    async.each(monthNumbers, function(i, callback) {
        var date = new Date();
        var currentMonth = monthNames[date.getMonth() - i];
        var currentYear = "" + date.getFullYear();
        var daysInMonth = "" + new Date(currentYear, (date.getMonth()+1)-i, 0).getDate();
        var currentDay = "" + date.getDate();
        var currentHour = "" + date.getHours();

        var classifications = `SELECT [Classification]
                        ,COUNT([Classification]) AS count 
                        FROM [dbo].[CMDB_RPT_DN_CMDBMaster]
                        WHERE [Active] = 1
                        GROUP BY Classification;`;
        var cmdbCIRelationTypes = `SELECT DISTINCT ([dbo].[CMDB_RPT_DN_CI_Relations].[Relation Type]) AS 'Relation Type'
                        ,COUNT(*) AS [count]
                    FROM [dbo].[CMDB_RPT_DN_CI_Relations]
                    LEFT JOIN [dbo].[CMDB_RPT_DN_CMDBMaster] WITH (NOLOCK) ON [dbo].[CMDB_RPT_DN_CMDBMaster].[Configuration Item Id] = [dbo].[CMDB_RPT_DN_CI_Relations].[Configuration Item Id]
                    WHERE [dbo].[CMDB_RPT_DN_CMDBMaster].[Active] = 1
                        AND [dbo].[CMDB_RPT_DN_CI_Relations].[Relation Type] NOT IN (
                            'Backup'
                            ,'Backup of'
                            )
                        AND [dbo].[CMDB_RPT_DN_CMDBMaster].[Classification] NOT IN ('Desktop')
                    GROUP BY [dbo].[CMDB_RPT_DN_CI_Relations].[Relation Type];`;
        var locations = `SELECT COUNT(DISTINCT Location) AS count FROM dbo.CMDB_RPT_DN_CMDBMaster;`;
        var serviceRequests = `SELECT COUNT(*) AS count
                        FROM dbo.SR_RPT_DN_ServiceTicketMaster
                        WHERE Month([Registered Time]) = Month(getdate()) - ${i}
                            AND Year([Registered Time]) = Year(getdate());`;
        var openServiceRequests = `SELECT Count(*) AS count
                        FROM dbo.SR_RPT_DN_ServiceTicketMaster
                        WHERE Month([Registered Time]) = Month(getdate()) - ${i}
                            AND Year([Registered Time]) = Year(getdate())
                            AND [Status] NOT IN (
                                'Closed'
                                ,'Resolved'
                                ,'Rejected'
                                ,'Cancelled'
                                );`;
        var problems = `SELECT COUNT(*) AS count
                        FROM dbo.PM_RPT_DN_ProblemMaster
                        WHERE Month([Registered Time]) = Month(getdate()) - ${i}
                            AND Year([Registered Time]) = Year(getdate())`;
        var changeRequests = `SELECT COUNT(*) AS count
                        FROM dbo.CM_RPT_DN_ChangeRequest_Master
                        WHERE Month([Request Registration Time]) = Month(getdate()) - ${i}
                            AND Year([Request Registration Time]) = Year(getdate());`;
        var incidents = `SELECT count(*) AS count
                        FROM [dbo].[IM_RPT_DN_TicketMaster]
                        WHERE (
                                Month([Registered Time]) = Month(getdate()) - ${i}
                                AND Year([Registered Time]) = Year(getdate())
                                );`;
        var p1p2 = `SELECT COUNT(DISTINCT [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident ID]) AS [count]
            ,[dbo].[IM_RPT_DN_MIM_Report_Vw].[Priority] AS 'severity'
        FROM [dbo].[IM_RPT_DN_MIM_Report_Vw] WITH (NOLOCK)
        INNER JOIN [dbo].[IM_RPT_DN_TicketMaster] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID] = [dbo].[IM_RPT_DN_MIM_Report_Vw].[Ticket ID]
        INNER JOIN [dbo].[IM_RPT_DN_TimeDiff] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_TimeDiff].[Ticket ID] = [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID]
        INNER JOIN [dbo].[IM_RPT_DN_Ticket_User_Attribute] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_Ticket_User_Attribute].[Ticket ID] = [dbo].[IM_RPT_DN_MIM_Report_Vw].[Ticket ID]
        LEFT JOIN [dbo].[IM_RPT_DN_IT_Outage_Information_Single_Valued] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_IT_Outage_Information_Single_Valued].[Ticket ID] = [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID]
        LEFT JOIN [dbo].[IM_TicketMaster_syn] WITH (NOLOCK) ON [dbo].[IM_TicketMaster_syn].[ticket_id] = [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID]
        WHERE (
                (
                    Month(CASE 
                            WHEN [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time] IS NULL
                                THEN NULL
                            ELSE DATEADD(Minute, - 480, [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time])
                            END) = Month(getdate()) - ${i}
                    AND Year(CASE 
                            WHEN [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time] IS NULL
                                THEN NULL
                            ELSE DATEADD(Minute, - 480, [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time])
                            END) = Year(getdate())
                    )
                )
            AND [dbo].[IM_RPT_DN_MIM_Report_Vw].[Priority] IN (
                'P1'
                ,'P2'
                )
            AND ([dbo].[IM_TicketMaster_syn].[ParentTicketID] IS NULL)
            AND NOT [dbo].[IM_RPT_DN_MIM_Report_Vw].[Related_to_3rd_party] = 'yes'
            AND NOT [dbo].[IM_RPT_DN_MIM_Report_Vw].[Related_to_Business_Administered] = 'yes'
            AND NOT [dbo].[IM_RPT_DN_MIM_Report_Vw].[Related_to_Facilities] = 'yes'
        GROUP BY [dbo].[IM_RPT_DN_MIM_Report_Vw].[Priority];`;
        var p3p4 = `SELECT count(*) AS count
                        ,[Severity Name] AS severity
                    FROM dbo.IM_RPT_DN_TicketMaster
                    WHERE Month([Registered Time]) = Month(getdate()) - ${i}
                        AND Year([Registered Time]) = Year(getdate())
                        AND [Severity Name] in ('P3', 'P4')
                    GROUP BY [Severity Name];`;
        var incResolutionSLA = `SELECT CAST(count(*) * 100 / (
                        SELECT count(*)
                        FROM dbo.IM_RPT_DN_TicketMaster
                        WHERE [Resolution SLA Met] IS NOT NULL
                            AND (
                                Month([Resolution Time]) = Month(getdate()) - ${i}
                                AND Year([Resolution Time]) = Year(getdate())
                                )
                        ) AS DECIMAL(10, 2)) AS count
            FROM dbo.IM_RPT_DN_TicketMaster
            WHERE [Resolution SLA Met] = 1
                AND (
                    Month([Resolution Time]) = Month(getdate()) - ${i}
                    AND Year([Resolution Time]) = Year(getdate())
                    );`;
        var incResponseSLA = `SELECT CAST(count(*) * 100 / (
                        SELECT count(*)
                        FROM dbo.IM_RPT_DN_TicketMaster
                        WHERE [Response SLA Met] IS NOT NULL
                            AND (
                                Month([Resolution Time]) = Month(getdate()) - ${i}
                                AND Year([Resolution Time]) = Year(getdate())
                                )
                        ) AS DECIMAL(10, 2)) AS count
            FROM dbo.IM_RPT_DN_TicketMaster
            WHERE [Response SLA Met] = 1
                AND (
                    Month([Resolution Time]) = Month(getdate()) - ${i}
                    AND Year([Resolution Time]) = Year(getdate())
                    );`;
        var srResolutionSLA = `SELECT CAST(count(*) * 100 / (
                        SELECT count(*)
                        FROM [dbo].[SR_RPT_DN_ServiceTicketMaster]
                        WHERE [Resolution SLA Met] IS NOT NULL
                            AND (
                                Month([Resolution Time]) = Month(getdate()) - ${ i}
                                AND Year([Resolution Time]) = Year(getdate())
                                )
                        ) AS DECIMAL(10, 2)) AS count
            FROM [dbo].[SR_RPT_DN_ServiceTicketMaster]
            WHERE [Resolution SLA Met] = 1
                AND (
                    Month([Resolution Time]) = Month(getdate()) - ${ i}
                    AND Year([Resolution Time]) = Year(getdate())
                    );`;
        var srResponseSLA = `SELECT CAST(count(*) * 100 / (
                        SELECT count(*)
                        FROM [dbo].[SR_RPT_DN_ServiceTicketMaster]
                        WHERE [Response SLA Met] IS NOT NULL
                            AND (
                                Month([Resolution Time]) = Month(getdate()) - ${i}
                                AND Year([Resolution Time]) = Year(getdate())
                                )
                        ) AS DECIMAL(10, 2)) AS count
            FROM [dbo].[SR_RPT_DN_ServiceTicketMaster]
            WHERE [Response SLA Met] = 1
                AND (
                    Month([Resolution Time]) = Month(getdate()) - ${i}
                    AND Year([Resolution Time]) = Year(getdate())
                    );`;
        var sdClosedTickets = `SELECT count(*) AS count
                        ,[Workgroup Name] AS location
                    FROM [dbo].[IM_RPT_DN_TicketMaster]
                    WHERE (
                            Month([Resolution Time]) = Month(getdate()) - ${i}
                            AND Year([Resolution Time]) = Year(getdate())
                            )
                        AND [Workgroup Name] IN ('Service Desk-Bangalore','Service Desk-Bucharest','Service Desk-Atlanta','Service Desk-Chengdu','Service Desk-Nizhny Novgorod','Service Desk-Lodz')
                        AND STATUS IN (
                            'Closed'
                            ,'Resolved'
                            )
                    GROUP BY [Workgroup Name];`;
        var sdOpenTickets = `SELECT count(*) AS count
                        ,[Workgroup Name] AS location
                    FROM [dbo].[IM_RPT_DN_TicketMaster]
                    WHERE (
                            Month([Registered Time]) = Month(getdate()) - ${i}
                            AND Year([Registered Time]) = Year(getdate())
                            )
                        AND [Workgroup Name] IN ('Service Desk-Bangalore','Service Desk-Bucharest','Service Desk-Atlanta','Service Desk-Chengdu','Service Desk-Nizhny Novgorod','Service Desk-Lodz')
                        AND STATUS NOT IN (
                            'Closed'
                            ,'Resolved'
                            ,'Rejected'
                            ,'Cancelled'
                            )
                    GROUP BY [Workgroup Name];`;
        var sdClosedSRs = `SELECT count(*) AS count
                    ,[Workgroup Name] AS location
                FROM [dbo].[SR_RPT_DN_ServiceTicketMaster]
                WHERE (
                        Month([Resolution Time]) = Month(getdate()) - ${i}
                        AND Year([Resolution Time]) = Year(getdate())
                        )
                    AND [Workgroup Name] IN ('Service Desk-Bangalore','Service Desk-Bucharest','Service Desk-Atlanta','Service Desk-Chengdu','Service Desk-Nizhny Novgorod','Service Desk-Lodz')
                    AND STATUS IN (
                        'Closed'
                        ,'Resolved'
                        )
                GROUP BY [Workgroup Name];`;
        var sdOpenSRs = `SELECT count(*) AS count
                    ,[Workgroup Name] AS location
                FROM [dbo].[SR_RPT_DN_ServiceTicketMaster]
                WHERE (
                        Month([Registered Time]) = Month(getdate()) - ${i}
                        AND Year([Registered Time]) = Year(getdate())
                        )
                    AND [Workgroup Name] IN ('Service Desk-Bangalore','Service Desk-Bucharest','Service Desk-Atlanta','Service Desk-Chengdu','Service Desk-Nizhny Novgorod','Service Desk-Lodz')
                    AND STATUS NOT IN (
                        'Closed'
                        ,'Resolved'
                        ,'Rejected'
                        ,'Cancelled'
                        )
                GROUP BY [Workgroup Name];`;
        var sdTotalVolumeThrough = `SELECT t.[location]
                ,SUM(t.[count]) AS 'count'
            FROM (
                SELECT count(*) AS count
                    ,[First Workgroup Name] AS location
                FROM [dbo].[SR_RPT_DN_ServiceTicketMaster]
                WHERE (
                        Month([Registered Time]) = Month(getdate()) - ${ i}
                        AND Year([Registered Time]) = Year(getdate())
                        )
                    AND [First Workgroup Name] IN (
                        'Service Desk-Lodz'
            ,'Service Desk-Nizhny Novgorod'
            ,'Service Desk-Chengdu'
            ,'Service Desk-Bangalore'
            ,'Service Desk-Atlanta'
            ,'Service Desk-Bucharest'
                        )
                GROUP BY [First Workgroup Name]
                
                UNION ALL
                
                SELECT count(*) AS count
                    ,[First Workgroup Name] AS location
                FROM [dbo].[IM_RPT_DN_TicketMaster]
                WHERE (
                        Month([Registered Time]) = Month(getdate()) - ${ i}
                        AND Year([Registered Time]) = Year(getdate())
                        )
                    AND [First Workgroup Name] IN (
                        'Service Desk-Lodz'
            ,'Service Desk-Nizhny Novgorod'
            ,'Service Desk-Chengdu'
            ,'Service Desk-Bangalore'
            ,'Service Desk-Atlanta'
            ,'Service Desk-Bucharest'
                        )
                GROUP BY [First Workgroup Name]
                ) t
            GROUP BY t.[location];`;
        var p1MTTR = `SELECT CAST(CAST(SUM(CAST([Actual_Outage_Duration_in_Minutes] AS DECIMAL(10, 2))) / COUNT(*) AS DECIMAL(10, 2)) / 60 AS DECIMAL(10, 2)) AS count
        FROM [IM_RPT_DN_MTTR_Vw]
        WHERE (
                Month([IM_RPT_DN_MTTR_Vw].[Actual_Resolved_Date_and_Time]) = Month(getdate()) - ${i}
                AND Year([IM_RPT_DN_MTTR_Vw].[Actual_Resolved_Date_and_Time]) = Year(getdate())
				AND Priority ='P1'
                )`;
        var p2MTTR = `SELECT CAST(CAST(SUM(CAST([Actual_Outage_Duration_in_Minutes] AS DECIMAL(10, 2))) / COUNT(*) AS DECIMAL(10, 2)) / 60 AS DECIMAL(10, 2)) AS count
        FROM [IM_RPT_DN_MTTR_Vw]
        WHERE (
                Month([IM_RPT_DN_MTTR_Vw].[Actual_Resolved_Date_and_Time]) = Month(getdate()) - ${i}
                AND Year([IM_RPT_DN_MTTR_Vw].[Actual_Resolved_Date_and_Time]) = Year(getdate())
				AND Priority ='P2'
                )`;
        var totalDownTimeP1P2 = `SELECT DISTINCT TOP 100000 SUM(ISNULL((datediff(ss, [dbo].[IM_RPT_DN_TicketMaster].[Registered Time], [dbo].[IM_RPT_DN_TicketMaster].[Resolution Time]) - ISNULL([dbo].[IM_RPT_DN_TicketMaster].[Total Pending Duration], 0) * 60), 0)) AS 'count'
                    FROM [dbo].[IM_RPT_DN_MIM_Report_Vw] WITH (NOLOCK)
                    INNER JOIN [dbo].[IM_RPT_DN_TicketMaster] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID] = [dbo].[IM_RPT_DN_MIM_Report_Vw].[Ticket ID]
                    INNER JOIN [dbo].[IM_RPT_DN_TimeDiff] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_TimeDiff].[Ticket ID] = [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID]
                    INNER JOIN [dbo].[IM_RPT_DN_Ticket_User_Attribute] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_Ticket_User_Attribute].[Ticket ID] = [dbo].[IM_RPT_DN_MIM_Report_Vw].[Ticket ID]
                    LEFT JOIN [dbo].[IM_RPT_DN_IT_Outage_Information_Single_Valued] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_IT_Outage_Information_Single_Valued].[Ticket ID] = [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID]
                    LEFT JOIN [dbo].[IM_RPT_DN_MajorIncidents_Vw] WITH (NOLOCK) ON [dbo].[IM_RPT_DN_MajorIncidents_Vw].[ticket_id] = [dbo].[IM_RPT_DN_TicketMaster].[Ticket ID]
                        AND ([dbo].[IM_RPT_DN_MajorIncidents_Vw].[majorincident] = 1)
                    WHERE (
                            (
                                Month(CASE 
                                        WHEN [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time] IS NULL
                                            THEN NULL
                                        ELSE DATEADD(Minute, - 480, [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time])
                                        END) = Month(getdate()) - ${i}
                                AND Year(CASE 
                                        WHEN [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time] IS NULL
                                            THEN NULL
                                        ELSE DATEADD(Minute, - 480, [dbo].[IM_RPT_DN_MIM_Report_Vw].[Incident Logged Time])
                                        END) = Year(getdate())
                                )
                            AND (
                                [dbo].[IM_RPT_DN_MIM_Report_Vw].[Priority] IN (
                                    N'P1'
                                    ,N'P2'
                                    )
                                )
                            AND (NOT ([dbo].[IM_RPT_DN_MajorIncidents_Vw].[majorincident] IS NULL))
                            )
                        AND (
                            (
                                [dbo].[IM_RPT_DN_MIM_Report_Vw].[Workgroup] IN (
                                    N'Asset Ops - Hardware'
                                    ,N'Asset Ops - Software'
                                    ,N'Authentication and Internet Services'
                                    ,N'Backup'
                                    ,N'Backup/Storage'
                                    ,N'Brazil L1'
                                    ,N'Brazil L2 Applications'
                                    ,N'Brazil L2 Infrastructure'
                                    ,N'Business Application Owners'
                                    ,N'Citrix'
                                    ,N'Cloud'
                                    ,N'CMDB Configuration Manager'
                                    ,N'CMDB Configuration Managers'
                                    ,N'CoC-BI tools'
                                    ,N'CoC-Digital Commerce'
                                    ,N'COC-DSC-Analytics'
                                    ,N'COC-DSC-Quality'
                                    ,N'COC-DSC-SCM'
                                    ,N'COC-DSC-SIOP'
                                    ,N'CoC-ETS'
                                    ,N'CoC-Finance'
                                    ,N'CoC-HR Tools'
                                    ,N'CoC-Project Mgmt. And Accounting'
                                    ,N'CoC-Sales Tools'
                                    ,N'CoC-Salesforce'
                                    ,N'CoC-SAP-BI'
                                    ,N'CoC-SAP-BIT Dashboard'
                                    ,N'CoC-SAP-CIMT'
                                    ,N'COC-SAP-IBP'
                                    ,N'CoC-Smart-Manufacturing'
                                    ,N'CoC-WEB'
                                    ,N'Compliance Managers'
                                    ,N'Compute'
                                    ,N'Database'
                                    ,N'Datacenter'
                                    ,N'DCAFS PM'
                                    ,N'DevOps-EDI'
                                    ,N'DevOps-HCL-Basis'
                                    ,N'DevOps-MWEFA'
                                    ,N'DevOps-SAP-ABAP'
                                    ,N'DevOps-SAP-Archiving'
                                    ,N'DevOps-SAP-Basis'
                                    ,N'DevOps-SAP-FICO'
                                    ,N'DevOps-SAP-PI'
                                    ,N'DevOps-SAP-SCM'
                                    ,N'DevOps-SAP-SD'
                                    ,N'DevOps-SAP-Security'
                                    ,N'DevOps-SAP-Tech'
                                    ,N'DigitalHUB Admin'
                                    ,N'Global Command Center'
                                    ,N'Gyakoktatas'
                                    ,N'Harman IT Architect Managers'
                                    ,N'Harman IT Ops Managers'
                                    ,N'HCS  Network'
                                    ,N'HCS  SAM'
                                    ,N'HCS India Telephony'
                                    ,N'HCS Wintel'
                                    ,N'HPRO-MSCRM'
                                    ,N'HTC Training'
                                    ,N'Information Security'
                                    ,N'IT Demand Fulfillment'
                                    ,N'Mac/IOS'
                                    ,N'Managers'
                                    ,N'Meeting Room Support'
                                    ,N'MFG Team'
                                    ,N'MIM'
                                    ,N'MIM – External Vendors'
                                    ,N'Mobile-SRM-Shopping-Carts'
                                    ,N'Navi Wiki'
                                    ,N'Network'
                                    ,N'New Hires'
                                    ,N'O and I-Richardson'
                                    ,N'Ops and Monitoring'
                                    ,N'Provisioning Team'
                                    ,N'Redbend IT'
                                    ,N'SCCM/OSD'
                                    ,N'Security'
                                    ,N'Security-L2'
                                    'NService Desk-Lodz'
                                    ,N'Service Desk-Nizhny Novgorod'
                                    ,N'Service Desk-Chengdu'
                                    ,N'Service Desk-Bangalore'
                                    ,N'Service Desk-Atlanta'
                                    ,N'Service Desk-Bucharest'
                                    ,N'SiteAdmins-Americas-BRMA'
                                    ,N'SiteAdmins-Americas-BRNV'
                                    ,N'SiteAdmins-Americas-MXJZ'
                                    ,N'SiteAdmins-Americas-MXQP'
                                    ,N'SiteAdmins-Americas-MXQT'
                                    ,N'SiteAdmins-Americas-MXTJ'
                                    ,N'SiteAdmins-Americas-USAT'
                                    ,N'SiteAdmins-Americas-USBE'
                                    ,N'SiteAdmins-Americas-USCH'
                                    ,N'SiteAdmins-Americas-USDE'
                                    ,N'SiteAdmins-Americas-USEK'
                                    ,N'SiteAdmins-Americas-USFR'
                                    ,N'SiteAdmins-Americas-USMV'
                                    ,N'SiteAdmins-Americas-USNO'
                                    ,N'SiteAdmins-Americas-USNR'
                                    ,N'SiteAdmins-Americas-USRD'
                                    ,N'SiteAdmins-Americas-USSF'
                                    ,N'SiteAdmins-Americas-USSJ'
                                    ,N'SiteAdmins-Americas-USVH'
                                    ,N'SiteAdmins-APAC'
                                    ,N'SiteAdmins-APAC-CNBJ'
                                    ,N'SiteAdmins-APAC-CNCD'
                                    ,N'SiteAdmins-APAC-CNCE'
                                    ,N'SiteAdmins-APAC-CNDD'
                                    ,N'SiteAdmins-APAC-CNSD'
                                    ,N'SiteAdmins-APAC-CNSH'
                                    ,N'SiteAdmins-APAC-CNSN'
                                    ,N'SiteAdmins-APAC-CNSR'
                                    ,N'SiteAdmins-APAC-CNSU'
                                    ,N'SiteAdmins-APAC-CNSZ'
                                    ,N'SiteAdmins-APAC-INBA'
                                    ,N'SiteAdmins-APAC-INBG'
                                    ,N'SiteAdmins-APAC-INBI'
                                    ,N'SiteAdmins-APAC-INBM'
                                    ,N'SiteAdmins-APAC-INBS'
                                    ,N'SiteAdmins-APAC-INBW'
                                    ,N'SiteAdmins-APAC-INCH'
                                    ,N'SiteAdmins-APAC-INCO'
                                    ,N'SiteAdmins-APAC-INGU'
                                    ,N'SiteAdmins-APAC-INMB'
                                    ,N'SiteAdmins-APAC-INNO'
                                    ,N'SiteAdmins-APAC-INPC'
                                    ,N'SiteAdmins-APAC-INPH'
                                    ,N'SiteAdmins-APAC-INPN'
                                    ,N'SiteAdmins-APAC-JPNG'
                                    ,N'SiteAdmins-APAC-JPTK'
                                    ,N'SiteAdmins-APAC-KRSL'
                                    ,N'SiteAdmins-APAC-KRSO'
                                    ,N'SiteAdmins-APAC-SGSG'
                                    ,N'SiteAdmins-EMEA-DEBC'
                                    ,N'SiteAdmins-EMEA-DEFI'
                                    ,N'SiteAdmins-EMEA-DEKA'
                                    ,N'SiteAdmins-EMEA-DEMG'
                                    ,N'SiteAdmins-EMEA-DESR'
                                    ,N'SiteAdmins-EMEA-DEUL'
                                    ,N'SiteAdmins-EMEA-DKAR'
                                    ,N'SiteAdmins-EMEA-FRPM'
                                    ,N'SiteAdmins-EMEA-FRPR'
                                    ,N'SiteAdmins-EMEA-GBBA'
                                    ,N'SiteAdmins-EMEA-GBHM'
                                    ,N'SiteAdmins-EMEA-HUBP'
                                    ,N'SiteAdmins-EMEA-HUPE'
                                    ,N'SiteAdmins-EMEA-HUSA'
                                    ,N'SiteAdmins-EMEA-HUSZ'
                                    ,N'SiteAdmins-EMEA-ILTE'
                                    ,N'SiteAdmins-EMEA-ITMI'
                                    ,N'SiteAdmins-EMEA-MACA'
                                    ,N'SiteAdmins-EMEA-NLAM'
                                    ,N'SiteAdmins-EMEA-PLLS'
                                    ,N'SiteAdmins-EMEA-PLLZ'
                                    ,N'SiteAdmins-EMEA-ROBC'
                                    ,N'SiteAdmins-EMEA-ROBW'
                                    ,N'SiteAdmins-EMEA-RUMW'
                                    ,N'SiteAdmins-EMEA-RUNN'
                                    ,N'SiteAdmins-Europe'
                                    ,N'SiteAdmins-North America'
                                    ,N'SiteAdmins-Russia'
                                    ,N'SiteAdmins-UK'
                                    ,N'Stack 8'
                                    ,N'Storage/SAN'
                                    ,N'Supplier MDM'
                                    ,N'Termination'
                                    ,N'Unified Communications'
                                    ,N'Unix'
                                    ,N'Viewfinity'
                                    ,N'WINTEL'
                                    ,N'WINTEL Alerts'
                                    ,N'Wipro IT Ops Managers'
                                    ,N'Workfront Support'
                                    )
                                )
                            )
                        AND NOT [Related_to_Business_Administered] = 'yes'`;
        var fcr = `SELECT CAST(AVG([count]) AS DECIMAL(10, 2)) AS [count]
                    FROM (
                        SELECT [Workgroup Name]
                            ,(SUM(fcrCount) * 100.0 / SUM(totalCount)) AS [count]
                        FROM (
                            SELECT [Workgroup Name]
                                ,(
                                    SUM(CASE 
                                            WHEN (DateDiff(HH, [Registered Time], [Resolution Time]) <= 4)
                                                THEN 1
                                            ELSE 0
                                            END)
                                    ) AS [fcrCount]
                                ,COUNT(*) AS [totalCount]
                            FROM dbo.IM_RPT_DN_TicketMaster
                            WHERE (
                                    [Workgroup Name] IN (
                                        'Service Desk-Lodz'
            ,'Service Desk-Nizhny Novgorod'
            ,'Service Desk-Chengdu'
            ,'Service Desk-Bangalore'
            ,'Service Desk-Atlanta'
            ,'Service Desk-Bucharest'
                                        )
                                    )
                                AND (
                                    Month([Resolution Time]) = Month(getdate()) - ${i}
                                    AND Year([Resolution Time]) = Year(getdate())
                                    )
                                AND (
                                    [Status] IN (
                                        'Closed'
                                        ,'Resolved'
                                        )
                                    )
                            GROUP BY [Workgroup Name]
                            
                            UNION ALL
                            
                            SELECT [Workgroup Name]
                                ,(
                                    SUM(CASE 
                                            WHEN (DateDiff(HH, [Registered Time], [Resolution Time]) <= 4)
                                                THEN 1
                                            ELSE 0
                                            END)
                                    ) AS [fcrCount]
                                ,COUNT(*) AS [totalCount]
                            FROM dbo.SR_RPT_DN_ServiceTicketMaster
                            WHERE (
                                    [Workgroup Name] IN (
                                        'Service Desk-Lodz'
            ,'Service Desk-Nizhny Novgorod'
            ,'Service Desk-Chengdu'
            ,'Service Desk-Bangalore'
            ,'Service Desk-Atlanta'
            ,'Service Desk-Bucharest'
                                        )
                                    )
                                AND (
                                    Month([Resolution Time]) = Month(getdate()) - ${i}
                                    AND Year([Resolution Time]) = Year(getdate())
                                    )
                                AND (
                                    [Status] IN (
                                        'Closed'
                                        ,'Resolved'
                                        )
                                    )
                            GROUP BY [Workgroup Name]
                            ) TEMP
                        GROUP BY [Workgroup Name]
                        ) TEMP2;`;
        var csat = `DECLARE @count DECIMAL(10, 2);

                SET @count = (
                        SELECT AVG([count])
                        FROM (
                            SELECT CAST(count(*) * 100 / (
                                        SELECT count(*)
                                        FROM [dbo].[IM_RPT_DN_CSAT_Details]
                                        WHERE (
                                                Month([FeedbackTime]) = Month(getdate()) - ${i}
                                                AND Year([FeedbackTime]) = Year(getdate())
                                                )
                                        ) AS DECIMAL(10, 2)) AS [count]
                            FROM [dbo].[IM_RPT_DN_CSAT_Details]
                            WHERE (
                                    Month([FeedbackTime]) = Month(getdate()) - ${i}
                                    AND Year([FeedbackTime]) = Year(getdate())
                                    )
                                AND [IsPoorRated] = '0'
                            GROUP BY [IsPoorRated]
                            
                            UNION
                            
                            SELECT CAST(count(*) * 100 / (
                                        SELECT count(*)
                                        FROM [dbo].[SR_RPT_DN_CSAT_Details]
                                        WHERE (
                                                Month([FeedbackTime]) = Month(getdate()) - ${i}
                                                AND Year([FeedbackTime]) = Year(getdate())
                                                )
                                        ) AS DECIMAL(10, 2)) AS [count]
                            FROM [dbo].[SR_RPT_DN_CSAT_Details]
                            WHERE (
                                    Month([FeedbackTime]) = Month(getdate()) - ${i}
                                    AND Year([FeedbackTime]) = Year(getdate())
                                    )
                                AND [IsPoorRated] = '0'
                            GROUP BY [IsPoorRated]
                            ) AS [count]
                        );
                
                SELECT @count AS [count];`;
        var mfg = `SELECT count(*) AS count
                        FROM [dbo].[IM_RPT_DN_TicketMaster]
                        WHERE Customer = 'Manufacturing'
                            AND (
                                Month([Registered Time]) = Month(getdate()) - ${i}
                                AND Year([Registered Time]) = Year(getdate())
                                );`;
        var totalHotspotTickets = `SELECT COUNT(*) as [count]
                        FROM [dbo].[IM_RPT_DN_TicketMaster]
                        WHERE [Medium] LIKE '%Hotspot%'
                            AND (
                                Month([Registered Time]) = Month(getdate()) - ${i}
                                AND Year([Registered Time]) = Year(getdate())
                                );`;
        var allocatedPCs = `SELECT COUNT(*) AS [count]
                        FROM [dbo].[AM_RPT_DN_AssetMaster]
                        WHERE [Category Name] = 'PC'
                            AND [Is Allocated] = 'Yes'`;

        const pool = new sql.ConnectionPool(config, err => {
            async.parallel({
                classifications: function(callback) {
                    pool.request().query(classifications, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            var obj = {};
                            var totalServers = 0;
                            var application = 0;
                            var backup = 0;
                            var dbCount = 0;
                            var winServers = 0;
                            var unixServers = 0;
                            var linuxServers = 0;
                            var nwdevices = 0;
                            var security = 0;
                            var storage = 0;
                            var ucdevices = 0;
                            var vmware = 0;
                            var desktop = 0;

                            result.recordset.forEach(x => {
                                if (!x.Classification == null || !x.Classification == "null" || x.Classification) {
                                    if(x.Classification != 'Desktop') {
                                        totalServers += x.count;
                                    }
                                    if (x.Classification == "Database / DB Instance") {
                                        dbCount += x.count;
                                    } else if (x.Classification == "Server / Wintel Server") {
                                        winServers += x.count;
                                    } else if (x.Classification == "Server / Linux Server") {
                                        unixServers += x.count;
                                    }else if (x.Classification == "Server / Linux Server") {
                                        linuxServers += x.count;
                                    } else if (x.Classification == "UC") {
                                        ucdevices += x.count;
                                    } else if (x.Classification.startsWith('Network & Security / Security Device')) {
                                        security += x.count;
                                    } else if (x.Classification.startsWith('Network & Security / Network Device')) {
                                        nwdevices += x.count;
                                    } else if (x.Classification.startsWith('Storage')) {
                                        storage += x.count;
                                    } else if (x.Classification == "Backup") {
                                        backup += x.count;
                                    } else if (x.Classification.startsWith('Application')) {
                                        application += x.count;
                                    } else if (x.Classification == "Server / Hypervisor Server") {
                                        vmware += x.count;
                                    }else if (x.Classification == "Desktop") {
                                        desktop += x.count;
                                    }

                                    obj.TotalServers = totalServers;
                                    obj.Application = application;
                                    obj.Database = dbCount;
                                    obj.Windows = winServers;
                                    obj.Unix = unixServers;
                                    obj.Linux = linuxServers;
                                    obj.UC = ucdevices;
                                    obj.Security = security;
                                    obj.Network = nwdevices;
                                    obj.Storage = storage;
                                    obj.Backup = backup;
                                    obj.VMWare = vmware;
                                    obj.Desktop = desktop;
                                }
                            });
                            callback(null, obj);
                        }
                    });
                },
                cmdbCIRelationTypes: function(callback) {
                    var array = [];
                    pool.request().query(cmdbCIRelationTypes, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            result.recordset.forEach(x => {
                                array.push({
                                    [x['Relation Type']]: x.count
                                });
                            });
                            callback(null, array);
                        }
                    });
                },  
                locations: function(callback) {
                    pool.request().query(locations, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                serviceRequests: function(callback) {
                    pool.request().query(serviceRequests, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                openServiceRequests: function(callback) {
                    pool.request().query(openServiceRequests, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                problems: function(callback) {
                    pool.request().query(problems, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                changeRequests: function(callback) {
                    pool.request().query(changeRequests, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                incidents: function(callback) {
                    pool.request().query(incidents, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                p1p2: function(callback) {
                    pool.request().query(p1p2, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            var obj = {};
                            var p1 = 0;
                            var p2 = 0;
                            if(result.recordset.length > 0) {
                                result.recordset.forEach(x => {
                                    if (x.severity == "P1") {
                                        p1 += x.count;
                                    } else if (x.severity == "P2") {
                                        p2 += x.count;
                                    }
                                    obj.p1 = p1;
                                    obj.p2 = p2;
                                })
                            }else {
                                obj.p1 = p1;
                                obj.p2 = p2;
                            }
                            callback(null, obj);
                        }
                    });
                },
                p3p4: function(callback) {
                    pool.request().query(p3p4, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            var obj = {};
                            var p3 = 0;
                            var p4 = 0;
                            result.recordset.forEach(x => {
                                if (x.severity == "P3") {
                                    p3 += x.count;
                                } else if (x.severity == "P4") {
                                    p4 += x.count;
                                }
                                obj.p3 = p3;
                                obj.p4 = p4;
                            })
                            callback(null, obj);
                        }
                    });
                },
                incResolutionSLA: function(callback) {
                    pool.request().query(incResolutionSLA, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                incResponseSLA: function(callback) {
                    pool.request().query(incResponseSLA, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                srResolutionSLA: function(callback) {
                    pool.request().query(srResolutionSLA, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                srResponseSLA: function(callback) {
                    pool.request().query(srResponseSLA, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                sdClosedTickets: function(callback) {
                    var obj = {};
                    pool.request().query(sdClosedTickets, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            result.recordset.forEach(x => {
                                obj[x.location] = x.count
                            })
                            callback(null, obj);
                        }
                    });
                },
                sdOpenTickets: function(callback) {
                    var obj = {};
                    pool.request().query(sdOpenTickets, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            result.recordset.forEach(x => {
                                obj[x.location] = x.count
                            })
                            callback(null, obj);
                        }
                    });
                },
                sdClosedSRs: function(callback) {
                    var obj = {};
                    pool.request().query(sdClosedSRs, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            result.recordset.forEach(x => {
                                obj[x.location] = x.count
                            })
                            callback(null, obj);
                        }
                    });
                },
                sdOpenSRs: function(callback) {
                    var obj = {};
                    pool.request().query(sdOpenSRs, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            result.recordset.forEach(x => {
                                obj[x.location] = x.count
                            })
                            callback(null, obj);
                        }
                    });
                },
                sdTotalVolumeThrough: function(callback) {
                    var obj = {};
                    pool.request().query(sdTotalVolumeThrough, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            result.recordset.forEach(x => {
                                obj[x.location] = x.count
                            })
                            callback(null, obj);
                        }
                    });
                },
                p1MTTR: function(callback) {
                    pool.request().query(p1MTTR, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count * 60 * 60);
                        }
                    });
                },
                p2MTTR: function(callback) {
                    pool.request().query(p2MTTR, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count * 60 * 60);
                        }
                    });
                },
                fcr: function(callback) {
                    pool.request().query(fcr, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                csat: function(callback) {
                    pool.request().query(csat, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                mfg: function(callback) {
                    pool.request().query(mfg, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                totalDownTimeP1P2: function(callback) {
                    pool.request().query(totalDownTimeP1P2, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                totalHotspotTickets: function(callback) {
                    pool.request().query(totalHotspotTickets, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                },
                allocatedPCs: function(callback) {
                    pool.request().query(allocatedPCs, (err, result) => {
                        if (err) {
                            console.log(err);
                            callback(null, 0);
                        } else {
                            callback(null, result.recordset[0].count);
                        }
                    });
                }
            }, function(err, result) {
                pool.close();
                dataToElastic.TotalServers = result.classifications.TotalServers;
                dataToElastic.Application = result.classifications.Application;
                dataToElastic.Backup = result.classifications.Backup;
                dataToElastic.Database = result.classifications.Database;
                dataToElastic.Windows = result.classifications.Windows;
                dataToElastic.Unix = result.classifications.Unix;
                dataToElastic.UC = result.classifications.UC;
                dataToElastic.Security = result.classifications.Security;
                dataToElastic.Network = result.classifications.Network;
                dataToElastic.Storage = result.classifications.Storage;
                dataToElastic.VMWare = result.classifications.VMWare;
                dataToElastic.Desktop = result.classifications.Desktop;

                dataToElastic.cmdbCIRelationTypes = result.cmdbCIRelationTypes;

                dataToElastic.AllocatedPCs = result.allocatedPCs;
                dataToElastic.Locations = result.locations;
                dataToElastic.ServiceRequests = result.serviceRequests;
                dataToElastic.OpenServiceRequests = result.openServiceRequests;
                dataToElastic.Problems = result.problems;
                dataToElastic.ChangeRequests = result.changeRequests;
                dataToElastic.Incidents = result.incidents;
                dataToElastic.TotalHotspotTickets = result.totalHotspotTickets;

                dataToElastic.P1 = result.p1p2.p1;
                dataToElastic.P2 = result.p1p2.p2;
                dataToElastic.P3 = result.p3p4.p3;
                dataToElastic.P4 = result.p3p4.p4;

                dataToElastic.ResolutionSLA = result.incResolutionSLA;
                dataToElastic.ResponseSLA = result.incResponseSLA;
                dataToElastic.SRResolutionSLA = result.srResolutionSLA;
                dataToElastic.SRResponseSLA = result.srResponseSLA;
                dataToElastic.SDClosedTickets = result.sdClosedTickets;
                dataToElastic.SDOpenTickets = result.sdOpenTickets;
                dataToElastic.SDClosedSRs = result.sdClosedSRs;
                dataToElastic.SDOpenSRs = result.sdOpenSRs;
                dataToElastic.SDTotalVolumeThrough = result.sdTotalVolumeThrough;
                dataToElastic.P1MTTR = result.p1MTTR;
                dataToElastic.P2MTTR = result.p2MTTR;
                dataToElastic.FCR = result.fcr;
                dataToElastic.CSAT = result.csat;
                dataToElastic.MFG = result.mfg;
                dataToElastic.TotalDownTimeP1P2 = result.totalDownTimeP1P2;
                dataToElastic.MajorRisk = 29;

                dataToElastic.MonthYear = "" + currentMonth + currentYear;
                var registeredTime = new Date(currentYear + "-" + currentMonth + "-" + daysInMonth + " 23:59:59.999 +0000");
                if (i == 0) {
                    registeredTime = new Date(currentYear + "-" + currentMonth + "-" + currentDay + " " + currentHour + ":59:59.999 +0000");
                }
                dataToElastic.RegisteredTime = registeredTime;
                sendDataToElastic(dataToElastic, currentMonth, currentYear);

            });
        })
        pool.on('error', err => {
            console.log(err);
            pool.close();
        })
    }, function(err) {
        console.log(`Error: ${err}`);
        callback(err);
    });
    callback(null, "All Done...");
}

// Send data to elasticsearch
function sendDataToElastic(dataToElastic, currentMonth, currentYear) {
    console.log(`Sending below data to elasticsearch for month ${currentMonth}: \n ${JSON.stringify(dataToElastic)}`);
    var client = new elasticsearch.Client({
        host: elasticHost
    });
    client.ping({
        requestTimeout: 30000,
    }, function(error) {
        if (error) {
            console.log('elasticsearch cluster is down!');
        } else {
            try {
                client.delete({
                    index: indexName,
                    type: indexName,
                    id: indexName + "_" + currentMonth + currentYear
                }, (err, resp, status) => {
                    console.log(err, resp);
                    client.index({
                        index: indexName,
                        id: indexName + "_" + currentMonth + currentYear,
                        type: indexName,
                        body: dataToElastic
                    }, function(err, resp, status) {
                        if (err) {
                            console.log(`Error on elastic upload for month ${currentMonth}: ${err}`);
                        } else {
                            console.log(`Successfully uploaded data to elastic for month ${currentMonth}: \n ${JSON.stringify(resp)}`);
                        }
                    });
                })
                
            } catch (error) {
                console.log(error);
            }
        }
    });
}