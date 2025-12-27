graph LR
    subgraph External_Sources [External Data]
        SEC[SEC EDGAR API/FTP]
    end

    subgraph Airflow_Orchestration [Airflow Pipeline]
        Extract[Extract Filings]
        Parse[Parse & Clean Data]
    end

    subgraph Cloud_Storage [Staging]
        S3[(S3 / GCS Bucket)]
    end

    subgraph Data_Warehouse [Snowflake]
        RAW_TBL[[Raw Stage Table]]
        PROD_TBL[[Analytical Tables]]
    end

    subgraph Downstream [Consumers]
        BI[Dashboard / BI Tool]
        ML[ML Models / Research]
    end

    %% Flow Connections
    SEC --> Extract
    Extract --> Parse
    Parse --> S3
    S3 --> RAW_TBL
    RAW_TBL --> PROD_TBL
    PROD_TBL --> BI
    PROD_TBL --> ML

    %% Styling
    style Airflow_Orchestration fill:#f9f,stroke:#333,stroke-width:2px
    style Data_Warehouse fill:#00a3e0,color:#fff,stroke:#333
