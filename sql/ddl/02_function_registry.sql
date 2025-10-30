-- ============================================================================
-- FunctionRegistry DDL
-- Metadata table for tracking registered transformation functions
-- ============================================================================
-- 
-- Purpose:
--   Track all registered functions in the ODIBI function registry for
--   documentation, versioning, and discovery purposes.
--
-- Key Features:
--   • Function versioning
--   • Engine-specific variants (spark, pandas, any)
--   • Module-based organization
--   • Metadata tracking (author, tags, description)
--   • Audit trail (created_at, updated_at)
-- ============================================================================

CREATE TABLE IF NOT EXISTS FunctionRegistry (
    -- Primary identification
    function_id VARCHAR(100) PRIMARY KEY,
    
    -- Function details
    module VARCHAR(255) NOT NULL,
    function_name VARCHAR(255) NOT NULL,
    
    -- Engine specification
    engine VARCHAR(20) NOT NULL DEFAULT 'any',  -- 'spark', 'pandas', 'any'
    
    -- Versioning
    version VARCHAR(50) NOT NULL DEFAULT '1.0',
    is_active BIT NOT NULL DEFAULT 1,
    
    -- Documentation
    description NVARCHAR(1000),
    usage_example NVARCHAR(MAX),
    
    -- Metadata
    author VARCHAR(100),
    tags NVARCHAR(500),  -- Comma-separated or JSON array
    category VARCHAR(100),
    
    -- Dependencies
    required_packages NVARCHAR(500),  -- JSON array of package names
    
    -- Performance hints
    estimated_complexity VARCHAR(50),  -- 'low', 'medium', 'high'
    supports_streaming BIT DEFAULT 0,
    
    -- Audit fields
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    
    -- Indexing for performance
    INDEX idx_module (module),
    INDEX idx_engine (engine),
    INDEX idx_function_name (function_name),
    INDEX idx_active (is_active),
    UNIQUE INDEX idx_module_function_engine (module, function_name, engine, version)
);

-- ============================================================================
-- Example Records - Data Cleaning Functions
-- ============================================================================

-- Universal deduplication function
INSERT INTO FunctionRegistry (
    function_id,
    module,
    function_name,
    engine,
    version,
    is_active,
    description,
    usage_example,
    author,
    tags,
    category,
    required_packages,
    estimated_complexity,
    supports_streaming,
    created_by
) VALUES (
    'cleaning-remove_duplicates-any-v1.0',
    'odibi_de_v2.odibi_functions.examples',
    'remove_duplicates',
    'any',
    '1.0',
    1,
    'Remove duplicate rows from DataFrame. Works with both Spark and Pandas engines.',
    'df_clean = remove_duplicates(df, subset=[''id'', ''timestamp''])',
    'odibi_team',
    'cleaning,deduplication,data_quality',
    'data_cleaning',
    '["pandas","pyspark"]',
    'low',
    1,
    'system'
);

-- Pandas-specific cleaning function
INSERT INTO FunctionRegistry (
    function_id,
    module,
    function_name,
    engine,
    version,
    is_active,
    description,
    usage_example,
    author,
    tags,
    category,
    required_packages,
    estimated_complexity,
    supports_streaming,
    created_by
) VALUES (
    'cleaning-deduplicate_pandas-pandas-v1.0',
    'odibi_de_v2.odibi_functions.examples',
    'deduplicate_pandas',
    'pandas',
    '1.0',
    1,
    'Remove duplicate rows from Pandas DataFrame using drop_duplicates method.',
    'clean_df = deduplicate_pandas(df, subset=[''transaction_id''])',
    'odibi_team',
    'cleaning,pandas,deduplication',
    'data_cleaning',
    '["pandas"]',
    'low',
    0,
    'system'
);

-- Spark-specific cleaning function
INSERT INTO FunctionRegistry (
    function_id,
    module,
    function_name,
    engine,
    version,
    is_active,
    description,
    usage_example,
    author,
    tags,
    category,
    required_packages,
    estimated_complexity,
    supports_streaming,
    created_by
) VALUES (
    'cleaning-deduplicate_spark-spark-v1.0',
    'odibi_de_v2.odibi_functions.examples',
    'deduplicate_spark',
    'spark',
    '1.0',
    1,
    'Remove duplicate rows from Spark DataFrame using dropDuplicates method. Optimized for distributed processing.',
    'clean_df = deduplicate_spark(spark_df, subset=[''transaction_id''])',
    'odibi_team',
    'cleaning,spark,deduplication,distributed',
    'data_cleaning',
    '["pyspark"]',
    'medium',
    1,
    'system'
);

-- ============================================================================
-- Example Records - Aggregation Functions
-- ============================================================================

INSERT INTO FunctionRegistry (
    function_id,
    module,
    function_name,
    engine,
    version,
    is_active,
    description,
    usage_example,
    author,
    tags,
    category,
    required_packages,
    estimated_complexity,
    supports_streaming,
    created_by
) VALUES (
    'aggregation-calculate_daily_stats-pandas-v1.0',
    'custom_functions.aggregation',
    'calculate_daily_stats',
    'pandas',
    '1.0',
    1,
    'Calculate daily statistics including count, sum, mean, and unique counts using Pandas groupby.',
    'daily_stats = calculate_daily_stats(df)',
    'analytics_team',
    'aggregation,statistics,time_series',
    'aggregation',
    '["pandas"]',
    'medium',
    0,
    'analytics_team'
);

-- ============================================================================
-- Example Records - Validation Functions
-- ============================================================================

INSERT INTO FunctionRegistry (
    function_id,
    module,
    function_name,
    engine,
    version,
    is_active,
    description,
    usage_example,
    author,
    tags,
    category,
    required_packages,
    estimated_complexity,
    supports_streaming,
    created_by
) VALUES (
    'validation-check_data_quality-pandas-v2.0',
    'custom_functions.quality',
    'check_data_quality',
    'pandas',
    '2.0',
    1,
    'Comprehensive data quality checker with configurable thresholds for null percentage and duplicates.',
    'report = check_data_quality(df, null_threshold=0.1, dup_threshold=0.05)',
    'quality_team',
    'validation,data_quality,monitoring',
    'data_quality',
    '["pandas"]',
    'low',
    0,
    'quality_team'
);

-- ============================================================================
-- Example Records - Enrichment Functions
-- ============================================================================

INSERT INTO FunctionRegistry (
    function_id,
    module,
    function_name,
    engine,
    version,
    is_active,
    description,
    usage_example,
    author,
    tags,
    category,
    required_packages,
    estimated_complexity,
    supports_streaming,
    created_by
) VALUES (
    'enrichment-add_revenue_column-pandas-v1.0',
    'customer_analytics.enrichment',
    'add_revenue_column',
    'pandas',
    '1.0',
    1,
    'Calculate revenue from quantity and price columns. Adds new revenue column to DataFrame.',
    'enriched_df = add_revenue_column(df)',
    'data_team',
    'enrichment,calculation,revenue',
    'data_enrichment',
    '["pandas"]',
    'low',
    0,
    'data_team'
);

INSERT INTO FunctionRegistry (
    function_id,
    module,
    function_name,
    engine,
    version,
    is_active,
    description,
    usage_example,
    author,
    tags,
    category,
    required_packages,
    estimated_complexity,
    supports_streaming,
    created_by
) VALUES (
    'enrichment-add_timestamp-any-v1.0',
    'customer_analytics.enrichment',
    'add_timestamp',
    'any',
    '1.0',
    1,
    'Add current timestamp column to DataFrame for audit trail. Universal function works with both engines.',
    'timestamped_df = add_timestamp(df)',
    'data_team',
    'enrichment,timestamp,audit',
    'data_enrichment',
    '["pandas","pyspark"]',
    'low',
    1,
    'data_team'
);

-- ============================================================================
-- Utility Views
-- ============================================================================

-- View: Active Functions by Engine
CREATE VIEW vw_ActiveFunctionsByEngine AS
SELECT 
    engine,
    COUNT(*) AS function_count,
    COUNT(DISTINCT module) AS module_count
FROM FunctionRegistry
WHERE is_active = 1
GROUP BY engine;

-- View: Function Inventory
CREATE VIEW vw_FunctionInventory AS
SELECT 
    function_id,
    module,
    function_name,
    engine,
    version,
    description,
    author,
    category,
    tags,
    created_at
FROM FunctionRegistry
WHERE is_active = 1
ORDER BY module, function_name, engine;

-- View: Function Dependencies
CREATE VIEW vw_FunctionDependencies AS
SELECT 
    function_name,
    engine,
    version,
    required_packages,
    estimated_complexity,
    supports_streaming
FROM FunctionRegistry
WHERE is_active = 1
    AND required_packages IS NOT NULL;

-- ============================================================================
-- Maintenance Queries
-- ============================================================================

-- Query 1: Find all functions in a module
-- SELECT * FROM FunctionRegistry 
-- WHERE module = 'odibi_de_v2.odibi_functions.examples' 
--   AND is_active = 1;

-- Query 2: Find engine-specific variants
-- SELECT function_name, engine, version, description
-- FROM FunctionRegistry
-- WHERE function_name = 'deduplicate_pandas'
--   AND is_active = 1;

-- Query 3: Find functions by tag
-- SELECT function_name, engine, tags, description
-- FROM FunctionRegistry
-- WHERE tags LIKE '%cleaning%'
--   AND is_active = 1;

-- Query 4: Function version history
-- SELECT function_name, engine, version, created_at, is_active
-- FROM FunctionRegistry
-- WHERE function_name = 'check_data_quality'
-- ORDER BY created_at DESC;

-- Query 5: Most recently added functions
-- SELECT TOP 10 
--     function_name,
--     engine,
--     module,
--     created_at
-- FROM FunctionRegistry
-- WHERE is_active = 1
-- ORDER BY created_at DESC;
