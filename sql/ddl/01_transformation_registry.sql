-- ============================================================================
-- TransformationRegistry DDL
-- Generic, project-agnostic transformation configuration table
-- ============================================================================
-- 
-- Purpose:
--   Replaces the old TransformationConfig table with a flexible design that
--   supports any project, any number of inputs, constants, and outputs.
--
-- Key Features:
--   • Generic entity system (entity_1, entity_2, entity_3)
--   • Multiple inputs support (JSON array)
--   • Constants/parameters (JSON object)
--   • Multiple outputs (JSON array)
--   • Layer-based execution ordering
--   • Environment-aware (qat/prod)
--   • Project-scoped
-- ============================================================================

CREATE TABLE IF NOT EXISTS TransformationRegistry (
    -- Primary identification
    transformation_id VARCHAR(100) PRIMARY KEY,
    transformation_group_id VARCHAR(100),
    
    -- Project and environment scoping
    project VARCHAR(100) NOT NULL,
    environment VARCHAR(20) NOT NULL DEFAULT 'qat',
    
    -- Layer and execution control
    layer VARCHAR(50) NOT NULL,
    step INT NOT NULL DEFAULT 1,
    enabled BIT NOT NULL DEFAULT 1,
    
    -- Generic entity hierarchy (domain-agnostic)
    entity_1 VARCHAR(100),  -- e.g., "plant" for manufacturing, "region" for retail
    entity_2 VARCHAR(100),  -- e.g., "asset" for manufacturing, "store" for retail
    entity_3 VARCHAR(100),  -- e.g., "equipment" for manufacturing, "department" for retail
    
    -- Transformation logic
    module VARCHAR(255) NOT NULL,
    function VARCHAR(255) NOT NULL,
    
    -- Inputs (flexible JSON array for multiple sources)
    inputs NVARCHAR(MAX),  -- JSON: ["table1", "table2", {"type": "query", "sql": "..."}]
    
    -- Constants/parameters (JSON object)
    constants NVARCHAR(MAX),  -- JSON: {"threshold": 100, "multiplier": 1.5}
    
    -- Outputs (JSON array for multiple targets)
    outputs NVARCHAR(MAX),  -- JSON: [{"table": "output1", "mode": "overwrite"}]
    
    -- Metadata
    description NVARCHAR(500),
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    
    -- Indexing for performance
    INDEX idx_project_env_layer (project, environment, layer),
    INDEX idx_enabled (enabled),
    INDEX idx_transformation_group (transformation_group_id)
);

-- ============================================================================
-- Example Records - Energy Efficiency Project
-- ============================================================================

-- Silver Layer - Processing individual assets
INSERT INTO TransformationRegistry (
    transformation_id,
    transformation_group_id,
    project,
    environment,
    layer,
    step,
    enabled,
    entity_1,
    entity_2,
    entity_3,
    module,
    function,
    inputs,
    constants,
    outputs,
    description
) VALUES (
    'energy-argo-boilers-silver',
    'energy-silver-assets',
    'energy efficiency',
    'qat',
    'Silver_1',
    1,
    1,
    'Argo',
    'Boiler 6,7,8,10',
    NULL,
    'silver.functions',
    'process_argo_boilers',
    '["qat_energy_efficiency.argo_boilers_bronze"]',
    '{}',
    '[{"table": "qat_energy_efficiency.argo_boilers_silver", "mode": "overwrite"}]',
    'Process Argo boilers data from bronze to silver'
);

-- Gold Layer - Combining multiple assets
INSERT INTO TransformationRegistry (
    transformation_id,
    transformation_group_id,
    project,
    environment,
    layer,
    step,
    enabled,
    entity_1,
    entity_2,
    entity_3,
    module,
    function,
    inputs,
    constants,
    outputs,
    description
) VALUES (
    'energy-combined-boilers-gold',
    'energy-gold-aggregations',
    'energy efficiency',
    'qat',
    'Gold_1',
    1,
    1,
    'combined',
    'Combined Boilers',
    NULL,
    'silver.functions',
    'process_combined_boilers',
    '["qat_energy_efficiency.argo_boilers_silver", "qat_energy_efficiency.cedar_rapids_boilers_silver", "qat_energy_efficiency.winston_salem_boilers_silver"]',
    '{"aggregation_level": "plant"}',
    '[{"table": "qat_energy_efficiency.combined_boilers_gold", "mode": "overwrite"}]',
    'Combine all boilers data across plants'
);

-- ============================================================================
-- Example Records - Customer Churn Project (Different Domain)
-- ============================================================================

INSERT INTO TransformationRegistry (
    transformation_id,
    transformation_group_id,
    project,
    environment,
    layer,
    step,
    enabled,
    entity_1,
    entity_2,
    entity_3,
    module,
    function,
    inputs,
    constants,
    outputs,
    description
) VALUES (
    'churn-features-silver',
    'churn-feature-engineering',
    'customer churn',
    'qat',
    'Silver_1',
    1,
    1,
    'North America',
    'Retail',
    'Online',
    'churn.features',
    'calculate_customer_features',
    '["qat_churn.customer_transactions_bronze", "qat_churn.customer_profile_bronze"]',
    '{"lookback_days": 90, "min_transactions": 5}',
    '[{"table": "qat_churn.customer_features_silver", "mode": "overwrite"}]',
    'Calculate customer behavior features for churn prediction'
);

-- ============================================================================
-- Migration Views (Optional - for backward compatibility)
-- ============================================================================

-- Create a view that mimics the old TransformationConfig structure
CREATE VIEW TransformationConfig_Legacy AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY transformation_id) AS id,
    project,
    entity_1 AS plant,
    entity_2 AS asset,
    module,
    function,
    NULL AS target_table,
    JSON_VALUE(inputs, '$[0]') AS input_table,
    enabled,
    environment AS env,
    created_at,
    updated_at,
    layer
FROM TransformationRegistry
WHERE environment IN ('qat', 'prod');
