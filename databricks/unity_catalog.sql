-- Unity Catalog namespace for the certified metrics framework.
CREATE CATALOG IF NOT EXISTS cmf;
CREATE SCHEMA IF NOT EXISTS cmf.analytics;

-- Governance: the metric registry is the contract; restrict who can alter definitions.
GRANT USE CATALOG ON CATALOG cmf TO `analytics-readers`;
GRANT CREATE, MODIFY ON SCHEMA cmf.analytics TO `analytics-engineers`;
