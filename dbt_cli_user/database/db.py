import sqlite3


def create_db():
    conn = sqlite3.connect("dbt_manifest.db")
    c = conn.cursor()

    # Enable foreign key constraints
    c.execute("PRAGMA foreign_keys = ON")

    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS project (
            project_name TEXT PRIMARY KEY
        );
        
        CREATE TABLE IF NOT EXISTS node (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            resource_type TEXT NOT NULL,
            project_name TEXT,
            FOREIGN KEY(project_name) REFERENCES project(project_name)
        );
        
        CREATE TABLE IF NOT EXISTS config (
            node_id TEXT PRIMARY KEY,
            enabled BOOLEAN NOT NULL,
            materialized TEXT,
            incremental_strategy TEXT,
            on_schema_change TEXT,
            on_configuration_change TEXT,
            severity TEXT,
            FOREIGN KEY(node_id) REFERENCES node(id)
        );
        
        CREATE TABLE IF NOT EXISTS node_columns (
            node_id TEXT,
            column_name TEXT,
            data_type TEXT,
            PRIMARY KEY (node_id, column_name),
            FOREIGN KEY(node_id) REFERENCES node(id)
        );
        
        CREATE TABLE IF NOT EXISTS node_references (
            node_id TEXT,
            reference TEXT,
            PRIMARY KEY (node_id, reference),
            FOREIGN KEY(node_id) REFERENCES node(id)
        );
        
        CREATE TABLE IF NOT EXISTS node_sources (
            node_id TEXT,
            source_schema TEXT,
            source_table TEXT,
            PRIMARY KEY (node_id, source_schema, source_table),
            FOREIGN KEY(node_id) REFERENCES node(id)
        );
        
        CREATE TABLE IF NOT EXISTS dependencies (
            node_id TEXT,
            dependency TEXT,
            PRIMARY KEY (node_id, dependency),
            FOREIGN KEY(node_id) REFERENCES node(id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_node_project_name ON node(project_name);
        CREATE INDEX IF NOT EXISTS idx_config_node_id ON config(node_id);
        CREATE INDEX IF NOT EXISTS idx_node_columns_node_id ON node_columns(node_id);
        CREATE INDEX IF NOT EXISTS idx_node_references_node_id ON node_references(node_id);
        CREATE INDEX IF NOT EXISTS idx_node_sources_node_id ON node_sources(node_id);
        CREATE INDEX IF NOT EXISTS idx_dependencies_node_id ON dependencies(node_id);
    """
    )

    conn.commit()
    conn.close()
