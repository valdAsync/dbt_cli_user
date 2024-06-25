import sqlite3

from dbt_parser import Manifest


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


def insert_manifest_data(manifest: Manifest, db_path: str = "dbt_manifest.db"):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    try:
        c.execute(
            "INSERT OR REPLACE INTO project (project_name) VALUES (?)",
            (manifest.metadata.project_name,),
        )

        for node_id, node in manifest.nodes.items():
            c.execute(
                """
                INSERT OR REPLACE INTO node (id, name, resource_type, project_name)
                VALUES (?, ?, ?, ?)
            """,
                (
                    node_id,
                    node.name,
                    node.resource_type,
                    manifest.metadata.project_name,
                ),
            )

            c.execute(
                """
                INSERT OR REPLACE INTO config
                (node_id, enabled, materialized, incremental_strategy,
                on_schema_change, on_configuration_change, severity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    node_id,
                    node.configuration.enabled,
                    node.configuration.materialized,
                    node.configuration.incremental_strategy,
                    node.configuration.on_schema_change,
                    node.configuration.on_configuration_change,
                    node.configuration.severity,
                ),
            )

            for col_name, col in node.columns.items():
                c.execute(
                    """
                    INSERT OR REPLACE INTO node_columns (node_id, column_name, data_type)
                    VALUES (?, ?, ?)
                """,
                    (node_id, col_name, col.data_type),
                )

            for ref in node.refs:
                c.execute(
                    """
                    INSERT OR REPLACE INTO node_references (node_id, reference)
                    VALUES (?, ?)
                """,
                    (node_id, ref.name),
                )

            for schema, table in node.sources:
                c.execute(
                    """
                    INSERT OR REPLACE INTO node_sources (node_id, source_schema, source_table)
                    VALUES (?, ?, ?)
                """,
                    (node_id, schema, table),
                )

            for dep in node.dependencies.nodes:
                c.execute(
                    """
                    INSERT OR REPLACE INTO dependencies (node_id, dependency)
                    VALUES (?, ?)
                """,
                    (node_id, dep),
                )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")
    finally:
        conn.close()


def query_db():
    conn = sqlite3.connect("dbt_manifest.db")
    c = conn.cursor()

    # List all tables
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = c.fetchall()
    print("Tables in the database:")
    for table in tables:
        print(f"- {table[0]}")
    print()

    # Query each table
    for table in tables:
        table_name = table[0]
        print(f"Contents of {table_name}:")
        c.execute(f"SELECT * FROM {table_name} LIMIT 5;")
        rows = c.fetchall()
        if rows:
            # Get column names
            column_names = [description[0] for description in c.description]
            print("  Columns:", ", ".join(column_names))
            for row in rows:
                print("  ", row)
        else:
            print("  (Table is empty)")
        print()

    conn.close()
