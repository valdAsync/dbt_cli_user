import sqlite3


def create_db():
    conn = sqlite3.connect("dbt_manifest.db")
    c = conn.cursor()

    c.execute(
        """CREATE TABLE IF NOT EXISTS project (
            project_name TEXT PRIMARY KEY
        )
    """
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS node (
            id TEXT PRIMARY KEY,
            name TEXT,
            resource_type TEXT,
            project_name TEXT,
            FOREIGN KEY(project_name) REFERENCES project(project_name)
        )
    """
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS config (
            node_id TEXT,
            enabled BOOLEAN,
            materialized TEXT,
            incremental_strategy TEXT,
            on_schema_change TEXT,
            on_configuration_change TEXT,
            severity TEXT,
            FOREIGN KEY(node_id) REFERENCES node(id)
        )
    """
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS columns (
            node_id TEXT,
            column_name TEXT,
            data_type TEXT,
            FOREIGN KEY(node_id) REFERENCES node(id)
        )
    """
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS references (
            node_id TEXT,
            reference TEXT,
            FOREIGN KEY(node_id) REFERENCES node(id)
        )
    """
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS sources (
            node_id TEXT,
            schema TEXT,
            table TEXT,
            FOREIGN KEY(node_id) REFERENCES node(id)
        )
    """
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS dependencies (
            node_id TEXT,
            nodes TEXT,
            FOREIGN KEY(node_id) REFERENCES node(id)
        )
    """
    )
