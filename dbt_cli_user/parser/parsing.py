from typing import Optional

from pydantic import BaseModel, Field


class Config(BaseModel):
    enabled: bool
    materialized: Optional[str] = None
    incremental_strategy: Optional[str] = None
    on_schema_change: Optional[str] = None
    on_configuration_change: Optional[str] = None
    severity: Optional[str] = None


class Column(BaseModel):
    name: str
    data_type: Optional[str] = None


class DependsOn(BaseModel):
    nodes: list[str] = Field(default_factory=list)


class Ref(BaseModel):
    name: str


class Node(BaseModel):
    name: str
    resource_type: str
    dependencies: DependsOn = Field(
        default_factory=lambda: DependsOn(), alias="depends_on"
    )
    configuration: Config = Field(
        default_factory=lambda: Config(enabled=False), alias="config"
    )
    columns: dict[str, Column] = Field(default_factory=dict, alias="columns")
    refs: list[Ref] = Field(default_factory=list, alias="refs")
    sources: list[tuple[str, str]] = Field(default_factory=list, alias="sources")


class Metadata(BaseModel):
    project_name: str


class Manifest(BaseModel):
    nodes: dict[str, Node]
    metadata: Metadata


def load_manifest(file_path: str) -> Manifest:
    with open(file_path, "r", encoding="utf8") as file:
        json_data = file.read()
    return Manifest.model_validate_json(json_data)


try:
    manifest = load_manifest("/home/valcer/projects/dbt_cli_user/manifest.json")
    print(f"Project Name: {manifest.metadata.project_name}\n")
    for node_id, node in manifest.nodes.items():
        unique_sources = set(node.sources)
        print(f"Node ID: {node_id}")
        print(f"  Name: {node.name}")
        print(f"  Resource type: {node.resource_type}")
        print(f"  Dependencies: {node.dependencies}")
        print(f"  Configuration: {node.configuration.model_dump()}")

        if node.columns:
            for col_name, col_details in node.columns.items():
                print(f"    Column Name: {col_name}")
                print(f"    Data Type: {col_details.data_type}")
        else:
            print("    No columns defined")

        if node.refs:
            for ref in node.refs:
                print(f"    Reference: {ref.name}")
        else:
            print("    No references")

        if unique_sources:
            for schema, table in unique_sources:
                print(f"    Schema: {schema}, Table: {table}")
        else:
            print("    No sources")

        print("\n")
except Exception as e:
    print(f"Error: {e}")
