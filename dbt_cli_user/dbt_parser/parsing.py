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
