import json
import os
from pathlib import Path
from typing import Dict

import typer
from dbt_parser import load_manifest
from dbt_projects_db import create_db, insert_manifest_data, query_db
from file_watcher import ProjectWatcher, start_watcher

app = typer.Typer()

CONFIG_FILE = Path.home() / "config.json"


def load_config() -> Dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        return {"projects": {}}


def save_config(config: Dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


def update_project(config: Dict, project_name: str):
    project_info = config["projects"][project_name]
    project_path = project_info["path"]
    manifest_path = os.path.join(project_path, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        typer.echo(f"Manifest file not found at {manifest_path}.")
        return

    try:
        manifest = load_manifest(manifest_path)
        insert_manifest_data(manifest)
        typer.echo(f"Manifest data for project '{project_name}' updated.")
    except Exception as e:
        typer.echo(f"Error updating manifest data: {e}")


def show_menu():
    typer.echo("DBT CLI Menu:")
    typer.echo("1. List dbt Projects")
    typer.echo("2. Add dbt Project")
    typer.echo("3. Remove dbt Project")
    typer.echo("4. Preview dbt Project")
    typer.echo("5. Quit")


@app.command()
def main():
    config = load_config()
    create_db()
    watcher = start_watcher(config)

    try:
        while True:
            show_menu()
            choice = typer.prompt("Enter your choice", type=int)

            if choice == 1:
                list_projects(config)
            elif choice == 2:
                add_project(config, watcher)
            elif choice == 3:
                remove_project(config, watcher)
            elif choice == 4:
                preview_project(config)
            elif choice == 5:
                typer.echo("Goodbye!")
                break
            else:
                typer.echo("Invalid choice")
    except KeyboardInterrupt:
        typer.echo("Shutting down!")
    finally:
        watcher.stop()


def list_projects(config: Dict):
    if not config["projects"]:
        typer.echo("No projects found.")
    else:
        for project in config["projects"]:
            typer.echo(project)


def add_project(config: Dict, watcher: ProjectWatcher):
    project_name = typer.prompt("Enter project name")
    project_path = typer.prompt("Enter project path")

    if not os.path.exists(project_path):
        typer.echo("Invalid project path")
        return

    manifest_path = os.path.join(project_path, "target", "manifest.json")
    if not os.path.exists(manifest_path):
        typer.echo(f"Manifest file not found at {manifest_path}")
        return

    try:
        manifest = load_manifest(manifest_path)
        dbt_project_name = manifest.metadata.project_name
        config["projects"][project_name] = {
            "path": project_path,
            "dbt_project_name": dbt_project_name,
        }
        save_config(config)
        watcher.add_project(project_name, config["projects"][project_name])
        typer.echo(f"Project '{project_name}' added successfully.")
        insert_manifest_data(manifest)
    except Exception as e:
        typer.echo(f"Error adding project: {e}")


def remove_project(config: Dict, watcher: ProjectWatcher):
    project_name = typer.prompt("Enter project name")

    if project_name in config["projects"]:
        watcher.remove_project(project_name)
        del config["projects"][project_name]
        save_config(config)
        typer.echo("Project removed.")
    else:
        typer.echo("Project not found.")


def preview_project(config: Dict):
    project_name = typer.prompt("Enter project name")

    if project_name in config["projects"]:
        update_project(config, project_name)
        dbt_project_name = config["projects"][project_name]["dbt_project_name"]
        query_db(dbt_project_name)
    else:
        typer.echo("Project not found.")


if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("Goodbye!")
