import json
import os
import time

from dbt_parser import load_manifest
from dbt_projects_db import insert_manifest_data
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class ManifestHandler(FileSystemEventHandler):
    def __init__(self, config):
        self.config = config

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith("manifest.json"):
            for project_name, project_info in self.config["projects"].items():
                if project_info["path"] in event.src_path:
                    print(
                        f"Detected change in manifest file for project: {project_name}"
                    )
                    self.process_manifest(event.src_path, project_name)
                    break

    def process_manifest(self, file_path, project_name, max_retries=5, delay=1):
        for attempt in range(max_retries):
            try:
                time.sleep(delay)  # Wait before attempting to read the file

                manifest = load_manifest(file_path)
                insert_manifest_data(manifest)
                print(f"Manifest data inserted for project: {project_name}")
                return
            except json.JSONDecodeError as e:
                print(f"Invalid JSON (attempt {attempt + 1}/{max_retries}): {e}")
            except Exception as e:
                print(
                    f"Error processing manifest (attempt {attempt + 1}/{max_retries}): {e}"
                )

            delay *= 2  # Exponential backoff

        print(
            f"Failed to process manifest for project: {project_name} after {max_retries} attempts"
        )


class ProjectWatcher:
    def __init__(self, config):
        self.config = config
        self.observer = Observer()
        self.handler = ManifestHandler(config)
        self.watch_descriptors = {}

    def start(self):
        for project_name, project_info in self.config["projects"].items():
            self.add_project(project_name, project_info)
        self.observer.start()
        print("Watching projects for changes")

    def add_project(self, project_name, project_info):
        project_path = os.path.join(project_info["path"], "target")
        watch = self.observer.schedule(self.handler, project_path, recursive=False)
        self.watch_descriptors[project_name] = watch
        print(f"Started watching project: {project_name}")

    def remove_project(self, project_name):
        if project_name in self.watch_descriptors:
            self.observer.unschedule(self.watch_descriptors[project_name])
            del self.watch_descriptors[project_name]
            print(f"Stopped watching project: {project_name}")

    def run(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        finally:
            print("Watcher stopped.")

    def stop(self):
        self.observer.stop()
        self.observer.join()


def start_watcher(config) -> ProjectWatcher:
    watcher = ProjectWatcher(config)
    watcher.start()
    return watcher
