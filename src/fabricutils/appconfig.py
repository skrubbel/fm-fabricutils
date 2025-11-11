"""Dataplatform configuration and relatet methods"""

from typing import Dict

import notebookutils
import json
import os
import yaml
import sempy.fabric as fabric


def _read_lakehouse_mappings(file_path: str = None) -> Dict:
    """Read lakehouse mappings from lakehouse_mappings.yaml file

    Args:
        file_path (str, optional): Path to the lakehouse mappings YAML file.
            If None, defaults to lakehouse_mappings.yaml in the same directory as this module.

    Returns:
        Dict: Parsed YAML data as dictionary

    Raises:
        FileNotFoundError: If lakehouse_mappings.yaml file is not found
        yaml.YAMLError: If file contains invalid YAML
    """

    try:
        if file_path is None:
            file_path = os.path.join(os.path.dirname(__file__), "lakehouse_mappings.yaml")

        with open(file_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"lakehouse_mappings.yaml file not found: {e}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Invalid YAML in lakehouse_mappings.yaml: {e}")
    except Exception as e:
        raise Exception(f"Error reading lakehouse_mappings.yaml: {e}")


def get_current_lakehouse_name() -> str:
    return notebookutils.lakehouse.getWithProperties(fabric.get_lakehouse_id())["displayName"]


def get_current_workspace_name() -> str:
    return fabric.resolve_workspace_name(fabric.get_workspace_id())


def get_source_to_target_lakehouse_paths(
    default_lakehouse_name: str, default_workspace_name: str, mapping_context: str
) -> Dict[str, str]:
    """Return dict with source and target lakehouse named abfss paths

    Args:
        default_lakehouse_name (str): Name of default lakehouse (Executing context)
        default_workspace_name (str): Name of workspace containing default lakehouse
        mapping_context (str): Transformation context for source -> target mapping

    Returns:
        Dict[str: str]: Dict with source and target abfss paths
    """
    map_key = f"{default_workspace_name}|{default_lakehouse_name}|{mapping_context}"

    map_values = _read_lakehouse_mappings().get(map_key)

    source_lh_path = (
        f"abfss://{map_values['source_workspace']}"
        f"@onelake.dfs.fabric.microsoft.com/{map_values['source_lakehouse']}.Lakehouse/"
    )

    target_lh_path = (
        f"abfss://{map_values['target_workspace']}"
        f"@onelake.dfs.fabric.microsoft.com/{map_values['target_lakehouse']}.Lakehouse/"
    )

    return {"source_path": source_lh_path, "target_path": target_lh_path}
