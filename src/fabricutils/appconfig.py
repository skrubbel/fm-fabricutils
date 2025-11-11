"""Dataplatform configuration and relatet methods"""

from typing import Dict

import notebookutils
import json
import os
import sempy.fabric as fabric


def _read_lakehouse_mappings(file_path: str = None) -> Dict:
    """Read lakehouse mappings from lakehouse_mappings.json file

    Args:
        file_path (str, optional): Path to the lakehouse mappings JSON file.
            If None, defaults to lakehouse_mappings.json in the same directory as this module.

    Returns:
        Dict: Parsed JSON data as dictionary

    Raises:
        FileNotFoundError: If lakehouse_mappings.json file is not found
        json.JSONDecodeError: If file contains invalid JSON
    """

    try:
        if file_path is None:
            file_path = os.path.join(os.path.dirname(__file__), "lakehouse_mappings.json")

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"lakehouse_mappings.json file not found: {e}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in lakehouse_mappings.json: {e}", e.doc, e.pos)
    except Exception as e:
        raise Exception(f"Error reading lakehouse_mappings.json: {e}")


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
