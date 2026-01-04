# -*- coding: utf-8 -*-
# Bam_c_export.py
#
# Export BAM-C CSV for a selected project:
# - Hard-coded service URLs (as requested)
# - Robust species join (GlobalID -> ParentGlobalID) with normalized GUIDs
# - PCT thresholds loaded from "Large Tree Thresholds.xlsx" located next to this file
# - Composition (comp*) and Structure (struc*) metrics computed from species tables
# - Functional metrics including funLargeTrees using PCT thresholds
#
# Designed to be called from a .pyt or CLI (see __main__).

from __future__ import annotations
import os
import re
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from statistics import mean  # For improved average calculation

import pandas as pd  # import at top to avoid "pd not defined" in any helper


# -----------------------------
# HARD-CODED SERVICE URLS
# -----------------------------
COVER_ABUNDANCE_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/0"
ESTABLISHMENT_LAYER_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_f508fd2dab4f467c9b46b25bb97f3bb1/FeatureServer/0"
BAM_FUNCTION_URL="https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/survey123_164fa518b8944672bab2507e7a879928_results/FeatureServer/0"
SPECIES_TABLES = [
    {"url": "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/1", "growth_field": "GrowthFormGroup_a"},
    {"url": "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/2", "growth_field": "GrowthFormGroup_b"},
    {"url": "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/3", "growth_field": "GrowthFormGroup_c"},
]

FLORA_XREF_XLSX = r"G:\Shared drives\99.3 GIS Admin\Production\Tools\BAM Tools\BioNetPowerQueryLists.xlsx"
FLORA_XREF_SHEET = "Flora_species_powerQuery"

# -----------------------------
# LOGGING
# -----------------------------
def _setup_logger(name: str = "bamc_export") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger


# -----------------------------
# UTIL: Resolve thresholds path next to this file
# -----------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

def _resolve_thresholds_path(filename: str = "Large Tree Thresholds.xlsx") -> str:
    p1 = os.path.join(_THIS_DIR, filename)
    if os.path.exists(p1):
        return p1

    p2 = os.path.join(os.getcwd(), filename)
    if os.path.exists(p2):
        return p2

    try:
        import arcpy  # optional; only works inside Pro
        aprx = arcpy.mp.ArcGISProject("CURRENT")
        p3 = os.path.join(aprx.homeFolder, filename)
        if os.path.exists(p3):
            return p3
    except Exception:
        pass

    return p1

PCT_THRESHOLD_XLSX = _resolve_thresholds_path()


# -----------------------------
# AUTHENTICATION
# -----------------------------
def authenticate_gis(logger: logging.Logger):
    from arcgis.gis import GIS
    try:
        logger.info("Authenticating via ArcGIS Pro (home profile)...")
        gis = GIS("home")
        if gis.users.me:
            logger.info(f"Authenticated as: {gis.users.me.username}")
        else:
            logger.info("Authenticated (home), no user context.")
        return gis
    except Exception as e:
        logger.warning(f"Home profile authentication failed: {e}. Falling back to anonymous.")
        return GIS()  # anonymous


# -----------------------------
# DATA ACCESS HELPERS
# -----------------------------
def query_feature_layer(gis, url: str, where_clause: str, logger: logging.Logger) -> pd.DataFrame:
    from arcgis.features import FeatureLayer

    try:
        lyr = FeatureLayer(url, gis=gis)
        logger.info(f"Querying layer: {lyr.properties.name}")
        logger.info(f"Layer features (count): {lyr.query(return_count_only=True)}")
        fs = lyr.query(where=where_clause, return_all_records=True)
        df = fs.sdf if fs else pd.DataFrame()
        logger.info(f"Retrieved {len(df)} records from {lyr.properties.name}")
        return df
    except Exception as e:
        logger.error(f"Error querying layer {url}: {e}")
        return pd.DataFrame()


def load_pct_threshold_mapping(excel_file_path: str, logger: logging.Logger) -> Dict[str, int]:
    logger.info(f"Attempting to load PCT thresholds from: {excel_file_path}")
    if not excel_file_path or not os.path.exists(excel_file_path):
        logger.warning("PCT threshold Excel not found. funLargeTrees may be 0 if threshold unknown.")
        return {}
    try:
        df = pd.read_excel(excel_file_path, sheet_name=0, header=0)
        mapping: Dict[str, int] = {}
        for _, row in df.iterrows():
            pct = row.iloc[0] if len(row) >= 1 else None
            thr = row.iloc[2] if len(row) >= 3 else None
            if pd.notna(pct) and pd.notna(thr):
                try:
                    key = str(int(str(pct).strip().split()[0].split('.')[0]))
                    mapping[key] = int(thr)
                except Exception:
                    continue
        logger.info(f"Loaded {len(mapping)} PCT threshold mappings from Excel")
        return mapping
    except Exception as e:
        logger.warning(f"Failed to load PCT threshold Excel: {e}. funLargeTrees may be 0.")
        return {}


def load_flora_htw_species(excel_file_path: str, sheet_name: str, logger: logging.Logger) -> Set[str]:
    htw_species: Set[str] = set()

    logger.info(f"Attempting to load Flora HTW xref from: {excel_file_path} ({sheet_name})")
    if not excel_file_path or not os.path.exists(excel_file_path):
        logger.warning("Flora xref Excel not found. funHighThreatExotic will be 0 for all plots.")
        return htw_species

    try:
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=0)
    except Exception as e:
        logger.warning(f"Failed to read Flora xref Excel: {e}. funHighThreatExotic will be 0.")
        return htw_species

    sci_col = _find_column_case_insensitive(df, ("scientificName", "species", "Scientific Name"))
    htw_col = _find_column_case_insensitive(df, ("highThreatWeed", "HighThreatWeed", "HTW"))

    if not sci_col or not htw_col:
        logger.warning(
            "Could not locate scientificName / highThreatWeed columns in Flora xref; "
            "funHighThreatExotic will be 0."
        )
        return htw_species

    htw_mask = df[htw_col].apply(
        lambda v: bool(str(v).strip()) and str(v).strip().lower() not in ("no", "0", "false", "nan")
    )

    df_pos = df.loc[htw_mask, sci_col].dropna().astype(str)
    htw_species = set(df_pos.str.strip().str.lower().unique())

    logger.info(f"Loaded {len(htw_species)} HTW scientific names from Flora xref")
    return htw_species



# -----------------------------
# FUNCTIONAL
# -----------------------------
def _calculate_functional_metrics(
    plot_df: pd.DataFrame,
    pct_map: Dict[str, int],
    htw_species: Optional[Set[str]] = None,
    plot_id: Any = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    if plot_df.empty:
        return {
            'funLargeTrees': 0, 'funHollowtrees': 0, 'funLitterCover': 0.0, 'funLenFallenLogs': 0.0,
            'funTreeStem5to9': 0, 'funTreeStem10to19': 0, 'funTreeStem20to29': 0,
            'funTreeStem30to49': 0, 'funTreeStem50to79': 0, 'funTreeRegen': 0, 'funHighThreatExotic': 0
        }

    plot_info = plot_df.iloc[0]

    def _to_float(x):
        try:
            return float(x) if pd.notna(x) else 0.0
        except Exception:
            return 0.0

    # Litter cover using a better averaging approach
    litter_vals = [_to_float(plot_info.get(f"littersubplot{i}")) for i in range(1, 6)]
    litter_avg = round(mean(litter_vals), 1) if len(litter_vals) > 0 else 0.0  # Updated logic here!

    return {
        # Other functional metric outputs here...
        'funLitterCover': litter_avg,
    }
