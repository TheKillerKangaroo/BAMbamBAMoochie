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
from statistics import mean

import pandas as pd  # import at top to avoid "pd not defined" in any helper


# -----------------------------
# HARD-CODED SERVICE URLS
# -----------------------------
COVER_ABUNDANCE_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/0"
ESTABLISHMENT_LAYER_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_f508fd2dab4f467c9b46b25bb97f3bb1/FeatureServer/0"
#BAM_FUNCTION_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_748874bb8cad415c91c7532a4d318e74/FeatureServer/0"
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
    # 1) Same folder as this file
    p1 = os.path.join(_THIS_DIR, filename)
    if os.path.exists(p1):
        return p1

    # 2) Current working directory
    p2 = os.path.join(os.getcwd(), filename)
    if os.path.exists(p2):
        return p2

    # 3) ArcGIS Pro project home (best-effort)
    try:
        import arcpy  # optional; only works inside Pro
        aprx = arcpy.mp.ArcGISProject("CURRENT")
        p3 = os.path.join(aprx.homeFolder, filename)
        if os.path.exists(p3):
            return p3
    except Exception:
        pass

    # 4) Fallback to file next to this script (even if missing; loader will warn)
    return p1

PCT_THRESHOLD_XLSX = _resolve_thresholds_path()


# -----------------------------
# AUTHENTICATION
# -----------------------------
def authenticate_gis(logger: logging.Logger):
    """
    Prefer the ArcGIS Pro signed-in profile (GIS('home')), otherwise anonymous.
    """
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
    """
    Query a FeatureLayer and return an SEDF (pandas DataFrame).
    """
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
    """
    Load PCT threshold mapping from Excel. Keys normalized to integer strings (e.g., '30','50','80').
    Non-fatal if file missing; return {} and continue (funLargeTrees may then be 0).
    """
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

def _find_column_case_insensitive(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    """
    Return the first column name in df whose lowercase version matches
    any of the provided candidate lowercase names.
    """
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        c = lower_map.get(cand.lower())
        if c:
            return c
    return None


def load_flora_htw_species(excel_file_path: str, sheet_name: str, logger: logging.Logger) -> Set[str]:
    """
    Load a set of scientific names that are marked as high threat weed
    in the BioNet flora xref spreadsheet (similar to MasterFlora.py).

    Returns a set of cleaned scientific names (lowercased + stripped).
    If anything goes wrong, returns an empty set.
    """
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

    # Find scientific name and HTW flag columns (case-insensitive)
    sci_col = _find_column_case_insensitive(df, ("scientificName", "species", "Scientific Name"))
    htw_col = _find_column_case_insensitive(df, ("highThreatWeed", "HighThreatWeed", "HTW"))

    if not sci_col or not htw_col:
        logger.warning(
            "Could not locate scientificName / highThreatWeed columns in Flora xref; "
            "funHighThreatExotic will be 0."
        )
        return htw_species

    # Positive HTW = any non-empty, non-'no' value in the HTW column
    htw_mask = df[htw_col].apply(
        lambda v: bool(str(v).strip()) and str(v).strip().lower() not in ("no", "0", "false", "nan")
    )

    df_pos = df.loc[htw_mask, sci_col].dropna().astype(str)
    htw_species = set(df_pos.str.strip().str.lower().unique())

    logger.info(f"Loaded {len(htw_species)} HTW scientific names from Flora xref")
    return htw_species



# -----------------------------
# METRIC HELPERS
# -----------------------------
def _present_flag_from_text(val) -> int:
    if val is None:
        return 0
    try:
        return 1 if str(val).strip().lower() == "yes" else 0
    except Exception:
        return 0


def _present_flag_from_count(val) -> int:
    try:
        num = float(val)
        return 1 if num > 0 else 0
    except (TypeError, ValueError):
        return 0

def _hte_flagged(val) -> bool:
    """
    True only when the text explicitly flags a High Threat Weed/Exotic.
    Examples: 'High Threat Weed - manageable', 'High Threat Weed - not manageable'
    """
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    s = str(val).strip().lower()
    if not s:
        return False
    # Accept both 'High Threat Weed' and 'High Threat Exotic' just in case schemas differ
    return bool(re.search(r"\bhigh\s*threat\s*(weed|exotic)\b", s))


def _hte_positive(val) -> bool:
    """
    Any non-null/non-empty value in a HighThreatExotic_* field counts as positive.
    """
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    if isinstance(val, str) and not val.strip():
        return False
    return True


# -----------------------------
# INTERNAL UTILITIES
# -----------------------------
def _find_join_field(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    for col in df.columns:
        if col.lower() in candidates:
            return col
    return None


def _first_case_insensitive(df: pd.DataFrame, name: str) -> Optional[str]:
    """Return the actual-cased column name matching `name` (case-insensitive)."""
    lname = name.lower()
    for c in df.columns:
        if c.lower() == lname:
            return c
    return None


def _norm_guid_series(s: pd.Series) -> pd.Series:
    """Normalize GUID strings: lower-case, strip braces and whitespace."""
    return (s.astype(str)
              .str.strip()
              .str.lower()
              .str.replace("{", "", regex=False)
              .str.replace("}", "", regex=False))


# -----------------------------
# COMPOSITION / STRUCTURE
# -----------------------------
def _calculate_composition_metrics(plot_species_df: pd.DataFrame) -> Dict[str, int]:
    """
    Counts per growth-form group:
      Tree / Shrub / Grass / Forbs / Ferns / Other
    Accepts GrowthFormGroup_a/_b/_c OR primarygrowthformgroup OR 'growth_form_field' hint.
    """
    if plot_species_df.empty:
        return {'Tree': 0, 'Shrub': 0, 'Grass': 0, 'Forbs': 0, 'Ferns': 0, 'Other': 0}

    counts = {'Tree': 0, 'Shrub': 0, 'Grass': 0, 'Forbs': 0, 'Ferns': 0, 'Other': 0}

    # Pre-resolve possible columns for speed
    gf_cols = {
        'a': _first_case_insensitive(plot_species_df, "GrowthFormGroup_a"),
        'b': _first_case_insensitive(plot_species_df, "GrowthFormGroup_b"),
        'c': _first_case_insensitive(plot_species_df, "GrowthFormGroup_c"),
        'primary': _first_case_insensitive(plot_species_df, "primarygrowthformgroup"),
    }

    for _, rec in plot_species_df.iterrows():
        gf_val = None

        # Priority: detected suffix columns, then primarygrowthformgroup, then hinted field
        for key in ('a', 'b', 'c', 'primary'):
            col = gf_cols.get(key)
            if col and pd.notna(rec.get(col)):
                gf_val = str(rec.get(col)).strip().lower()
                if gf_val:
                    break

        if gf_val is None or gf_val == '' or gf_val == 'nan':
            # try hinted field per-table if present
            hint = rec.get("growth_form_field")
            if hint and hint in plot_species_df.columns and pd.notna(rec.get(hint)):
                gf_val = str(rec.get(hint)).strip().lower()

        if not gf_val:
            continue

        if "tree" in gf_val:
            counts["Tree"] += 1
        elif "shrub" in gf_val or "bush" in gf_val:
            counts["Shrub"] += 1
        elif any(x in gf_val for x in ["grass", "graminoid", "sedge", "rush"]):
            counts["Grass"] += 1
        elif any(x in gf_val for x in ["forb", "herb", "wildflower"]):
            counts["Forbs"] += 1
        elif "fern" in gf_val or "pteridophyte" in gf_val:
            counts["Ferns"] += 1
        else:
            counts["Other"] += 1

    return counts


def _calculate_structure_metrics(plot_species_df: pd.DataFrame) -> Dict[str, float]:
    """
    Sums of cover by growth-form (strucTree/…/strucOther).
    Uses cover_a/_b/_c (case-insensitive); pairs GrowthFormGroup_* with the matching cover_*.
    """
    if plot_species_df.empty:
        return {'strucTree': 0.0, 'strucShrub': 0.0, 'strucGrass': 0.0, 'strucForbs': 0.0, 'strucFerns': 0.0, 'strucOther': 0.0}

    sums = {'strucTree': 0.0, 'strucShrub': 0.0, 'strucGrass': 0.0, 'strucForbs': 0.0, 'strucFerns': 0.0, 'strucOther': 0.0}

    gf_a = _first_case_insensitive(plot_species_df, "GrowthFormGroup_a")
    gf_b = _first_case_insensitive(plot_species_df, "GrowthFormGroup_b")
    gf_c = _first_case_insensitive(plot_species_df, "GrowthFormGroup_c")
    cov_a = _first_case_insensitive(plot_species_df, "cover_a")
    cov_b = _first_case_insensitive(plot_species_df, "cover_b")
    cov_c = _first_case_insensitive(plot_species_df, "cover_c")

    for _, rec in plot_species_df.iterrows():
        gf_val = None
        cover_val = 0.0

        if gf_a and pd.notna(rec.get(gf_a)):
            gf_val = str(rec.get(gf_a)).lower()
            cover_val = rec.get(cov_a, 0) if cov_a else 0
        elif gf_b and pd.notna(rec.get(gf_b)):
            gf_val = str(rec.get(gf_b)).lower()
            cover_val = rec.get(cov_b, 0) if cov_b else 0
        elif gf_c and pd.notna(rec.get(gf_c)):
            gf_val = str(rec.get(gf_c)).lower()
            cover_val = rec.get(cov_c, 0) if cov_c else 0
        else:
            continue

        try:
            cover_val = float(0 if pd.isna(cover_val) else cover_val)
        except Exception:
            cover_val = 0.0

        if not gf_val or cover_val == 0:
            continue

        if "tree" in gf_val:
            sums["strucTree"] += cover_val
        elif "shrub" in gf_val or "bush" in gf_val:
            sums["strucShrub"] += cover_val
        elif any(x in gf_val for x in ["grass", "graminoid", "sedge", "rush"]):
            sums["strucGrass"] += cover_val
        elif any(x in gf_val for x in ["forb", "herb", "wildflower"]):
            sums["strucForbs"] += cover_val
        elif "fern" in gf_val or "pteridophyte" in gf_val:
            sums["strucFerns"] += cover_val
        else:
            sums["strucOther"] += cover_val

    for k in list(sums.keys()):
        sums[k] = round(sums[k], 1)

    return sums


def _sum_cover_highthreatexotic(
    plot_species_df: pd.DataFrame,
    htw_species: Optional[Set[str]] = None,
    plot_id: Any = None,
    logger: Optional[logging.Logger] = None
) -> float:
    """
    Sum cover_a/_b/_c for rows where the *scientific name* is a High Threat Weed,
    based on an external BioNet flora xref list (htw_species).

    We ignore any HighThreatExotic_* fields in the tables and instead:
      - For stratum A: use upper_stratum_a + cover_a
      - For stratum B: use mid_stratum_b + cover_b
      - For stratum C: use lower_stratum_c + cover_c

    Emits debug logs showing which rows were included.
    """
    if plot_species_df.empty:
        return 0.0

    if logger is None:
        logger = _setup_logger()

    if not htw_species:
        logger.info("No HTW species set loaded; funHighThreatExotic will be 0 for all plots.")
        return 0.0

    cov_a = _first_case_insensitive(plot_species_df, "cover_a")
    cov_b = _first_case_insensitive(plot_species_df, "cover_b")
    cov_c = _first_case_insensitive(plot_species_df, "cover_c")

    sp_a = _first_case_insensitive(plot_species_df, "upper_stratum_a")
    sp_b = _first_case_insensitive(plot_species_df, "mid_stratum_b")
    sp_c = _first_case_insensitive(plot_species_df, "lower_stratum_c")

    total = 0.0
    debug_rows = []

    def _sum_for_suffix(cov_col: Optional[str], sp_col: Optional[str], suffix_label: str):
        nonlocal total, debug_rows
        if not cov_col or not sp_col:
            return
        if cov_col not in plot_species_df.columns or sp_col not in plot_species_df.columns:
            return

        species_series = plot_species_df[sp_col].astype(str).str.strip().str.lower()
        mask = species_series.isin(htw_species)
        if not mask.any():
            return

        vals = pd.to_numeric(plot_species_df.loc[mask, cov_col], errors="coerce").fillna(0.0)
        subtotal = float(vals.sum())
        total += subtotal

        # Collect debug info for this suffix
        cols = [c for c in [
            "bam_surv", sp_col, cov_col, "ScientificName", "CommonName", "source_table"
        ] if c in plot_species_df.columns]

        dbg = plot_species_df.loc[mask, cols].copy()
        dbg["__suffix__"] = suffix_label
        dbg["__subtotal__"] = subtotal
        debug_rows.append(dbg)

    # A, B, C suffix logic using species names from strata
    _sum_for_suffix(cov_a, sp_a, "A (upper_stratum_a)")
    _sum_for_suffix(cov_b, sp_b, "B (mid_stratum_b)")
    _sum_for_suffix(cov_c, sp_c, "C (lower_stratum_c)")

    if debug_rows:
        dbg_all = pd.concat(debug_rows, ignore_index=True)
        logger.info(
            "HTW (species-based) debug for plot %s:\n%s",
            str(plot_id),
            dbg_all.to_string(index=False)
        )
        logger.info(
            "Species-based HTW total cover for plot %s = %.1f",
            str(plot_id),
            round(total, 1)
        )
    else:
        logger.info("HTW (species-based) debug for plot %s: no species flagged as HTW", str(plot_id))

    return round(total, 1)





# -----------------------------
# FUNCTIONAL
# -----------------------------
def _calculate_large_trees(plot_info: pd.Series, pct_map: Dict[str, int]) -> int:
    """
    Uses normalized PCT (leading integer part) to look up threshold (30/50/80) and
    compute large-tree counts from size_1a/1b/1c accordingly.
    """
    pct_raw = plot_info.get("PCT", "")
    pct_str = str(pct_raw).strip()
    m = re.match(r"^\s*(\d+)", pct_str)
    pct_value = m.group(1) if m else ""

    threshold_size = pct_map.get(pct_value)
    if threshold_size is None:
        return 0

    def _to_int(x):
        try:
            return int(x) if pd.notna(x) else 0
        except Exception:
            return 0

    size_1a = _to_int(plot_info.get("size_1a"))
    size_1b = _to_int(plot_info.get("size_1b"))
    size_1c = _to_int(plot_info.get("size_1c"))

    if threshold_size == 30:
        return size_1a + size_1b + size_1c
    elif threshold_size == 50:
        return size_1b + size_1c
    elif threshold_size == 80:
        return size_1c
    return 0


def _calculate_functional_metrics(
    plot_df: pd.DataFrame,
    pct_map: Dict[str, int],
    htw_species: Optional[Set[str]] = None,
    plot_id: Any = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Computes:
      funLargeTrees, funHollowtrees, funLitterCover, funLenFallenLogs,
      funTreeStem5to9, funTreeStem10to19, funTreeStem20to29,
      funTreeStem30to49, funTreeStem50to79, funTreeRegen, funHighThreatExotic

    funHighThreatExotic is now based on an external HTW species list (htw_species),
    matched on scientific name per stratum.
    """
    if plot_df.empty:
        return {
            'funLargeTrees': 0, 'funHollowtrees': 0, 'funLitterCover': 0.0, 'funLenFallenLogs': 0.0,
            'funTreeStem5to9': 0, 'funTreeStem10to19': 0, 'funTreeStem20to29': 0,
            'funTreeStem30to49': 0, 'funTreeStem50to79': 0, 'funTreeRegen': 0, 'funHighThreatExotic': 0
        }

    plot_info = plot_df.iloc[0]

    large_trees = _calculate_large_trees(plot_info, pct_map)

    def _to_int(x):
        try:
            return int(x) if pd.notna(x) else 0
        except Exception:
            return 0

    def _to_float(x):
        try:
            return float(x) if pd.notna(x) else 0.0
        except Exception:
            return 0.0

    hollow = _to_int(plot_info.get("hbt_num"))
    log_len = _to_float(plot_info.get("log_length"))

    # Litter cover = average of littersubplot1..5
    litter_vals = [_to_float(plot_info.get(f"littersubplot{i}")) for i in range(1, 6)]
    litter_vals = [v for v in litter_vals if v is not None]
    litter_avg = round(mean(litter_vals), 1) if len(litter_vals) > 0 else 0.0

    funTreeStem5to9  = _present_flag_from_text(plot_info.get('five_nine'))
    funTreeStem10to19 = _present_flag_from_text(plot_info.get('ten_nineteen'))
    funTreeStem20to29 = _present_flag_from_text(plot_info.get('twenty_twentynine'))
    funTreeStem30to49 = _present_flag_from_count(plot_info.get('size_1a'))
    funTreeStem50to79 = _present_flag_from_count(plot_info.get('size_1b'))
    funTreeRegen      = _present_flag_from_text(plot_info.get('five'))

    funHighThreatExotic = _sum_cover_highthreatexotic(
        plot_df,
        htw_species=htw_species,
        plot_id=plot_id,
        logger=logger
    )

    return {
        'funLargeTrees': large_trees,
        'funHollowtrees': hollow,
        'funLitterCover': litter_avg,
        'funLenFallenLogs': round(log_len, 1),
        'funTreeStem5to9': funTreeStem5to9,
        'funTreeStem10to19': funTreeStem10to19,
        'funTreeStem20to29': funTreeStem20to29,
        'funTreeStem30to49': funTreeStem30to49,
        'funTreeStem50to79': funTreeStem50to79,
        'funTreeRegen': funTreeRegen,
        'funHighThreatExotic': funHighThreatExotic
    }

# -----------------------------
# MAIN EXPORT
# -----------------------------
def run_export(PROJECT_NUMBER: str, OUTPUT_CSV: str, logger: Optional[logging.Logger] = None) -> str:
    """
    Core export function:
      - Queries layers for the selected project,
      - Joins species (robust normalized GUIDs),
      - Computes comp/struc/function metrics,
      - Writes BAM-C CSV with two header rows.
    """
    logger = logger or _setup_logger()

    logger.info("Starting BAM CSV export...")
    if not PROJECT_NUMBER:
        raise ValueError("PROJECT_NUMBER is required.")
    if not OUTPUT_CSV:
        raise ValueError("OUTPUT_CSV is required.")

    # Ensure output folder exists
    out_dir = os.path.dirname(OUTPUT_CSV)
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # Thresholds (non-fatal if missing)
    pct_threshold_mapping = load_pct_threshold_mapping(PCT_THRESHOLD_XLSX, logger)

    # HTW species set from BioNet flora xref (non-fatal if missing)
    htw_species = load_flora_htw_species(FLORA_XREF_XLSX, FLORA_XREF_SHEET, logger)

    # Authenticate
    gis = authenticate_gis(logger)

    # 1) Establishment (by project)
    logger.info(f"Fetching establishment layer data for project: {PROJECT_NUMBER}...")
    df_est = query_feature_layer(gis, ESTABLISHMENT_LAYER_URL, f"aep_projnum = '{PROJECT_NUMBER}'", logger)
    if df_est.empty:
        raise RuntimeError(f"No establishment data found for project number: {PROJECT_NUMBER}")

    # Extract BAM IDs (establish best join key)
    est_bam_col = _find_join_field(df_est, ("bam", "bam_surv", "plot_id", "plotid"))
    if not est_bam_col:
        raise RuntimeError("Could not find BAM/Plot field in establishment layer.")
    bam_ids = df_est[est_bam_col].dropna().astype(str).unique()
    logger.info(f"Found {len(bam_ids)} BAM IDs for project {PROJECT_NUMBER}: {list(bam_ids)}")
    if len(bam_ids) == 0:
        raise RuntimeError("No valid BAM IDs found in establishment data")

    # Build BAM where clause
    if len(bam_ids) == 1:
        bam_where = f"bam_surv = '{bam_ids[0]}'"
    else:
        bam_where = "bam_surv IN ('" + "', '".join(bam_ids) + "')"
    logger.info(f"BAM filter clause: {bam_where}")

    # 2) Cover Abundance
    logger.info("Fetching cover abundance data for project BAM IDs...")
    df_cov = query_feature_layer(gis, COVER_ABUNDANCE_URL, bam_where, logger)
    if df_cov.empty:
        raise RuntimeError("No cover abundance data found for project BAM IDs")

    # 3) BAM Function (optional)
    logger.info("Fetching BAM function data for project BAM IDs...")
    df_fun = query_feature_layer(gis, BAM_FUNCTION_URL, bam_where, logger)
    if df_fun.empty:
        logger.warning("No BAM function data found for project BAM IDs")
        df_fun = pd.DataFrame()
    else:
        logger.info(f"Found {len(df_fun)} BAM function records")

    # 4) Species (GlobalID (cover) -> ParentGlobalID (species))
    logger.info("Fetching species data for project...")

    cov_gid_col = _first_case_insensitive(df_cov, "GlobalID")
    if not cov_gid_col:
        raise RuntimeError("No GlobalID field found in cover abundance data for filtering species tables")

    globalids = df_cov[cov_gid_col].dropna().astype(str).unique()
    logger.info(f"Found {len(globalids)} GlobalIDs to filter species tables")
    if len(globalids) == 0:
        raise RuntimeError("No valid GlobalIDs in cover abundance data")

    if len(globalids) == 1:
        gid_where = f"parentglobalid = '{globalids[0]}'"
    else:
        gid_where = "parentglobalid IN ('" + "', '".join(globalids) + "')"
    logger.info(f"Species filter clause: {gid_where[:140]}...")

    df_species_all: List[pd.DataFrame] = []
    for i, tbl in enumerate(SPECIES_TABLES):
        df_sp = query_feature_layer(gis, tbl["url"], gid_where, logger)
        if not df_sp.empty:
            parent_col = _first_case_insensitive(df_sp, "ParentGlobalID")
            if not parent_col:
                logger.warning(f"Species table {i} missing ParentGlobalID; skipping")
                continue
            df_sp["growth_form_field"] = tbl["growth_field"]
            df_sp["source_table"] = i
            df_species_all.append(df_sp)
            logger.info(f"Species table {i}: {len(df_sp)} records")

    df_species = pd.concat(df_species_all, ignore_index=True) if df_species_all else pd.DataFrame()
    if df_species.empty:
        logger.warning("No species records joined from tables A/B/C.")

    # 5) Establishment ↔ Cover Abundance join
    est_join = _find_join_field(df_est, ("bam", "plot_id", "plotid"))
    cov_join = _find_join_field(df_cov, ("bam_surv", "bam", "plot_id", "plotid"))
    if not est_join or not cov_join:
        raise RuntimeError("Could not find join fields between establishment and cover abundance layers")

    df_join = pd.merge(df_est, df_cov, left_on=est_join, right_on=cov_join, how="inner", suffixes=("_est", "_cov"))
    logger.info(f"After establishment-cover abundance join: {len(df_join)} records")

    # Join optional function data (left)
    if not df_fun.empty:
        fun_join = _find_join_field(df_fun, ("bam_surv", "bam", "plot_id", "plotid"))
        if fun_join:
            df_join = pd.merge(df_join, df_fun, left_on=est_join, right_on=fun_join, how="left", suffixes=("", "_func"))
            logger.info(f"After BAM function join: {len(df_join)} records")
        else:
            logger.warning("Could not find BAM field in function data to join; skipping function join")

    # 6) Species join (robust, normalized GUIDs)
    parent_globalid_field = _first_case_insensitive(df_species, "ParentGlobalID") if not df_species.empty else None

    if not parent_globalid_field or df_species.empty:
        logger.warning("Proceeding without species join (missing ParentGlobalID or no species data).")
        df_final = df_join
    else:
        # Determine how the cover GlobalID appears inside df_join after the previous merge
        # If both est and cov had GlobalID, the cover one will likely be 'GlobalID_cov'
        cov_gid_in_join = f"{cov_gid_col}_cov" if cov_gid_col in df_est.columns else cov_gid_col
        if cov_gid_in_join not in df_join.columns:
            # Fallback to any plausible variant
            cov_gid_in_join = _first_case_insensitive(df_join, "GlobalID_cov") or _first_case_insensitive(df_join, "GlobalID")

        if not cov_gid_in_join or cov_gid_in_join not in df_join.columns:
            logger.warning("Could not locate cover GlobalID column inside the joined frame; skipping species join.")
            df_final = df_join
        else:
            df_join["_gid_norm"] = _norm_guid_series(df_join[cov_gid_in_join])
            df_species["_parentgid_norm"] = _norm_guid_series(df_species[parent_globalid_field])

            left_keys = set(df_join["_gid_norm"].dropna().unique())
            right_keys = set(df_species["_parentgid_norm"].dropna().unique())
            logger.info(f"Species join keys: left={len(left_keys)} right={len(right_keys)} intersect={len(left_keys & right_keys)}")

            df_final = pd.merge(
                df_join, df_species,
                left_on="_gid_norm", right_on="_parentgid_norm",
                how="left", suffixes=("", "_sp")
            )
            logger.info(f"After species join (normalized GUIDs): {len(df_final)} records")

            df_final.drop(columns=["_gid_norm", "_parentgid_norm"], errors="ignore", inplace=True)

    # 7) Group by plot; compute metrics; write rows
    plot_key = est_join  # grouping key from establishment join field
    rows: List[Dict[str, Any]] = []

    for plot_id, group in df_final.groupby(plot_key):
        plot_info = group.iloc[0]
        comp = _calculate_composition_metrics(group)
        stru = _calculate_structure_metrics(group)
        func = _calculate_functional_metrics(
            group,
            pct_threshold_mapping,
            htw_species=htw_species,
            plot_id=plot_id,
            logger=logger
        )

        # Use plot_num (if present) as the numeric sort key
        plot_num_val = plot_info.get('plot_num', plot_id)

        # Useful diagnostics:
        logger.info(f"Plot {plot_id}: comp={comp}  struc={stru}")

        row = {
            'plot': str(plot_num_val)[:10],
            'pct': plot_info.get('PCT', ''),
            'area': plot_info.get('Vegarea', ''),
            'patchsize': plot_info.get('patchsize', ''),
            'conditionclass': (str(plot_info.get('ConditionClass', ''))[:20]
                               if pd.notna(plot_info.get('ConditionClass', '')) else ''),
            'zone': plot_info.get('startzone', ''),
            'easting': plot_info.get('starteasting', ''),
            'northing': plot_info.get('startnorthing', ''),
            'bearing': plot_info.get('compass_bearing', ''),
            'compTree': comp.get('Tree', 0),
            'compShrub': comp.get('Shrub', 0),
            'compGrass': comp.get('Grass', 0),
            'compForbs': comp.get('Forbs', 0),
            'compFerns': comp.get('Ferns', 0),
            'compOther': comp.get('Other', 0),
            **stru,
            **func,
            # hidden helper for numeric sorting
            '__plot_sort__': plot_num_val
        }
        rows.append(row)

    if not rows:
        raise RuntimeError("No output rows generated; check joins and filters.")

    out_df = pd.DataFrame(rows)

    # Sort by numeric plot_num (via __plot_sort__) if available
    if '__plot_sort__' in out_df.columns:
        out_df['__plot_sort__'] = pd.to_numeric(out_df['__plot_sort__'], errors='coerce')
        if out_df['__plot_sort__'].notna().any():
            out_df = out_df.sort_values('__plot_sort__')
            logger.info("Sorted output rows by plot_num ascending.")
        else:
            logger.warning("plot_num could not be interpreted as numbers; leaving original order.")
        out_df = out_df.drop(columns=['__plot_sort__'])

    # 8) Write CSV with two header rows (field names + format specs)
    headers = [
        'plot', 'pct', 'area', 'patchsize', 'conditionclass', 'zone', 'easting', 'northing', 'bearing',
        'compTree', 'compShrub', 'compGrass', 'compForbs', 'compFerns', 'compOther',
        'strucTree', 'strucShrub', 'strucGrass', 'strucForbs', 'strucFerns', 'strucOther',
        'funLargeTrees', 'funHollowtrees', 'funLitterCover', 'funLenFallenLogs',
        'funTreeStem5to9', 'funTreeStem10to19', 'funTreeStem20to29', 'funTreeStem30to49',
        'funTreeStem50to79', 'funTreeRegen', 'funHighThreatExotic'
    ]
    format_specs = [
        'Text[Maximum 10 characters]', 'Number', 'Number with 2 decimal point', 'Number',
        'Text[Letters, numbers, underscores and hyphens] Please fill condition-class name in all plots [Maximum 20 characters]',
        '[54 or 55 or 56]', 'Range in [0-359]', 'Number', 'Number', 'Number', 'Number', 'Number', 'Number',
        'Number with 1 decimal point', 'Number with 1 decimal point', 'Number with 1 decimal point',
        'Number with 1 decimal point', 'Number with 1 decimal point', 'Number with 1 decimal point',
        'Number', 'Number', 'Number with 1 decimal point', 'Number with 1 decimal point',
        '[0-1]', '[0-1]', '[0-1]', '[0-1]', '[0-1]', '[0-1]', 'Number with 1 decimal point'
    ]

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(headers) + ",,\n")
        f.write(",".join(format_specs) + "\n")
        for _, r in out_df.iterrows():
            f.write(",".join([str(r.get(col, "")) if r.get(col) is not None else "" for col in headers]) + "\n")

    logger.info(f"CSV export completed: {OUTPUT_CSV} ({len(rows)} data rows)")
    return OUTPUT_CSV


# -----------------------------
# CLI ENTRY (optional)
# -----------------------------
if __name__ == "__main__":
    import argparse
    _log = _setup_logger()
    parser = argparse.ArgumentParser(description="Export BAM-C CSV for a project number.")
    parser.add_argument("project_number", help="Project number (e.g., 9876)")
    parser.add_argument("output_csv", help="Output CSV path")
    args = parser.parse_args()
    run_export(args.project_number, args.output_csv, logger=_log)
