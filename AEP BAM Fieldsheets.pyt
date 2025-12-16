# -*- coding: utf-8 -*-
# BAM_Field_Sheets.pyt
#
# Generates AEP BAM Field Sheets (multi-site DOCX) for a selected project number.
# - Project numbers read from Establishment hosted feature layer.
# - End photos + end coordinates from related End Point layer (/1) via parentglobalid.
# - All service URLs + template path are hardcoded in CONFIG.
# - Output DOCX is a parameter with a sensible default in the user's Documents folder.
# - Robust diagnostics for species join & per-plot selection.
# - Photo auto-rotation (EXIF) and orientation-based sizing.

import arcpy
import os
import tempfile
from datetime import datetime
import pandas as pd
from docxtpl import DocxTemplate, InlineImage
from docx import Document
from docxcompose.composer import Composer
from docx.shared import Mm
from PIL import Image, ImageOps  # EXIF auto-rotation

# ---- CONFIG (hardcoded) ------------------------------------------------------
CONFIG = {
    # Template (docx)
    "SITE_TPL": r"G:\Shared drives\99.3 GIS Admin\Development-Testing\Tools\GitHub\BAM_field_sheets\AEP Field Sheet.docx",

    # Hosted feature layers
    "ESTABLISHMENT_LAYER_URL":      "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_f508fd2dab4f467c9b46b25bb97f3bb1/FeatureServer/0",
    "ENDPOINT_LAYER_URL":           "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_f508fd2dab4f467c9b46b25bb97f3bb1/FeatureServer/1",
    "COVER_ABUNDANCE_LAYER_URL":    "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/0",
    #"STRUCTURE_FUNCTION_LAYER_URL": "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_748874bb8cad415c91c7532a4d318e74/FeatureServer/0",
    "STRUCTURE_FUNCTION_LAYER_URL": "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/survey123_164fa518b8944672bab2507e7a879928_results/FeatureServer/0",
    # Species tables (A/B/C)
    "SPECIES_TABLE_A_URL":          "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/1",
    "SPECIES_TABLE_B_URL":          "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/2",
    "SPECIES_TABLE_C_URL":          "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/3",

    # Photo detection & layout
    "PHOTO_START_PATTERNS": ["start", "begin", "upstream"],
    "PHOTO_END_PATTERNS":   ["end", "finish", "downstream"],  # retained for future filename use
    "PHOTO_MAX_START": 2,
    "PHOTO_MAX_END":   2,
    "PHOTO_GRID_COLS": 2,

    # Orientation-aware sizing + EXIF auto-rotate
    "PHOTO_MAX_WIDTH_MM": 80,    # landscape width
    "PHOTO_MAX_HEIGHT_MM": 100,  # portrait height
    "AUTO_ORIENT_PHOTOS": True,  # apply EXIF orientation
}

# ---- Null-safe helpers -------------------------------------------------------
def _nz_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.strip() == "":
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        return int(round(float(x)))
    except Exception:
        return default

def _nz_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.strip() == "":
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

def _nz_str(x, default=""):
    try:
        return default if x is None else str(x)
    except Exception:
        return default

# ---- General helpers ---------------------------------------------------------

def _present_absent_to_yesno(x, default_no=False):
    """Map 'Present'/'Absent' (case-insensitive) to 'Yes'/'No'."""
    #arcpy.AddMessage(f"Stem present/absent: {x}")
    try:
        if isinstance(x, str):
            v = x.strip().lower()
            if v == "yes": return "Yes"
            if v == "no":  return "No"
        if isinstance(x, bool):
            return "Yes" if x else "No"
    except Exception:
        pass
    return "No" if default_no else ""

def _make_feature_layer(url, name):
    arcpy.management.MakeFeatureLayer(url, name)
    return name

def _make_table_view(url, name):
    arcpy.management.MakeTableView(url, name)
    return name

def _project_wgs84_to_gda94_mga(lon, lat):
    zone = int((lon + 180) / 6) + 1
    if not (49 <= zone <= 56):
        raise ValueError(f"MGA zone {zone} outside AU range (49–56) for lon={lon}")
    sr_wgs84 = arcpy.SpatialReference(4326)
    sr_mga   = arcpy.SpatialReference(28300 + zone)
    pt_wgs   = arcpy.PointGeometry(arcpy.Point(lon, lat), sr_wgs84)
    pt_mga   = pt_wgs.projectAs(sr_mga)
    c = pt_mga.centroid
    return {"zone": zone, "epsg": 28300 + zone, "easting": round(c.X, 0), "northing": round(c.Y, 0)}

def _list_unique_projects(est_url, project_field="aep_projnum"):
    lyr = _make_feature_layer(est_url, "est_proj_lyr")
    vals = set()
    with arcpy.da.SearchCursor(lyr, [project_field]) as rows:
        for (v,) in rows:
            if v is None:
                continue
            vals.add(str(v))
    return sorted(vals, key=lambda x: (len(x), x))

def _hosted_to_df(url_or_view, fields="*"):
    if fields == "*":
        fields = [f.name for f in arcpy.ListFields(url_or_view) if f.type not in ("Geometry", "OID")]
    if "OBJECTID" not in fields:
        fields = list(fields) + ["OBJECTID"]
    out = []
    with arcpy.da.SearchCursor(url_or_view, fields) as cur:
        for row in cur:
            out.append(dict(zip(fields, row)))
    return pd.DataFrame(out)


def _standardize_species(df, stratum, species_col, cover_col, ab_col, unknown_col=None):
    """
    Standardise species DF to a common schema. If unknown_col is provided and exists,
    it is renamed to 'unknown_text'. Otherwise an empty 'unknown_text' column is added.
    """
    if df.empty:
        return pd.DataFrame(columns=["parentglobalid", "species_name", "cover", "ab", "unknown_text", "stratum"])
    # Map case-insensitively
    cols = {c.lower(): c for c in df.columns}
    species_col = cols.get(str(species_col).lower(), species_col)
    cover_col   = cols.get(str(cover_col).lower(),   cover_col)
    ab_col      = cols.get(str(ab_col).lower(),      ab_col)
    unk_src     = None
    if unknown_col is not None:
        unk_src = cols.get(str(unknown_col).lower())
    # ParentGlobalID normalisation
    if "parentglobalid" not in df.columns:
        for cand in ("ParentGlobalID", "PARENTGLOBALID", "parentguid", "ParentGUID", "parent_id"):
            if cand in df.columns:
                df = df.rename(columns={cand: "parentglobalid"})
                break
    # Rename known cols
    rename_map = {species_col: "species_name", cover_col: "cover", ab_col: "ab"}
    if unk_src:
        rename_map[unk_src] = "unknown_text"
    df = df.rename(columns=rename_map)
    if "unknown_text" not in df.columns:
        df["unknown_text"] = ""
    df["stratum"] = stratum
    keep = ["parentglobalid", "species_name", "cover", "ab", "unknown_text", "stratum"]
    return df[[c for c in keep if c in df.columns]]


def _species_group(sub):
    if sub is None or sub.empty:
        return []
    rows = []
    for _, r in sub.iterrows():
        name = _nz_str(r.get("species_name", ""))
        unk  = _nz_str(r.get("unknown_text", "")).strip()
        # Append annotation only for Unknown placeholders (A/B/C)
        if unk and "unknown" in name.lower():
            name = f"{name} ({unk})"
        rows.append({
            "name":  name,
            "cover": round(_nz_float(r.get("cover"), 0.0), 1),
            "ab":    _nz_int(r.get("ab"), 0),
        })
    return rows

def _norm_guid_series(s):
    if s is None or len(s) == 0:
        return pd.Series(dtype="object")
    return (
        s.astype(str)
         .str.strip()
         .str.replace("{", "", regex=False)
         .str.replace("}", "", regex=False)
         .str.upper()
    )

# ---- Photo helpers (export, classify, grid, EXIF rotation) -------------------
def _download_attachments_for_selection(layer_name, where_clause, out_dir):
    arcpy.management.SelectLayerByAttribute(layer_name, "NEW_SELECTION", where_clause)
    os.makedirs(out_dir, exist_ok=True)
    try:
        arcpy.management.ExportAttachments(layer_name, out_dir)
    except Exception as ex1:
        last_err = ex1
        for opt in ("USE_ORIGINAL", "Use Original Filenames"):
            try:
                arcpy.management.ExportAttachments(layer_name, out_dir, None, opt, None)
                last_err = None
                break
            except Exception as ex2:
                last_err = ex2
        if last_err is not None:
            try:
                arcpy.management.ExportAttachments(layer_name, out_dir, None)
                last_err = None
            except Exception as ex3:
                last_err = ex3
        if last_err is not None:
            arcpy.AddWarning(
                f"ExportAttachments failed on {layer_name}. Tried multiple signatures. Last error: {last_err}"
            )
            return []
    files = []
    for root, _, fnames in os.walk(out_dir):
        for fn in fnames:
            if fn.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tif", ".tiff")):
                files.append(os.path.join(root, fn))
    if not files:
        arcpy.AddWarning("No image attachments were exported. Check attachments are present on the selected feature.")
    return files

def _score_filename(name_lower, patterns, plotid=None):
    score = 0
    for p in patterns:
        if p in name_lower:
            score += 2
    if plotid and plotid in name_lower:
        score += 1
    return score

def _classify_photos(files, start_patterns, end_patterns, plotid=None, max_start=None, max_end=None):
    if not files:
        return [], []
    spats = [s.lower() for s in start_patterns]
    epats = [e.lower() for e in end_patterns]
    pid   = str(plotid).lower() if plotid is not None else None
    scored = []
    for f in files:
        nlow = os.path.basename(f).lower()
        s_score = _score_filename(nlow, spats, pid)
        e_score = _score_filename(nlow, epats, pid)
        scored.append((f, s_score, e_score, nlow))
    # exclude non-matching files entirely (e.g., weather photos)
    scored = [t for t in scored if (t[1] > 0 or t[2] > 0)]
    start_files = [f for (f, s, e, n) in sorted(scored, key=lambda t: (t[1]-t[2], t[1], t[3]), reverse=True) if s >= e]
    end_files   = [f for (f, s, e, n) in sorted(scored, key=lambda t: (t[2]-t[1], t[2], t[3]), reverse=True) if e > s]
    start_set = set(start_files)
    end_files = [f for f in end_files if f not in start_set]
    if isinstance(max_start, int) and max_start > 0:
        start_files = start_files[:max_start]
    if isinstance(max_end, int) and max_end > 0:
        end_files = end_files[:max_end]
    return start_files, end_files

def _chunk_rows(items, cols):
    cols = max(1, int(cols))
    if not items:
        return []
    rows, row = [], []
    for img in items:
        row.append(img)
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows

def _prep_photo(in_path, tmp_dir, max_w_mm, max_h_mm, auto_orient=True):
    """Returns (out_path, is_portrait) with EXIF orientation applied."""
    try:
        os.makedirs(tmp_dir, exist_ok=True)
        with Image.open(in_path) as im:
            if auto_orient:
                im = ImageOps.exif_transpose(im)
            is_portrait = im.height >= im.width
            out_path = os.path.join(tmpfile_dir := tmp_dir, f"prep_{os.path.basename(in_path)}.jpg")
            im.convert("RGB").save(out_path, "JPEG", quality=90, optimize=True)
            return out_path, is_portrait
    except Exception as ex:
        arcpy.AddWarning(f"Photo prep failed for '{in_path}': {ex}")
        return in_path, None

# ---- Toolbox -----------------------------------------------------------------
class Toolbox(object):
    def __init__(self):
        self.label = "BAM Field Sheets"
        self.alias = "bam_fieldsheets"
        self.tools = [GenerateBAMFieldSheets]

class GenerateBAMFieldSheets(object):
    def __init__(self):
        self.label = "Generate AEP BAM Field Sheets"
        self.description = "Builds a multi-site DOCX using the AEP Field Sheet template for the selected project number."
        self.canRunInBackground = False

    def getParameterInfo(self):
        p_project = arcpy.Parameter(
            displayName="Project Number",
            name="project_number",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )
        p_project.filter.type = "ValueList"

        p_outdoc = arcpy.Parameter(
            displayName="Output Word Document",
            name="output_doc",
            datatype="DEFile",
            parameterType="Required",
            direction="Output"
        )
        p_outdoc.filter.list = ["docx"]

        return [p_project, p_outdoc]

    def updateParameters(self, parameters):
        if not parameters[0].filter.list:
            try:
                projects = _list_unique_projects(CONFIG["ESTABLISHMENT_LAYER_URL"], "aep_projnum")
                parameters[0].filter.list = projects
                if not parameters[0].value and projects:
                    parameters[0].value = projects[0]
            except Exception as ex:
                arcpy.AddWarning(f"Could not read project numbers: {ex}")

        if not parameters[1].altered:
            user_docs = os.path.join(os.path.expanduser("~"), "Documents")
            os.makedirs(user_docs, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            default_path = os.path.join(user_docs, f"Bam fieldsheet_{timestamp}.docx")
            parameters[1].value = default_path
        return

    def execute(self, parameters, messages):
        project_number = str(parameters[0].valueAsText).strip()
        out_doc = parameters[1].valueAsText

        if not project_number:
            raise arcpy.ExecuteError("Project Number is required.")
        if not out_doc.lower().endswith(".docx"):
            raise arcpy.ExecuteError("Output document must be a .docx file")

        site_tpl = CONFIG["SITE_TPL"]
        if not os.path.exists(site_tpl):
            raise arcpy.ExecuteError(f"Template not found: {site_tpl}")

        out_dir = os.path.dirname(out_doc) or os.getcwd()
        os.makedirs(out_dir, exist_ok=True)

        arcpy.AddMessage(f"Template: {site_tpl}")
        arcpy.AddMessage(f"Output: {out_doc}")
        arcpy.AddMessage(f"Project: {project_number}")

        # ---- Establishment (include globalid) ----------------------------------
        est_lyr = _make_feature_layer(CONFIG["ESTABLISHMENT_LAYER_URL"], "bam_establishment_lyr")
        est_fields = [
            "OBJECTID", "globalid", "aep_projnum", "bam", "plot_num", "survey_start",
            "grp_assessor1", "grp_assessor2", "grp_assessor3",
            "compass_bearing", "SHAPE@XY"
        ]
        where_est = f"aep_projnum = '{project_number}'"

        plot_rows = []
        with arcpy.da.SearchCursor(est_lyr, est_fields, where_clause=where_est) as cur:
            for (
                    oid, est_gid, aep_projnum, bam_surv, plotid, surveydate,
                    obs1, obs2, obs3,
                    bearing, (x, y)
            ) in cur:
                # Build observers as a comma-separated list, skipping blanks
                parts = [obs1, obs2, obs3]
                observers_str = ", ".join([str(p).strip() for p in parts if p and str(p).strip()])

                proj = _project_wgs84_to_gda94_mga(x, y)
                plot_rows.append({
                    "oid": _nz_int(oid),
                    "est_globalid_raw": _nz_str(est_gid),
                    "projnum": _nz_str(aep_projnum),
                    "bam_surv": bam_surv,
                    "plotid": _nz_str(plotid),
                    "surveydate": surveydate,
                    "observers": observers_str,
                    "bearing": _nz_int(bearing, 0),
                    "easting": _nz_int(proj["easting"]),
                    "northing": _nz_int(proj["northing"]),
                    "zone": _nz_int(proj["zone"])
                })

        if not plot_rows:
            raise arcpy.ExecuteError(f"No plots found for project {project_number}.")
        df_plots = pd.DataFrame(plot_rows)
        arcpy.AddMessage(f"Establishment plots: {len(df_plots)}")
        if "plotid" in df_plots.columns:
            try:
                df_plots["_plot_sort"] = pd.to_numeric(df_plots["plotid"], errors="coerce")
                # If any valid numbers exist, sort by them; otherwise leave as-is
                if df_plots["_plot_sort"].notna().any():
                    df_plots.sort_values(by="_plot_sort", inplace=True)
                    df_plots.reset_index(drop=True, inplace=True)
                    arcpy.AddMessage("Plots sorted by plot number (plot_num / plotid) ascending.")
                else:
                    arcpy.AddWarning("Could not interpret plotid values as numbers; leaving original order.")
                df_plots.drop(columns=["_plot_sort"], inplace=True)
            except Exception as ex:
                arcpy.AddWarning(f"Failed to sort plots by plot number; using original order. Details: {ex}")


        # ---- Cover & Abundance (diagnostics + robust filtering) ----------------
        cov_lyr = _make_feature_layer(CONFIG["COVER_ABUNDANCE_LAYER_URL"], "bam_coverab_lyr")
        cov_fields_actual = [f.name for f in arcpy.ListFields(cov_lyr)]
        fld_globalid = next((n for n in cov_fields_actual if n.lower() == "globalid"), "globalid")
        fld_projnum = next((n for n in cov_fields_actual if n.lower() == "aep_projnum"), "aep_projnum")
        fld_bamsurv = next((n for n in cov_fields_actual if n.lower() == "bam_surv"), "bam_surv")

        df_cover = _hosted_to_df(cov_lyr, [fld_bamsurv, fld_globalid, fld_projnum])
        arcpy.AddMessage(f"Cover rows total: {len(df_cover)}; fields: {list(df_cover.columns)}")

        project_number_norm = str(project_number).strip()
        if not df_cover.empty and fld_projnum in df_cover.columns:
            df_cover["aep_projnum_norm"] = df_cover[fld_projnum].astype(str).str.strip()
            sample_proj_counts = df_cover["aep_projnum_norm"].value_counts(dropna=False).head(10).to_dict()
            arcpy.AddMessage(f"Cover unique aep_projnum_norm (top 10): {sample_proj_counts}")
            df_cover_proj = df_cover[df_cover["aep_projnum_norm"] == project_number_norm]
        else:
            df_cover_proj = pd.DataFrame(columns=df_cover.columns)
        arcpy.AddMessage(f"Cover rows after aep_projnum filter: {len(df_cover_proj)}")

        if len(df_cover_proj) == 0:
            est_bams = set(df_plots["bam_surv"].astype(str).tolist())
            if not df_cover.empty and fld_bamsurv in df_cover.columns:
                df_cover["bam_surv_norm"] = df_cover[fld_bamsurv].astype(str).str.trim() if hasattr(str, 'trim') else df_cover[fld_bamsurv].astype(str).str.strip()
                arcpy.AddMessage(f"Establishment bam_surv keys: {sorted(list(est_bams))[:10]}{' ...' if len(est_bams)>10 else ''}")
                df_cover_proj = df_cover[df_cover["bam_surv_norm"].isin(est_bams)]
                arcpy.AddMessage(f"Cover rows after bam_surv fallback filter: {len(df_cover_proj)}")
            else:
                arcpy.AddWarning("Composition and structure layer is missing bam_surv field; cannot use fallback filter.")
        df_cover = df_cover_proj.copy()

        if not df_cover.empty:
            df_cover["globalid_norm"] = (
                df_cover[fld_globalid].astype(str).str.strip()
                .str.replace("{", "", regex=False)
                .str.replace("}", "", regex=False)
                .str.upper()
            )
            if "bam_surv_norm" not in df_cover.columns and fld_bamsurv in df_cover.columns:
                df_cover["bam_surv_norm"] = df_cover[fld_bamsurv].astype(str).str.strip()
        arcpy.AddMessage(f"Cover rows ready for join: {len(df_cover)}")

        # ---- Species tables (A/B/C) + standardise + diagnostics -----------------
        tbl_a = _make_table_view(CONFIG["SPECIES_TABLE_A_URL"], "species_a_tbl")
        tbl_b = _make_table_view(CONFIG["SPECIES_TABLE_B_URL"], "species_b_tbl")
        tbl_c = _make_table_view(CONFIG["SPECIES_TABLE_C_URL"], "species_c_tbl")

        df_a_raw = _hosted_to_df(tbl_a, "*")
        df_b_raw = _hosted_to_df(tbl_b, "*")
        df_c_raw = _hosted_to_df(tbl_c, "*")

        def _first_cols(df, n=8):
            try:
                return list(df.columns)[:n]
            except Exception:
                return []

        arcpy.AddMessage(f"Species A rows: {len(df_a_raw)}; cols: {_first_cols(df_a_raw)}...")
        arcpy.AddMessage(f"Species B rows: {len(df_b_raw)}; cols: {_first_cols(df_b_raw)}...")
        arcpy.AddMessage(f"Species C rows: {len(df_c_raw)}; cols: {_first_cols(df_c_raw)}...")

        df_a = _standardize_species(df_a_raw, "upper", "upper_stratum_a", "cover_a", "abund_a", unknown_col="unknown_a")
        df_b = _standardize_species(df_b_raw, "mid",   "mid_stratum_b",   "cover_b", "abund_b", unknown_col="unknown_b")
        df_c = _standardize_species(df_c_raw, "lower", "lower_stratum_c", "cover_c", "abund_c", unknown_col="unknown_c")

        df_all_species = (
            pd.concat([df_a, df_b, df_c], ignore_index=True)
            if not (df_a.empty and df_b.empty and df_c.empty)
            else pd.DataFrame(columns=["parentglobalid","species_name","cover","ab","stratum"])
        )
        arcpy.AddMessage(f"All species combined: {len(df_all_species)}")

        if not df_cover.empty:
            df_cover = df_cover.copy()
        if not df_all_species.empty:
            df_all_species = df_all_species.copy()
            if "parentglobalid" not in df_all_species.columns:
                arcpy.AddWarning("Species tables missing 'parentglobalid' column; check schema.")
                df_all_species["parentglobalid"] = None
            df_all_species["parentglobalid_norm"] = _norm_guid_series(df_all_species["parentglobalid"])

        if not df_cover.empty and not df_all_species.empty:
            df_species_joined = df_all_species.merge(
                df_cover[["globalid_norm", "bam_surv_norm"]],
                left_on="parentglobalid_norm",
                right_on="globalid_norm",
                how="left"
            )
        else:
            df_species_joined = pd.DataFrame(columns=["species_name","cover","ab","stratum","bam_surv_norm"])

        arcpy.AddMessage(f"Species joined rows: {len(df_species_joined)}")
        if not df_species_joined.empty:
            arcpy.AddMessage("df_species_joined (first 10 rows):")
            arcpy.AddMessage(df_species_joined.head(10).to_string())
            counts = df_species_joined.groupby(["bam_surv_norm","stratum"])["species_name"].count()
            arcpy.AddMessage("Species counts by bam_surv & stratum:")
            arcpy.AddMessage(counts.to_string())
        else:
            arcpy.AddWarning("Species join produced 0 rows – check GUID normalization and column names.")

        # ---- Structure & Function ----------------------------------------------
        func_lyr = _make_feature_layer(CONFIG["STRUCTURE_FUNCTION_LAYER_URL"], "bam_function_lyr")
        func_fields = [
            'aep_projnum','bam_surv','grp_assessor','plotnum',
            'five','five_nine','ten_nineteen','twenty_twentynine',
            'size_1a','size_1b','size_1c','log_length','hbt_num',
            'disturbance_type','hab_feat','globalid',
            'littersubplot1','littersubplot2','littersubplot3','littersubplot4','littersubplot5'
        ]

        # ---- End Point related layer (/1) – preload + normalize -----------------
        end_lyr = _make_feature_layer(CONFIG["ENDPOINT_LAYER_URL"], "bam_endpoints_lyr")

        # Autodetect actual field names in End Point layer
        end_fields_actual = [f.name for f in arcpy.ListFields(end_lyr)]
        fld_parentgid = next((n for n in end_fields_actual if n.lower() == "parentglobalid"), "parentglobalid")
        fld_ende      = next((n for n in end_fields_actual if n.lower() == "endeasting"), "endeasting")
        fld_endn      = next((n for n in end_fields_actual if n.lower() == "endnorthing"), "endnorthing")

        df_end = _hosted_to_df(end_lyr, [fld_parentgid, fld_ende, fld_endn, "OBJECTID"])
        if not df_end.empty:
            df_end["parentglobalid_norm"] = (
                df_end[fld_parentgid].astype(str).str.strip()
                .str.replace("{", "", regex=False)
                .str.replace("}", "", regex=False)
                .str.upper()
            )
            end_lookup = {
                row["parentglobalid_norm"]: (row.get(fld_ende), row.get(fld_endn), row.get("OBJECTID"))
                for _, row in df_end.iterrows()
            }
        else:
            end_lookup = {}
        arcpy.AddMessage(f"EndPoint rows loaded: {len(df_end)}; unique parent IDs: {df_end['parentglobalid_norm'].nunique() if not df_end.empty else 0}")

        # Temp base for photos
        photo_tmp_base = os.path.join(tempfile.gettempdir(), f"bam_photos_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        os.makedirs(photo_tmp_base, exist_ok=True)

        # ---- Build site contexts ------------------------------------------------
        sites = []
        for _, row in df_plots.iterrows():
            bam_surv = row["bam_surv"]
            # Species for this plot
            if not df_species_joined.empty:
                bam_key = str(bam_surv).strip()
                species_subset = df_species_joined[df_species_joined["bam_surv_norm"] == bam_key]
            else:
                species_subset = pd.DataFrame()

            site = {
                "projectnum": _nz_str(row["projnum"]),
                "plotid":     _nz_str(row["plotid"]),
                "surveydate": row["surveydate"],
                "observers":  _nz_str(row["observers"]),
                "easting":    _nz_int(row["easting"]),
                "northing":   _nz_int(row["northing"]),
                "zone":       _nz_int(row["zone"]),
                "bearing":    _nz_int(row["bearing"], 0),

                "upspecies":  _species_group(species_subset[species_subset["stratum"] == "upper"]) if not species_subset.empty else [],
                "midspecies": _species_group(species_subset[species_subset["stratum"] == "mid"])   if not species_subset.empty else [],
                "lowspecies": _species_group(species_subset[species_subset["stratum"] == "lower"]) if not species_subset.empty else [],

                "functionaccessor": "",
                "stem_lt5":        "-",
                "stem_5_9":        "-",
                "stem_10_19":      "-",
                "stem_20_29":      "-",
                "count_30_49":     0,
                "count_50_79":     0,
                "count_gt80":      0,
                "count_HBT":       0,
                "len_logs":        0,
                "struct_disturbance": "",
                "struct_comment":     "",
                "sp1": 0, "sp2": 0, "sp3": 0, "sp4": 0, "sp5": 0,
                "litter_total": 0,
                "stem_total": 0,

                # End coordinates (from related layer)
                "end_easting":  0,
                "end_northing": 0,

                # Photos
                "photo_start_paths": [],
                "photo_end_paths":   [],
            }

            # Structure & Function record
            where_func = (
                f"aep_projnum = '{project_number}' AND bam_surv = {bam_surv}"
                if isinstance(bam_surv, (int, float)) and not pd.isna(bam_surv)
                else f"aep_projnum = '{project_number}' AND bam_surv = '{_nz_str(bam_surv)}'"
            )
            found_func = False
            with arcpy.da.SearchCursor(func_lyr, [
                'aep_projnum','bam_surv','grp_assessor','plotnum',
                'five','five_nine','ten_nineteen','twenty_twentynine',
                'size_1a','size_1b','size_1c','log_length','hbt_num',
                'disturbance_type','hab_feat','globalid',
                'littersubplot1','littersubplot2','littersubplot3','littersubplot4','littersubplot5'
            ], where_clause=where_func) as fcur:
                for (aep_projnum, bam_surv_f, grp_accessor, plotnum,
                     five, five_nine, ten_nineteen, twenty_twentynine,
                     size_1a, size_1b, size_1c, log_length, hbt_num,
                     disturbance_type, hab_feat, func_globalid,
                     lit1, lit2, lit3, lit4, lit5) in fcur:
                    site.update({
                        "functionaccessor": _nz_str(grp_accessor),
                        "stem_lt5":         _present_absent_to_yesno(five),
                        "stem_5_9":         _present_absent_to_yesno(five_nine),
                        "stem_10_19":       _present_absent_to_yesno(ten_nineteen),
                        "stem_20_29":       _present_absent_to_yesno(twenty_twentynine),
                        "count_30_49":      _nz_int(size_1a),
                        "count_50_79":      _nz_int(size_1b),
                        "count_gt80":       _nz_int(size_1c),
                        "count_HBT":        _nz_int(hbt_num),
                        "len_logs":         _nz_int(log_length),
                        "struct_disturbance": _nz_str(disturbance_type),
                        "struct_comment":     _nz_str(hab_feat),
                        "sp1": _nz_int(lit1),
                        "sp2": _nz_int(lit2),
                        "sp3": _nz_int(lit3),
                        "sp4": _nz_int(lit4),
                        "sp5": _nz_int(lit5),
                    })
                    found_func = True
                    break
            site["litter_total"] = _nz_int(site["sp1"]) + _nz_int(site["sp2"]) + _nz_int(site["sp3"]) + _nz_int(site["sp4"]) + _nz_int(site["sp5"])
            site["stem_total"] = _nz_int(site["stem_lt5"]) + _nz_int(site["stem_5_9"]) + _nz_int(site["stem_10_19"]) + _nz_int(site["stem_20_29"])
            if not found_func:
                arcpy.AddWarning(f"No BAM Function record for bam_surv={bam_surv} (project {project_number}); using defaults.")

            # --- START PHOTOS from Establishment (classified by filename)
            plot_oid = _nz_int(row["oid"])
            plot_dir = os.path.join(photo_tmp_base, f"plot_{_nz_str(row['plotid'])}_start")
            start_files_all = _download_attachments_for_selection("bam_establishment_lyr", f"OBJECTID = {plot_oid}", plot_dir)
            start_files, _ignore = _classify_photos(
                start_files_all,
                start_patterns=CONFIG["PHOTO_START_PATTERNS"],
                end_patterns=CONFIG["PHOTO_END_PATTERNS"],
                plotid=row["plotid"],
                max_start=CONFIG.get("PHOTO_MAX_START"),
                max_end=CONFIG.get("PHOTO_MAX_END"),
            )
            site["photo_start_paths"] = start_files

            # --- END COORDINATES + END PHOTOS from related End Point layer
            est_gid_raw  = _nz_str(row["est_globalid_raw"])
            est_gid_norm = est_gid_raw.strip().replace("{", "").replace("}", "").upper()

            e_pair = end_lookup.get(est_gid_norm)  # (endeasting, endnorthing, OBJECTID)
            if e_pair:
                ee, en, e_oid = e_pair
                site["end_easting"]  = _nz_int(ee)
                site["end_northing"] = _nz_int(en)
            else:
                arcpy.AddWarning(f"No End Point coords for Establishment GlobalID={est_gid_raw} (plot {site['plotid']}).")

            end_dir = os.path.join(photo_tmp_base, f"plot_{_nz_str(row['plotid'])}_end")
            # Attempt selection with raw first, then with braces if needed
            where_end = f"{fld_parentgid} = '{est_gid_raw}'"
            end_files = _download_attachments_for_selection("bam_endpoints_lyr", where_end, end_dir)
            if not end_files and (est_gid_raw and "{" not in est_gid_raw and "}" not in est_gid_raw):
                where_end2 = f"{fld_parentgid} = '{{{est_gid_raw}}}'"
                end_files = _download_attachments_for_selection("bam_endpoints_lyr", where_end2, end_dir)

            if isinstance(CONFIG.get("PHOTO_MAX_END"), int) and CONFIG["PHOTO_MAX_END"] > 0:
                end_files = end_files[:CONFIG["PHOTO_MAX_END"]]
            site["photo_end_paths"] = end_files

            arcpy.AddMessage(f"Plot {site['plotid']}: start photos={len(site['photo_start_paths'])}, end photos={len(site['photo_end_paths'])}")
            if site["end_easting"] and site["end_northing"]:
                arcpy.AddMessage(f"Plot {site['plotid']}: end coords E={site['end_easting']}, N={site['end_northing']}")

            sites.append(site)

        if not sites:
            raise arcpy.ExecuteError("No site records assembled to write into the template.")

        # ---- Render & compose --------------------------------------------------
        tmp_files = []
        try:
            prep_dir = os.path.join(tempfile.gettempdir(), "bam_photo_prep")
            for idx, site in enumerate(sites, start=1):
                doc = DocxTemplate(site_tpl)

                # EXIF auto-rotate and orientation-based sizing
                start_photos = []
                for p in site.get("photo_start_paths", []):
                    pp, is_portrait = _prep_photo(
                        p, prep_dir,
                        max_w_mm=CONFIG.get("PHOTO_MAX_WIDTH_MM", 80),
                        max_h_mm=CONFIG.get("PHOTO_MAX_HEIGHT_MM", 100),
                        auto_orient=CONFIG.get("AUTO_ORIENT_PHOTOS", True)
                    )
                    try:
                        if is_portrait is True:
                            start_photos.append(InlineImage(doc, pp, height=Mm(CONFIG["PHOTO_MAX_HEIGHT_MM"])))
                        elif is_portrait is False:
                            start_photos.append(InlineImage(doc, pp, width=Mm(CONFIG["PHOTO_MAX_WIDTH_MM"])))
                        else:
                            start_photos.append(InlineImage(doc, pp, width=Mm(CONFIG["PHOTO_MAX_WIDTH_MM"])))
                    except Exception as ex:
                        arcpy.AddWarning(f"Failed to load start photo '{pp}': {ex}")

                end_photos = []
                for p in site.get("photo_end_paths", []):
                    pp, is_portrait = _prep_photo(
                        p, prep_dir,
                        max_w_mm=CONFIG.get("PHOTO_MAX_WIDTH_MM", 80),
                        max_h_mm=CONFIG.get("PHOTO_MAX_HEIGHT_MM", 100),
                        auto_orient=CONFIG.get("AUTO_ORIENT_PHOTOS", True)
                    )
                    try:
                        if is_portrait is True:
                            end_photos.append(InlineImage(doc, pp, height=Mm(CONFIG["PHOTO_MAX_HEIGHT_MM"])))
                        elif is_portrait is False:
                            end_photos.append(InlineImage(doc, pp, width=Mm(CONFIG["PHOTO_MAX_WIDTH_MM"])))
                        else:
                            end_photos.append(InlineImage(doc, pp, width=Mm(CONFIG["PHOTO_MAX_WIDTH_MM"])))
                    except Exception as ex:
                        arcpy.AddWarning(f"Failed to load end photo '{pp}': {ex}")

                site["start_photos"] = start_photos
                site["end_photos"]   = end_photos
                cols = max(1, int(CONFIG.get("PHOTO_GRID_COLS", 2)))
                site["start_photo_rows"] = _chunk_rows(start_photos, cols)
                site["end_photo_rows"]   = _chunk_rows(end_photos, cols)

                doc.render(site)
                fd, tmp_path = tempfile.mkstemp(suffix=".docx")
                os.close(fd)
                doc.save(tmp_path)
                tmp_files.append(tmp_path)

            base = Document(tmp_files[0])
            composer = Composer(base)
            for p in tmp_files[1:]:
                composer.append(Document(p))
            composer.save(out_doc)

            arcpy.AddMessage(f"Written → {out_doc}")
        finally:
            for p in tmp_files:
                try:
                    os.remove(p)
                except Exception:
                    pass

        return
