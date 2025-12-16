# -*- coding: utf-8 -*-
# AEP BAM Fieldsheets - ArcGIS API for Python rewrite
#
# Purpose:
# - Uses ArcGIS API for Python to query hosted feature layers and attachments
#   (avoids arcpy.MakeFeatureLayer / ExportAttachments fragility).
# - Builds site contexts using pandas merges and vectorized ops.
# - Renders a single DOCX via docxtpl using a Jinja loop (no docxcompose merging).
# - Keeps arcpy only for toolbox integration and the WGS84->GDA94(MGA) projection helper.
#
# Dependencies / install (ArcGIS Pro 3.6):
# - Clone your arcgispro-py3 environment before installing packages. Example:
#   conda create -n pro-bam --clone "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3"
#   conda activate pro-bam
#   conda install -c esri arcgis    # ArcGIS API for Python
#   conda install -c conda-forge pandas pillow requests
#   pip install docxtpl python-docx
#
# Template requirements:
# - Template (SITE_TPL) must contain a Jinja loop for "sites", e.g.:
#   {% for site in sites %}
#   Project: {{ site.projectnum }}  Plot: {{ site.plotid }}
#   {% for row in site.start_photo_rows %}
#     {% for img in row %}{{ img }}{% endfor %}
#   {% endfor %}
#   {% endfor %}
#
# Authentication:
# - If your layers are private, provide credentials when running the tool. The toolbox
#   exposes optional username/password parameters; these are passed to GIS(...) here.
#
import arcpy
import os
import tempfile
from datetime import datetime
import pandas as pd
from docxtpl import DocxTemplate, InlineImage
from docx import Document
from docx.shared import Mm
from PIL import Image, ImageOps
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from pathlib import Path
import requests
import logging

# ---- CONFIG -----------------------------------------------------------------
CONFIG = {
    "SITE_TPL": r"G:\Shared drives\99.3 GIS Admin\Development-Testing\Tools\GitHub\BAM_field_sheets\AEP Field Sheet.docx",

    # Hosted feature layers (FeatureServer/<layer_index>)
    "ESTABLISHMENT_LAYER_URL":      "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_f508fd2dab4f467c9b46b25bb97f3bb1/FeatureServer/0",
    "ENDPOINT_LAYER_URL":           "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_f508fd2dab4f467c9b46b25bb97f3bb1/FeatureServer/1",
    "COVER_ABUNDANCE_LAYER_URL":    "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/0",
    "STRUCTURE_FUNCTION_LAYER_URL": "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/survey123_164fa518b8944672bab2507e7a879928_results/FeatureServer/0",

    "SPECIES_TABLE_A_URL":          "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/1",
    "SPECIES_TABLE_B_URL":          "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/2",
    "SPECIES_TABLE_C_URL":          "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/3",

    # Photo detection & layout
    "PHOTO_START_PATTERNS": ["start", "begin", "upstream"],
    "PHOTO_END_PATTERNS":   ["end", "finish", "downstream"],
    "PHOTO_MAX_START": 2,
    "PHOTO_MAX_END":   2,
    "PHOTO_GRID_COLS": 2,

    # Orientation-aware sizing + EXIF auto-rotate
    "PHOTO_MAX_WIDTH_MM": 80,
    "PHOTO_MAX_HEIGHT_MM": 100,
    "AUTO_ORIENT_PHOTOS": True,
}

# ---- Logging helper ---------------------------------------------------------
logger = logging.getLogger("bam_fieldsheets")
logger.setLevel(logging.DEBUG)
# arcpy messages are used for user feedback; also write debug to a temp file if needed.
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
logger.addHandler(sh)

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

def _present_absent_to_yesno(x, default_no=False):
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

def _norm_guid(g):
    if g is None:
        return ""
    return str(g).strip().replace("{", "").replace("}", "").upper()

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

# keep using arcpy for projection helper (simple and reliable inside Pro)
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

# ---- ArcGIS API helpers -----------------------------------------------------
def _get_featurelayer(url, gis=None):
    if gis:
        # If a portal url is present, the arcgis FeatureLayer can be created regardless
        return FeatureLayer(url, gis)
    return FeatureLayer(url)

def _query_layer_to_df(layer: FeatureLayer, where="1=1", out_fields="*", return_geometry=False):
    """
    Query a FeatureLayer and return a pandas DataFrame. Includes geometry XY if requested.
    """
    # out_fields can be list or string
    if isinstance(out_fields, (list, tuple)):
        of = ",".join(out_fields)
    else:
        of = out_fields
    fs = layer.query(where=where, out_fields=of, return_geometry=return_geometry)
    records = []
    for feat in fs.features:
        attrs = feat.attributes.copy()
        if return_geometry and feat.geometry:
            # geometry keys vary by geometry type; for points use x,y
            g = feat.geometry
            if "x" in g and "y" in g:
                attrs["__x"] = g["x"]
                attrs["__y"] = g["y"]
            elif "rings" in g:
                attrs["__geom"] = g
        records.append(attrs)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records)

def _download_attachments_for_object(layer: FeatureLayer, objectid, out_dir, gis=None, accept_image_ext=(".jpg",".jpeg",".png",".gif",".bmp",".tif",".tiff")):
    """
    Download attachments for a single feature objectid using ArcGIS API attachment list and requests.
    Returns list of saved file paths.
    """
    os.makedirs(out_dir, exist_ok=True)
    saved = []
    try:
        att_list = layer.attachments.get_list(objectid)
    except Exception as ex:
        logger.warning(f"attachments.get_list failed for OID={objectid}: {ex}")
        return saved

    token = None
    if gis and getattr(gis, "_con", None) and getattr(gis._con, "token", None):
        token = gis._con.token

    for att in att_list:
        name = att.get("name") or att.get("id") or "att"
        url = att.get("url")
        content_type = att.get("contentType", "").lower()
        ext = os.path.splitext(name)[1].lower()
        if (ext in accept_image_ext) or content_type.startswith("image"):
            try:
                params = {}
                if token:
                    params["token"] = token
                # requests with stream to file
                resp = requests.get(url, params=params, stream=True, timeout=30)
                resp.raise_for_status()
                fname = os.path.join(out_dir, name)
                # ensure unique filename
                base, extn = os.path.splitext(fname)
                i = 1
                while os.path.exists(fname):
                    fname = f"{base}_{i}{extn}"
                    i += 1
                with open(fname, "wb") as fh:
                    for chunk in resp.iter_content(4096):
                        fh.write(chunk)
                saved.append(fname)
            except Exception as ex:
                logger.warning(f"Failed to download attachment {name} for OID={objectid}: {ex}")
    if not saved:
        logger.info(f"No image attachments downloaded for OID={objectid}")
    return saved

# ---- Photo utilities --------------------------------------------------------
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
            out_path = os.path.join(tmp_dir, f"prep_{os.path.basename(in_path)}.jpg")
            im.convert("RGB").save(out_path, "JPEG", quality=90, optimize=True)
            return out_path, is_portrait
    except Exception as ex:
        logger.warning(f"Photo prep failed for '{in_path}': {ex}")
        return in_path, None

# ---- Toolbox -----------------------------------------------------------------
class Toolbox(object):
    def __init__(self):
        self.label = "BAM Field Sheets (API)"
        self.alias = "bam_fieldsheets_api"
        self.tools = [GenerateBAMFieldSheets]

class GenerateBAMFieldSheets(object):
    def __init__(self):
        self.label = "Generate AEP BAM Field Sheets (API)"
        self.description = "Build multi-site DOCX using ArcGIS API for Python to read hosted layers/attachments."
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

        # Optional portal credentials (if the services are private)
        p_user = arcpy.Parameter(
            displayName="Portal Username (optional)",
            name="portal_user",
            datatype="GPString",
            parameterType="Optional",
            direction="Input"
        )
        p_pass = arcpy.Parameter(
            displayName="Portal Password (optional)",
            name="portal_pass",
            datatype="GPString",
            parameterType="Optional",
            direction="Input"
        )

        return [p_project, p_outdoc, p_user, p_pass]

    def updateParameters(self, parameters):
        # populate project list using the existing endpoint (read-only via arcgis API)
        try:
            gis = GIS()  # anonymous
            fl = _get_featurelayer(CONFIG["ESTABLISHMENT_LAYER_URL"], gis=gis)
            df = _query_layer_to_df(fl, where="1=1", out_fields="aep_projnum")
            projects = sorted(df["aep_projnum"].dropna().astype(str).unique().tolist(), key=lambda x:(len(x), x))
            if projects:
                parameters[0].filter.list = projects
                if not parameters[0].value:
                    parameters[0].value = projects[0]
        except Exception as ex:
            arcpy.AddWarning(f"Could not populate project list: {ex}")

        if not parameters[1].altered:
            user_docs = os.path.join(os.path.expanduser("~"), "Documents")
            os.makedirs(user_docs, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            default_path = os.path.join(user_docs, f"Bam_fieldsheet_{timestamp}.docx")
            parameters[1].value = default_path
        return

    def execute(self, parameters, messages):
        project_number = str(parameters[0].valueAsText).strip()
        out_doc = parameters[1].valueAsText
        portal_user = parameters[2].valueAsText if len(parameters) > 2 else None
        portal_pass = parameters[3].valueAsText if len(parameters) > 3 else None

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

        # Create GIS connection (anonymous or with credentials)
        try:
            if portal_user and portal_pass:
                gis = GIS("https://www.arcgis.com", portal_user, portal_pass)  # change portal URL if needed
            else:
                gis = GIS()  # anonymous
        except Exception as ex:
            raise arcpy.ExecuteError(f"Failed to create GIS connection: {ex}")

        # ---- Load establishment features ------------------------------------------------
        est_fl = _get_featurelayer(CONFIG["ESTABLISHMENT_LAYER_URL"], gis=gis)
        est_fields = ["OBJECTID", "globalid", "aep_projnum", "bam", "plot_num", "survey_start",
                      "grp_assessor1", "grp_assessor2", "grp_assessor3", "compass_bearing"]
        df_est = _query_layer_to_df(est_fl, where=f"aep_projnum = '{project_number}'", out_fields=est_fields, return_geometry=True)
        if df_est.empty:
            raise arcpy.ExecuteError(f"No plots found for project {project_number}.")
        arcpy.AddMessage(f"Establishment plots: {len(df_est)}")

        # extract geometry coords if present
        if "__x" in df_est.columns and "__y" in df_est.columns:
            df_est["lon"] = df_est["__x"].astype(float)
            df_est["lat"] = df_est["__y"].astype(float)
            # project to GDA94/MGA per row (using arcpy)
            proj_data = df_est.apply(lambda r: _project_wgs84_to_gda94_mga(r["lon"], r["lat"]), axis=1)
            df_est["easting"] = [p["easting"] for p in proj_data]
            df_est["northing"] = [p["northing"] for p in proj_data]
            df_est["zone"] = [p["zone"] for p in proj_data]
        else:
            df_est["easting"] = 0
            df_est["northing"] = 0
            df_est["zone"] = 0

        # normalise columns
        df_est["plotid"] = df_est.get("plot_num") if "plot_num" in df_est.columns else df_est.get("plotid", "")
        df_est["bam_surv"] = df_est.get("bam") if "bam" in df_est.columns else df_est.get("bam_surv", "")
        df_est["est_globalid_raw"] = df_est.get("globalid")

        # ---- Cover/abundance table --------------------------------------------------
        cov_fl = _get_featurelayer(CONFIG["COVER_ABUNDANCE_LAYER_URL"], gis=gis)
        df_cov = _query_layer_to_df(cov_fl, where="1=1", out_fields="*")
        arcpy.AddMessage(f"Cover rows total: {len(df_cov)}")

        if not df_cov.empty:
            # Normalize project field names if present
            proj_col = next((c for c in df_cov.columns if c.lower() == "aep_projnum"), None)
            bam_col = next((c for c in df_cov.columns if c.lower() == "bam_surv"), None)
            gid_col = next((c for c in df_cov.columns if c.lower() == "globalid"), None)

            if proj_col:
                df_cov["aep_projnum_norm"] = df_cov[proj_col].astype(str).str.strip()
                df_cov_proj = df_cov[df_cov["aep_projnum_norm"] == project_number]
            else:
                df_cov_proj = pd.DataFrame(columns=df_cov.columns)

            if df_cov_proj.empty and bam_col:
                # fallback: match on bam_surv values from establishments
                est_bams = df_est["bam_surv"].astype(str).tolist()
                df_cov_proj = df_cov[df_cov[bam_col].astype(str).isin(est_bams)]
            df_cov = df_cov_proj.copy()

            if gid_col in df_cov.columns:
                df_cov["globalid_norm"] = _norm_guid_series(df_cov[gid_col])
            if bam_col in df_cov.columns:
                df_cov["bam_surv_norm"] = df_cov[bam_col].astype(str).str.strip()
        arcpy.AddMessage(f"Cover rows after filtering: {len(df_cov)}")

        # ---- Species tables (A/B/C) ------------------------------------------------
        def _load_table(url):
            try:
                tbl = _get_featurelayer(url, gis=gis)
                return _query_layer_to_df(tbl, where="1=1", out_fields="*")
            except Exception as ex:
                logger.warning(f"Failed to load table {url}: {ex}")
                return pd.DataFrame()

        df_a_raw = _load_table(CONFIG["SPECIES_TABLE_A_URL"])
        df_b_raw = _load_table(CONFIG["SPECIES_TABLE_B_URL"])
        df_c_raw = _load_table(CONFIG["SPECIES_TABLE_C_URL"])

        def _standardize_species(df, stratum, species_col, cover_col, ab_col, unknown_col=None):
            if df.empty:
                return pd.DataFrame(columns=["parentglobalid", "species_name", "cover", "ab", "unknown_text", "stratum"])
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
            rename_map = {}
            if species_col in df.columns:
                rename_map[species_col] = "species_name"
            if cover_col in df.columns:
                rename_map[cover_col] = "cover"
            if ab_col in df.columns:
                rename_map[ab_col] = "ab"
            if unk_src:
                rename_map[unk_src] = "unknown_text"
            df = df.rename(columns=rename_map)
            if "unknown_text" not in df.columns:
                df["unknown_text"] = ""
            df["stratum"] = stratum
            keep = ["parentglobalid", "species_name", "cover", "ab", "unknown_text", "stratum"]
            return df[[c for c in keep if c in df.columns]]

        df_a = _standardize_species(df_a_raw, "upper", "upper_stratum_a", "cover_a", "abund_a", unknown_col="unknown_a")
        df_b = _standardize_species(df_b_raw, "mid",   "mid_stratum_b",   "cover_b", "abund_b", unknown_col="unknown_b")
        df_c = _standardize_species(df_c_raw, "lower", "lower_stratum_c", "cover_c", "abund_c", unknown_col="unknown_c")

        if not (df_a.empty and df_b.empty and df_c.empty):
            df_all_species = pd.concat([df_a, df_b, df_c], ignore_index=True)
        else:
            df_all_species = pd.DataFrame(columns=["parentglobalid","species_name","cover","ab","stratum"])

        if not df_all_species.empty:
            df_all_species["parentglobalid_norm"] = _norm_guid_series(df_all_species["parentglobalid"])

        # join species to cover via parent GUID normalization
        if not df_cov.empty and not df_all_species.empty:
            df_species_joined = df_all_species.merge(
                df_cov[["globalid_norm", "bam_surv_norm"]],
                left_on="parentglobalid_norm",
                right_on="globalid_norm",
                how="left"
            )
        else:
            df_species_joined = pd.DataFrame(columns=["species_name","cover","ab","stratum","bam_surv_norm"])

        arcpy.AddMessage(f"Species joined rows: {len(df_species_joined)}")

        # ---- Structure & Function (one-row-per-site preferred) ---------------------
        func_fl = _get_featurelayer(CONFIG["STRUCTURE_FUNCTION_LAYER_URL"], gis=gis)
        df_func = _query_layer_to_df(func_fl, where=f"aep_projnum = '{project_number}'", out_fields="*")

        # Normalise column names in df_func used later
        # Example mapping is similar to earlier code; keep flexible by lower-casing keys
        func_cols = {c.lower(): c for c in df_func.columns}

        # ---- End Point layer (related) --------------------------------------------
        end_fl = _get_featurelayer(CONFIG["ENDPOINT_LAYER_URL"], gis=gis)
        # attempt to find parentglobalid and easting/northing field names
        end_df = _query_layer_to_df(end_fl, where="1=1", out_fields="*")
        if not end_df.empty:
            fld_parentgid = next((n for n in end_df.columns if n.lower() == "parentglobalid"), None)
            fld_ende      = next((n for n in end_df.columns if n.lower() == "endeasting"), None)
            fld_endn      = next((n for n in end_df.columns if n.lower() == "endnorthing"), None)
            if fld_parentgid:
                end_df["parentglobalid_norm"] = _norm_guid_series(end_df[fld_parentgid])
                end_lookup = {
                    row["parentglobalid_norm"]: (row.get(fld_ende), row.get(fld_endn), row.get("OBJECTID"))
                    for _, row in end_df.iterrows()
                    if pd.notna(row.get(fld_parentgid))
                }
            else:
                end_lookup = {}
        else:
            end_lookup = {}

        # ---- Temp base for photos
        photo_tmp_base = Path(tempfile.gettempdir()) / f"bam_photos_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        photo_tmp_base.mkdir(parents=True, exist_ok=True)

        # ---- Build site contexts (vectorized where possible) -----------------------
        sites = []
        # Prepare species grouping by bam_surv_norm and stratum
        if not df_species_joined.empty:
            df_species_joined["bam_surv_norm"] = df_species_joined["bam_surv_norm"].astype(str)
        # iterate establishment rows (small-ish set)
        for _, est in df_est.iterrows():
            bam_surv = est.get("bam_surv")
            bam_key = str(bam_surv).strip()
            if not df_species_joined.empty:
                species_subset = df_species_joined[df_species_joined["bam_surv_norm"] == bam_key]
            else:
                species_subset = pd.DataFrame()

            # compile species lists
            def _species_group(sub):
                if sub is None or sub.empty:
                    return []
                rows = []
                for _, r in sub.iterrows():
                    name = _nz_str(r.get("species_name", ""))
                    unk  = _nz_str(r.get("unknown_text", "")).strip()
                    if unk and "unknown" in name.lower():
                        name = f"{name} ({unk})"
                    rows.append({
                        "name":  name,
                        "cover": round(_nz_float(r.get("cover"), 0.0), 1),
                        "ab":    _nz_int(r.get("ab"), 0),
                    })
                return rows

            site = {
                "projectnum": _nz_str(est.get("aep_projnum")),
                "plotid":     _nz_str(est.get("plotid") or est.get("plot_num")),
                "surveydate": est.get("survey_start"),
                "observers":  ", ".join([_nz_str(est.get(c)) for c in ("grp_assessor1","grp_assessor2","grp_assessor3") if est.get(c)]),
                "easting":    _nz_int(est.get("easting", 0)),
                "northing":   _nz_int(est.get("northing", 0)),
                "zone":       _nz_int(est.get("zone", 0)),
                "bearing":    _nz_int(est.get("compass_bearing", 0)),

                "upspecies":  _species_group(species_subset[species_subset["stratum"] == "upper"]) if not species_subset.empty else [],
                "midspecies": _species_group(species_subset[species_subset["stratum"] == "mid"])   if not species_subset.empty else [],
                "lowspecies": _species_group(species_subset[species_subset["stratum"] == "lower"]) if not species_subset.empty else [],

                # defaults; will be updated from func table if present
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

                # Photos (paths will be converted to InlineImage objects at render time)
                "photo_start_paths": [],
                "photo_end_paths":   [],
            }

            # attach structure/function row if found (match bam_surv and project)
            if not df_func.empty:
                # flexible matching: numeric or string
                cond = (df_func.apply(lambda r: str(r.get(next((c for c in df_func.columns if c.lower()=="aep_projnum"), "aep_projnum"), "")).strip() == str(project_number).strip(), axis=1))
                cond = cond & (df_func.apply(lambda r: str(r.get(next((c for c in df_func.columns if "bam" in c.lower()), "bam_surv"), "")).strip() == str(bam_surv).strip(), axis=1))
                func_matches = df_func[cond]
                if not func_matches.empty:
                    f = func_matches.iloc[0]
                    site.update({
                        "functionaccessor": _nz_str(f.get(next((c for c in df_func.columns if c.lower()=="grp_assessor"), "grp_assessor"), "")),
                        "stem_lt5":         _present_absent_to_yesno(f.get("five", ""), default_no=True),
                        "stem_5_9":         _present_absent_to_yesno(f.get("five_nine", ""), default_no=True),
                        "stem_10_19":       _present_absent_to_yesno(f.get("ten_nineteen", ""), default_no=True),
                        "stem_20_29":       _present_absent_to_yesno(f.get("twenty_twentynine", ""), default_no=True),
                        "count_30_49":      _nz_int(f.get("size_1a")),
                        "count_50_79":      _nz_int(f.get("size_1b")),
                        "count_gt80":       _nz_int(f.get("size_1c")),
                        "count_HBT":        _nz_int(f.get("hbt_num")),
                        "len_logs":         _nz_int(f.get("log_length")),
                        "struct_disturbance": _nz_str(f.get("disturbance_type")),
                        "struct_comment":     _nz_str(f.get("hab_feat")),
                        "sp1": _nz_int(f.get("littersubplot1")),
                        "sp2": _nz_int(f.get("littersubplot2")),
                        "sp3": _nz_int(f.get("littersubplot3")),
                        "sp4": _nz_int(f.get("littersubplot4")),
                        "sp5": _nz_int(f.get("littersubplot5")),
                    })
            site["litter_total"] = _nz_int(site["sp1"]) + _nz_int(site["sp2"]) + _nz_int(site["sp3"]) + _nz_int(site["sp4"]) + _nz_int(site["sp5"])
            # stem_total: interpret "Yes"/"No" as 1/0 only where present; otherwise treat "-" as 0
            def _stem_val(v):
                if isinstance(v, str):
                    if v.lower() == "yes": return 1
                    if v.lower() == "no": return 0
                try:
                    return int(v)
                except Exception:
                    return 0
            site["stem_total"] = _stem_val(site["stem_lt5"]) + _stem_val(site["stem_5_9"]) + _stem_val(site["stem_10_19"]) + _stem_val(site["stem_20_29"])

            # end coords
            est_gid_norm = _norm_guid(est.get("est_globalid_raw") or est.get("globalid"))
            e_pair = end_lookup.get(est_gid_norm)
            if e_pair:
                ee, en, e_oid = e_pair
                site["end_easting"]  = _nz_int(ee)
                site["end_northing"] = _nz_int(en)
            else:
                arcpy.AddWarning(f"No End Point coords for Establishment GlobalID={est.get('est_globalid_raw') or est.get('globalid')} (plot {site['plotid']}).")

            # --- download attachments for establishment (start photos)
            plot_oid = _nz_int(est.get("OBJECTID"))
            start_dir = str(photo_tmp_base / f"plot_{site['plotid']}_start")
            start_files_all = _download_attachments_for_object(est_fl, plot_oid, start_dir, gis=gis)
            start_files, _ = _classify_photos(
                start_files_all,
                start_patterns=CONFIG["PHOTO_START_PATTERNS"],
                end_patterns=CONFIG["PHOTO_END_PATTERNS"],
                plotid=site["plotid"],
                max_start=CONFIG.get("PHOTO_MAX_START"),
                max_end=CONFIG.get("PHOTO_MAX_END"),
            )
            site["photo_start_paths"] = start_files

            # --- download attachments for endpoint (end photos)
            # If we have an OBJECTID for the related endpoint, we can download attachments for it.
            end_saved = []
            e_oid = None
            e_lookup_entry = end_lookup.get(est_gid_norm)
            if e_lookup_entry:
                e_oid = e_lookup_entry[2]
            if e_oid:
                end_dir = str(photo_tmp_base / f"plot_{site['plotid']}_end")
                end_saved = _download_attachments_for_object(end_fl, e_oid, end_dir, gis=gis)
            else:
                # try to find endpoint features by parentglobalid field query (attempt both brace/no-brace)
                # This is slower - only used when direct OID not found
                if est.get("est_globalid_raw"):
                    graw = est.get("est_globalid_raw")
                    for candidate in (graw, f"{{{graw}}}" if "{" not in graw else graw.replace("{","").replace("}","")):
                        try:
                            where = f"parentglobalid = '{candidate}'"
                            found = end_fl.query(where=where, out_fields="OBJECTID")
                            for feat in found.features:
                                end_saved += _download_attachments_for_object(end_fl, feat.attributes.get("OBJECTID"), str(photo_tmp_base / f"plot_{site['plotid']}_end"), gis=gis)
                            if end_saved:
                                break
                        except Exception:
                            continue
            if CONFIG.get("PHOTO_MAX_END") and isinstance(CONFIG["PHOTO_MAX_END"], int):
                end_saved = end_saved[:CONFIG["PHOTO_MAX_END"]]
            site["photo_end_paths"] = end_saved

            arcpy.AddMessage(f"Plot {site['plotid']}: start photos={len(site['photo_start_paths'])}, end photos={len(site['photo_end_paths'])}")
            sites.append(site)

        if not sites:
            raise arcpy.ExecuteError("No site records assembled to write into the template.")

        # ---- Prepare images for docx + Render template in single pass --------------
        try:
            doc = DocxTemplate(site_tpl)
            prep_dir = tempfile.mkdtemp(prefix="bam_prep_")
            # Replace raw paths with InlineImage objects in each site's photo rows
            for site in sites:
                start_photos = []
                for p in site.get("photo_start_paths", []):
                    pp, is_portrait = _prep_photo(p, prep_dir, CONFIG.get("PHOTO_MAX_WIDTH_MM",80), CONFIG.get("PHOTO_MAX_HEIGHT_MM",100), CONFIG.get("AUTO_ORIENT_PHOTOS", True))
                    try:
                        if is_portrait is True:
                            start_photos.append(InlineImage(doc, pp, height=Mm(CONFIG["PHOTO_MAX_HEIGHT_MM"])))
                        else:
                            start_photos.append(InlineImage(doc, pp, width=Mm(CONFIG["PHOTO_MAX_WIDTH_MM"])))
                    except Exception as ex:
                        arcpy.AddWarning(f"Failed to load start photo '{pp}': {ex}")
                end_photos = []
                for p in site.get("photo_end_paths", []):
                    pp, is_portrait = _prep_photo(p, prep_dir, CONFIG.get("PHOTO_MAX_WIDTH_MM",80), CONFIG.get("PHOTO_MAX_HEIGHT_MM",100), CONFIG.get("AUTO_ORIENT_PHOTOS", True))
                    try:
                        if is_portrait is True:
                            end_photos.append(InlineImage(doc, pp, height=Mm(CONFIG["PHOTO_MAX_HEIGHT_MM"])))
                        else:
                            end_photos.append(InlineImage(doc, pp, width=Mm(CONFIG["PHOTO_MAX_WIDTH_MM"])))
                    except Exception as ex:
                        arcpy.AddWarning(f"Failed to load end photo '{pp}': {ex}")

                site["start_photos"] = start_photos
                site["end_photos"]   = end_photos
                cols = max(1, int(CONFIG.get("PHOTO_GRID_COLS", 2)))
                site["start_photo_rows"] = _chunk_rows(start_photos, cols)
                site["end_photo_rows"]   = _chunk_rows(end_photos, cols)

            # Render once with sites context (template must iterate over sites)
            context = {"sites": sites}
            doc.render(context)
            doc.save(out_doc)
            arcpy.AddMessage(f"Written → {out_doc}")

        finally:
            # cleanup prep_dir and photo_tmp_base if desired
            try:
                # remove temporary prep files
                for root, _, files in os.walk(prep_dir if 'prep_dir' in locals() else ""):
                    for f in files:
                        try:
                            os.remove(os.path.join(root, f))
                        except Exception:
                            pass
                # we do NOT automatically delete downloaded original photos to allow inspection;
                # if you prefer automatic cleanup, uncomment:
                # import shutil; shutil.rmtree(photo_tmp_base, ignore_errors=True)
            except Exception:
                pass

        return
