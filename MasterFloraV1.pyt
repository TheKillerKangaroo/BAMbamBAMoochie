import arcpy
import pandas as pd
import os
import datetime
import traceback
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter


# === CONFIG: Hardcoded service URLs (must remain) ===
BAM_LAYER_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/0"
FLORA_TABLE_A_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/1"
FLORA_TABLE_B_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/2"
FLORA_TABLE_C_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/3"


# Field alias presets
SPECIES_ALIASES = ['mid_stratum_b', 'upper_stratum_a', 'lower_stratum_c']
COVER_ALIASES = ['cover_b', 'cover_a', 'cover_c']
PARENT_FIELD_ALIASES = ['parentglobalid', 'parent_globalid', 'parentGlobalID', 'parent_gid', 'parentid']
PROJECT_FIELD_CANDIDATES = ['aep_projnum', 'aep_proj_num', 'projnum', 'proj_num', 'projectnumber', 'project_number']
EXTRA_SPECIES_FIELDS = ['family', 'vernacularName', 'establishmentMeans', 'primaryGrowthForm', 'primaryGrowthFormGroup', 'highThreatWeed']


# --- Logging helpers ---
def _msg(s):
    try:
        arcpy.AddMessage(s)
    except Exception:
        pass


def _warn(s):
    try:
        arcpy.AddWarning(s)
    except Exception:
        pass


def _err(s):
    try:
        arcpy.AddError(s)
    except Exception:
        pass


# Helper functions
def find_field_by_alias(field_list, aliases):
    """Find the actual field name from a list given possible aliases (case-insensitive).
    Returns the first matching field name or None.
    """
    low_map = {f.lower(): f for f in field_list}
    for a in aliases:
        if a.lower() in low_map:
            return low_map[a.lower()]
    return None


def normalize_guid(g):
    """Normalize GlobalID/ParentGlobalID values for reliable matching.
    - Converts to string, strips whitespace and braces, and uppercases.
    - Returns None for empty/None inputs.
    """
    if g is None:
        return None
    s = str(g).strip()
    if s == "":
        return None
    s = s.strip('{} ')
    return s.upper()


def build_where_clause_for_project(proj_field, project_number):
    """Build a where clause that tries numeric and string equality where appropriate."""
    try:
        project_num_int = int(project_number)
        return f"{proj_field} = {project_num_int}"
    except Exception:
        safe_val = str(project_number).replace("'", "''")
        return f"{proj_field} = '{safe_val}'"


def get_project_globalids(bam_layer, proj_field, globalid_field, project_number):
    """Return a set of normalized globalids for records in bam_layer matching project_number.
    Returns an empty set if nothing found.
    """
    where = build_where_clause_for_project(proj_field, project_number)
    _msg(f"Querying BAM layer for project using where clause: {where}")
    gset = set()
    try:
        with arcpy.da.SearchCursor(bam_layer, [globalid_field], where_clause=where) as cur:
            for (g,) in cur:
                ng = normalize_guid(g)
                if ng:
                    gset.add(ng)
    except Exception as e:
        _warn(f"Could not query BAM layer with where clause ({where}): {e}")
    return gset


def table_to_df_filtered(table, parent_col, allowed_globalids, needed_fields=None):
    """Read only rows from `table` whose parent_col (normalized) is in allowed_globalids.
    - needed_fields: list of fields to retrieve (if None, will select all non-OID/Geometry fields).
    - Returns a pandas DataFrame (may be empty).
    """
    try:
        available_fields = [f.name for f in arcpy.ListFields(table) if f.type not in ('OID', 'Geometry')]
    except Exception as e:
        raise RuntimeError(f"Failed to list fields for table {table}: {e}")

    if needed_fields:
        fields = [f for f in needed_fields if f in available_fields]
    else:
        fields = available_fields[:]

    if parent_col not in fields:
        if parent_col in available_fields:
            fields = [parent_col] + fields
        else:
            raise ValueError(f"Parent ID column '{parent_col}' not found in table {table}")

    rows = []
    try:
        with arcpy.da.SearchCursor(table, fields) as cur:
            for row in cur:
                rec = dict(zip(fields, row))
                pg = normalize_guid(rec.get(parent_col))
                if pg and pg in allowed_globalids:
                    rows.append([rec.get(f) for f in fields])
    except Exception as e:
        raise RuntimeError(f"Error reading rows from {table}: {e}")

    if not rows:
        return pd.DataFrame(columns=fields)
    return pd.DataFrame(rows, columns=fields)


def safe_write_excel(output_path, species_list_df, joined_df, include_detailed, project_number):
    """Write Excel with improved formatting, metadata, and safe overwrite.
    """
    outdir = os.path.dirname(output_path)
    if outdir and not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)

    tmp_path = output_path + ".tmp"

    try:
        with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
            species_list_df.to_excel(writer, sheet_name='Species List', index=False)
            if include_detailed and joined_df is not None:
                joined_df.to_excel(writer, sheet_name='Flora Data', index=False)

            workbook = writer.book
            # Metadata sheet
            meta = workbook.create_sheet('Metadata')
            meta['A1'] = 'Generated'
            meta['B1'] = datetime.datetime.now().isoformat()
            meta['A2'] = 'Project Number'
            meta['B2'] = str(project_number)
            meta['A3'] = 'Species Count'
            meta['B3'] = len(species_list_df)

            # Format Species List sheet
            species_ws = workbook['Species List']
            for cell in species_ws[1]:
                cell.font = Font(bold=True)

            italic_font = Font(italic=True)
            # Try to italicise first column which is Scientific Name / species
            for row_idx in range(2, len(species_list_df) + 2):
                try:
                    species_ws[f'A{row_idx}'].font = italic_font
                except Exception:
                    pass
            species_ws.freeze_panes = 'A2'

            # Autofit columns (approximate)
            for ws_name in ['Species List'] + (['Flora Data'] if include_detailed and joined_df is not None else []):
                try:
                    ws = workbook[ws_name]
                except KeyError:
                    continue
                for col in ws.columns:
                    try:
                        max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
                        adjusted_width = min(max(8, max_length + 2), 60)
                        col_letter = get_column_letter(col[0].column)
                        ws.column_dimensions[col_letter].width = adjusted_width
                    except Exception:
                        continue

        # Replace existing file atomically where possible
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except Exception:
                pass
        os.replace(tmp_path, output_path)
    except Exception as e:
        # Cleanup tmp and re-raise
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise


# ================================
class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the .pyt file)."""
        self.label = "Flora Master Listing Tools"
        self.alias = "FloraTools"
        self.tools = [FloraMasterListingTool]


class FloraMasterListingTool(object):
    def __init__(self):
        self.label = "Create Flora Master Listing"
        self.description = "Creates a master flora listing as a spreadsheet from BAM Cover and Abundance data"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        param0 = arcpy.Parameter(
            displayName="Project Number",
            name="project_number",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )

        try:
            _vals = _get_unique_project_numbers()
            if _vals:
                param0.filter.type = "ValueList"
                param0.filter.list = _vals
                param0.value = _vals[0]
        except Exception:
            pass

        param1 = arcpy.Parameter(
            displayName="Include Detailed Flora Data (Plot-by-Plot Cover)",
            name="include_detailed",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        param1.value = True

        param2 = arcpy.Parameter(
            displayName="Output Excel File",
            name="output_excel",
            datatype="DEFile",
            parameterType="Required",
            direction="Output"
        )
        param2.filter.list = ['xlsx']
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        home = os.path.join(os.path.expanduser("~"), "Documents")
        default_xlsx = os.path.join(home, f"Masterflora_{ts}.xlsx")
        param2.value = default_xlsx

        param3 = arcpy.Parameter(
            displayName="BioNet Reference File (optional)",
            name="bionet_file",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input"
        )
        param3.filter.list = ['xlsx']
        param3.value = r"G:\Shared drives\99.3 GIS Admin\Production\Tools\BAM Tools\BioNetPowerQueryLists.xlsx"

        return [param0, param1, param2, param3]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        try:
            arcpy.SetProgressor('default', 'Preparing flora master listing...')
            project_number = parameters[0].valueAsText
            include_detailed = parameters[1].value
            output_excel = parameters[2].valueAsText
            bionet_file = parameters[3].valueAsText if len(parameters) > 3 and parameters[3].valueAsText else None

            bam_layer = BAM_LAYER_URL
            flora_tables = [FLORA_TABLE_A_URL, FLORA_TABLE_B_URL, FLORA_TABLE_C_URL]

            _msg(f"BAM layer (hardcoded): {bam_layer}")
            _msg(f"Project Number: {project_number}")

            if not arcpy.Exists(bam_layer):
                _err(f"BAM layer does not exist: {bam_layer}")
                return

            bam_fields = [f.name for f in arcpy.ListFields(bam_layer)]
            _msg(f"Available fields in BAM layer: {bam_fields}")

            globalid_field = find_field_by_alias(bam_fields, ['globalid', 'global_id'])
            plotnum_field = find_field_by_alias(bam_fields, ['plotnum', 'plot_num', 'plotnumber', 'plot_number'])
            projnum_field = find_field_by_alias(bam_fields, PROJECT_FIELD_CANDIDATES)

            if not globalid_field:
                _err("Could not find GlobalID field in BAM layer")
                return
            if not projnum_field:
                _err("Could not find project number field in BAM layer")
                return
            if not plotnum_field:
                _warn("Could not find plot number field in BAM layer. Plot numbers may not be available in output.")

            _msg(f"Using fields - GlobalID: {globalid_field}, Plot Number: {plotnum_field}, Project Number: {projnum_field}")

            _msg("Retrieving GlobalIDs for selected project...")
            project_globalids = get_project_globalids(bam_layer, projnum_field, globalid_field, project_number)
            if not project_globalids:
                _err(f"No BAM records found for project {project_number}")
                return
            _msg(f"Found {len(project_globalids)} BAM records for project {project_number}")

            dfs = []
            for t in flora_tables:
                _msg(f"Verifying flora table exists: {t}")
                if not arcpy.Exists(t):
                    _err(f"Flora table does not exist: {t}")
                    return

                avail = [f.name for f in arcpy.ListFields(t)]
                parent_col = find_field_by_alias(avail, PARENT_FIELD_ALIASES)
                if not parent_col:
                    _err(f"Could not find Parent GlobalID field in flora table {t}. Available fields: {avail}")
                    return

                species_cols = [c for c in SPECIES_ALIASES if c in avail]
                cover_cols = [c for c in COVER_ALIASES if c in avail]

                needed = [parent_col] + species_cols + cover_cols
                extra = [c for c in EXTRA_SPECIES_FIELDS if c in avail]
                needed += extra

                _msg(f"Reading filtered rows from flora table {t} (fields: {needed})")
                try:
                    df = table_to_df_filtered(t, parent_col, project_globalids, needed_fields=needed)
                except Exception as e:
                    _warn(f"Skipping table {t} due to read error: {e}")
                    continue

                if not df.empty:
                    # Normalize parent column consistently
                    df[parent_col] = df[parent_col].astype(str).str.strip().str.strip('{}').str.upper()
                    dfs.append(df)

            if not dfs:
                _err(f"No flora records found for project {project_number}")
                return

            merged_df = pd.concat(dfs, ignore_index=True)
            _msg(f"Merged flora records: {len(merged_df)}")

            _msg("Building BAM lookup for plot numbers...")
            join_fields = [globalid_field]
            if plotnum_field:
                join_fields.append(plotnum_field)
            if projnum_field:
                join_fields.append(projnum_field)

            join_data = []
            with arcpy.da.SearchCursor(bam_layer, join_fields) as cur:
                for row in cur:
                    g = normalize_guid(row[0])
                    if not g:
                        continue
                    plotval = row[1] if len(row) > 1 else None
                    projval = row[2] if len(row) > 2 else None
                    join_data.append((g, plotval, projval))

            join_df = pd.DataFrame(join_data, columns=['globalid_lookup', 'plotnum', 'aep_projnum'])
            join_df['globalid_lookup'] = join_df['globalid_lookup'].astype(str).str.strip().str.strip('{}').str.upper()

            # Find which parent column the merged df contains
            parent_col_in_merged = None
            lowered_parents = [p.lower() for p in PARENT_FIELD_ALIASES]
            for col in merged_df.columns:
                if col.lower() in lowered_parents:
                    parent_col_in_merged = col
                    break

            if not parent_col_in_merged:
                _err(f"Could not find parent ID column in merged flora data. Available columns: {list(merged_df.columns)}")
                return

            merged_df[parent_col_in_merged] = merged_df[parent_col_in_merged].astype(str).str.strip().str.strip('{}').str.upper()

            merged_df = merged_df.merge(join_df, left_on=parent_col_in_merged, right_on='globalid_lookup', how='left')

            _msg(f"Filtering merged flora by project number: {project_number}")
            # Normalize project column
            try:
                merged_df['aep_projnum'] = merged_df['aep_projnum'].astype(object)
            except Exception:
                pass

            mask = pd.Series(False, index=merged_df.index)
            try:
                mask = mask | (merged_df['aep_projnum'].astype(str).str.strip() == str(project_number).strip())
            except Exception:
                pass
            try:
                pn_int = int(project_number)
                mask = mask | (merged_df['aep_projnum'] == pn_int)
            except Exception:
                pass

            filtered_df = merged_df[mask]

            if filtered_df.empty:
                _err(f"No flora data found for project number {project_number} after filtering")
                return

            _msg(f"Filtered flora records: {len(filtered_df)}")

            species_columns = [c for c in SPECIES_ALIASES if c in filtered_df.columns]
            abundance_columns = [c for c in COVER_ALIASES if c in filtered_df.columns]

            if not species_columns:
                _err('No species columns found in the filtered data. Expected one of: mid_stratum_b, upper_stratum_a, lower_stratum_c')
                return

            # Combine species/cover columns - prefer left-most non-null value across species columns
            filtered_df['species'] = filtered_df[species_columns].bfill(axis=1).iloc[:, 0]
            if abundance_columns:
                filtered_df['cover'] = filtered_df[abundance_columns].bfill(axis=1).iloc[:, 0]
            else:
                filtered_df['cover'] = None

            if include_detailed:
                _msg('Creating pivot table for detailed data...')
                pivot_df = filtered_df.pivot_table(index='species', columns='plotnum', values='cover', aggfunc='first')
                pivot_df.columns = [f"plot {col}" for col in pivot_df.columns]
                pivot_df = pivot_df.reset_index()
            else:
                pivot_df = filtered_df[['species']].drop_duplicates().reset_index(drop=True)

            _msg('Loading BioNet reference data (if provided)...')
            if bionet_file and arcpy.Exists(bionet_file):
                try:
                    xref_df = pd.read_excel(bionet_file, sheet_name='Flora_species_powerQuery')
                    xref_df = xref_df.rename(columns={'scientificName': 'species'})
                    pivot_df['species'] = pivot_df['species'].astype(str).str.strip()
                    xref_df['species'] = xref_df['species'].astype(str).str.strip()
                    joined_df = pivot_df.merge(xref_df[
                        ['species', 'family', 'vernacularName', 'stateConservation', 'countryConservation',
                         'establishmentMeans', 'primaryGrowthForm', 'primaryGrowthFormGroup', 'highThreatWeed']
                    ], on='species', how='left')
                except Exception as e:
                    _warn(f'Failed to load or join BioNet reference data: {e}')
                    joined_df = pivot_df.copy()
                    for col in ['family', 'vernacularName', 'establishmentMeans']:
                        joined_df[col] = ''
            else:
                if bionet_file:
                    _warn(f'BioNet reference file not found: {bionet_file}. Proceeding without species details.')
                else:
                    _msg('No BioNet reference file provided. Proceeding without species details.')
                joined_df = pivot_df.copy()
                for col in ['family', 'vernacularName', 'establishmentMeans']:
                    joined_df[col] = ''

            # Rename and reorder
            joined_df = joined_df.rename(columns={'species': 'Scientific Name'})

            if include_detailed:
                plot_cols = [col for col in joined_df.columns if str(col).startswith('plot ')]
                other_cols = [col for col in joined_df.columns if col not in plot_cols]
                joined_df = joined_df[other_cols + plot_cols]

            _msg('Creating species list...')
            unique_species_df = joined_df[['Scientific Name', 'vernacularName', 'establishmentMeans']].drop_duplicates()

            def mark_introduced(row):
                name = row.get('Scientific Name') or ''
                if row.get('establishmentMeans') == 'Introduced':
                    return f"{name}*"
                return name

            unique_species_df['Scientific Name'] = unique_species_df.apply(mark_introduced, axis=1)
            species_list_df = unique_species_df[['Scientific Name', 'vernacularName']].copy()
            species_list_df = species_list_df.rename(columns={'vernacularName': 'Common Name'})
            species_list_df = species_list_df.sort_values('Scientific Name')

            _msg(f'Writing results to: {output_excel}')
            try:
                safe_write_excel(output_excel, species_list_df, joined_df if include_detailed else None, include_detailed, project_number)
            except Exception as e:
                _err(f'Failed to write Excel file: {e}')
                _err(traceback.format_exc())
                return

            _msg('Flora master listing created successfully!')
            _msg(f'Species list contains {len(species_list_df)} unique species')
            if include_detailed:
                _msg(f'Detailed data contains {len(joined_df)} species records')

        except Exception as e:
            _err(f'Error: {e}')
            _err(traceback.format_exc())


# Utility: get unique project numbers from BAM layer (keeps the original behaviour but more robust)
def _get_unique_project_numbers():
    try:
        bam_fields = [f.name for f in arcpy.ListFields(BAM_LAYER_URL)]
        proj_field = None
        for f in bam_fields:
            if f.lower() in PROJECT_FIELD_CANDIDATES:
                proj_field = f
                break
        if proj_field is None:
            return []

        values = set()
        with arcpy.da.SearchCursor(BAM_LAYER_URL, [proj_field]) as cur:
            for (v,) in cur:
                if v is None:
                    continue
                values.add(str(v))

        def sort_key(x):
            try:
                return (0, int(x))
            except Exception:
                return (1, x)

        return sorted(values, key=sort_key)
    except Exception:
        return []
