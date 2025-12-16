import arcpy
import pandas as pd
import os
from openpyxl.styles import Font


# === CONFIG: Hardcoded service URLs ===
# Set these to your ArcGIS Online/Portal Feature Service layer and tables.
# Example format:
# BAM_LAYER_URL = "https://services-ap1.arcgis.com/<orgid>/arcgis/rest/services/<itemname>/FeatureServer/0"
# FLORA_TABLE_A_URL = "https://services-ap1.arcgis.com/<orgid>/arcgis/rest/services/<itemname>/FeatureServer/1"
# FLORA_TABLE_B_URL = "https://services-ap1.arcgis.com/<orgid>/arcgis/rest/services/<itemname>/FeatureServer/2"
# FLORA_TABLE_C_URL = "https://services-ap1.arcgis.com/<orgid>/arcgis/rest/services/<itemname>/FeatureServer/3"
BAM_LAYER_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/0"
FLORA_TABLE_A_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/1"
FLORA_TABLE_B_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/2"
FLORA_TABLE_C_URL = "https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/service_da5758bbd4e14c1bac4e3fc360429bb3/FeatureServer/3"
def _get_unique_project_numbers():
    """Return sorted unique project numbers from the BAM layer URL.
    Falls back to empty list on any error."""
    try:
        candidate_fields = ['aep_projnum', 'aep_proj_num', 'projnum', 'proj_num', 'projectnumber', 'project_number']
        bam_fields = [f.name for f in arcpy.ListFields(BAM_LAYER_URL)]
        proj_field = None
        for f in bam_fields:
            if f.lower() in candidate_fields:
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
# ================================
class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Flora Master Listing Tools"
        self.alias = "FloraTools"

        # List of tool classes associated with this toolbox
        self.tools = [FloraMasterListingTool]


class FloraMasterListingTool(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Flora Master Listing"
        self.description = "Creates a master flora listing as a spreadsheet from BAM Cover and Abundance data"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        import datetime
        import os

        # Parameter 0: Project Number
        param0 = arcpy.Parameter(
            displayName="Project Number",
            name="project_number",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )

        # Populate as dropdown from BAM layer
        try:
            _vals = _get_unique_project_numbers()
            if _vals:
                param0.filter.type = "ValueList"
                param0.filter.list = _vals
                param0.value = _vals[0]
        except Exception:
            pass

        # Parameter 1: Include Detailed Data
        param1 = arcpy.Parameter(
            displayName="Include Detailed Flora Data (Plot-by-Plot Cover)",
            name="include_detailed",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        param1.value = True  # Default to True

        # Parameter 2: Output Excel File
        param2 = arcpy.Parameter(
            displayName="Output Excel File",
            name="output_excel",
            datatype="DEFile",
            parameterType="Required",
            direction="Output"
        )
        param2.filter.list = ['xlsx']

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        #workspace = arcpy.env.workspace or os.getcwd()
        #if workspace.lower().endswith(".gdb"):
        #    workspace = os.path.dirname(workspace)
        home = os.path.join(os.path.expanduser("~"), "Documents")
        default_xlsx = os.path.join(home, f"Masterflora_{ts}.xlsx")
        param2.value = default_xlsx

        # Parameter 3: BioNet Reference File Path (optional, used to enrich species details)
        """
        param3 = arcpy.Parameter(
            displayName="BioNet Reference File (optional)",
            name="bionet_file",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input"
        )
        param3.filter.list = ['xlsx']
        param3.value = r"G:\Shared drives\99.1 Data management\Reference Lists for Surveys\BioNetPowerQueryLists_GH.xlsx"
        """
        return [param0, param1, param2]#param3

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        try:
            # Get parameters (using hardcoded service URLs from CONFIG)
            project_number = parameters[0].valueAsText
            include_detailed = parameters[1].value
            output_excel = parameters[2].valueAsText
            #bionet_file = parameters[3].valueAsText if len(parameters) > 3 else None
            bionet_file = r"G:\Shared drives\99.3 GIS Admin\Production\Tools\BAM Tools\BioNetPowerQueryLists.xlsx"
            bam_layer = BAM_LAYER_URL
            flora_table_a = FLORA_TABLE_A_URL
            flora_table_b = FLORA_TABLE_B_URL
            flora_table_c = FLORA_TABLE_C_URL


            arcpy.AddMessage(f"Processing BAM layer (hardcoded): {bam_layer}")
            arcpy.AddMessage(f"Flora Table A: {flora_table_a}")
            arcpy.AddMessage(f"Flora Table B: {flora_table_b}")
            arcpy.AddMessage(f"Flora Table C: {flora_table_c}")
            arcpy.AddMessage(f"Project Number: {project_number}")
            arcpy.AddMessage(f"Include detailed data: {include_detailed}")

            # List of table paths from parameters
            tables = [flora_table_a, flora_table_b, flora_table_c]

            # Verify tables exist
            for i, table in enumerate(tables, 1):
                if not arcpy.Exists(table):
                    arcpy.AddError(f"Flora table {chr(64 + i)} does not exist: {table}")
                    return

            arcpy.AddMessage(f"All 3 flora tables verified")

            # Function to convert a table to DataFrame
            def table_to_df(table):
                fields = [f.name for f in arcpy.ListFields(table) if f.type not in ('OID', 'Geometry')]
                data = [row for row in arcpy.da.SearchCursor(table, fields)]
                return pd.DataFrame(data, columns=fields)

            # Merge all DataFrames
            arcpy.AddMessage("Merging flora tables...")
            merged_df = pd.concat([table_to_df(t) for t in tables], ignore_index=True)

            # Join plot number from main feature layer
            # Check available fields and find the correct field names
            bam_fields = [f.name for f in arcpy.ListFields(bam_layer)]
            arcpy.AddMessage(f"Available fields in BAM layer: {bam_fields}")

            # Find the correct field names (case-insensitive)
            globalid_field = None
            plotnum_field = None
            projnum_field = None

            # Look for globalid field
            for field in bam_fields:
                if field.lower() in ['globalid', 'global_id']:
                    globalid_field = field
                    break

            # Look for plot number field
            for field in bam_fields:
                if field.lower() in ['plotnum', 'plot_num', 'plotnumber', 'plot_number']:
                    plotnum_field = field
                    break

            # Look for project number field
            for field in bam_fields:
                if field.lower() in ['aep_projnum', 'aep_proj_num', 'projnum', 'proj_num', 'projectnumber',
                                     'project_number']:
                    projnum_field = field
                    break

            # Validate required fields were found
            if not globalid_field:
                arcpy.AddError("Could not find GlobalID field in BAM layer")
                return
            if not plotnum_field:
                arcpy.AddError(
                    "Could not find plot number field in BAM layer. Looking for: plotnum, plot_num, plotnumber, plot_number")
                return
            if not projnum_field:
                arcpy.AddError(
                    "Could not find project number field in BAM layer. Looking for: aep_projnum, aep_proj_num, projnum, proj_num, projectnumber, project_number")
                return

            arcpy.AddMessage(
                f"Using fields - GlobalID: {globalid_field}, Plot Number: {plotnum_field}, Project Number: {projnum_field}")

            join_fields = [globalid_field, plotnum_field, projnum_field]

            # Build a lookup DataFrame from the join table
            join_data = [
                (str(row[0]), row[1], str(row[2]))
                for row in arcpy.da.SearchCursor(bam_layer, join_fields)
            ]
            join_df = pd.DataFrame(join_data, columns=["globalid_lookup", "plotnum", "aep_projnum"])

            # Debug: Show available columns in merged flora data
            arcpy.AddMessage(f"Available columns in merged flora data: {list(merged_df.columns)}")

            # Check for parent ID column (could be different names)
            parent_id_col = None
            possible_parent_cols = ['parentglobalid', 'parent_globalid', 'ParentGlobalID', 'PARENTGLOBALID']

            for col in possible_parent_cols:
                if col in merged_df.columns:
                    parent_id_col = col
                    break

            if parent_id_col is None:
                arcpy.AddError(f"Could not find parent ID column. Available columns: {list(merged_df.columns)}")
                return

            arcpy.AddMessage(f"Using parent ID column: {parent_id_col}")

            # Merge with merged_df using parentglobalid from flora tables = globalid from BAM layer
            merged_df = merged_df.merge(join_df, left_on=parent_id_col, right_on="globalid_lookup", how="left")

            # Filter by project number
            arcpy.AddMessage(f"Filtering by project number: {project_number}")

            # Debug: Show unique project numbers in the data
            unique_projects = merged_df['aep_projnum'].unique()
            arcpy.AddMessage(f"Unique project numbers found in data: {unique_projects}")
            arcpy.AddMessage(f"Data types - Project numbers: {merged_df['aep_projnum'].dtype}")

            # Convert project number parameter to match data type
            # Try both string and numeric comparison
            filtered_df = merged_df[
                (merged_df['aep_projnum'].astype(str) == str(project_number)) |
                (merged_df['aep_projnum'] == project_number) |
                (merged_df['aep_projnum'].astype(str) == project_number)
                ]

            if filtered_df.empty:
                # Try converting project_number to int if it's numeric
                try:
                    project_num_int = int(project_number)
                    filtered_df = merged_df[merged_df['aep_projnum'] == project_num_int]
                    if not filtered_df.empty:
                        arcpy.AddMessage(f"Found data using integer comparison: {project_num_int}")
                except ValueError:
                    pass

            if filtered_df.empty:
                arcpy.AddError(f"No data found for project number: {project_number}")
                arcpy.AddError(f"Available project numbers: {sorted(unique_projects)}")
                return

            arcpy.AddMessage(f"Found {len(filtered_df)} records for project {project_number}")

            # Combine species columns
            species_columns = ['mid_stratum_b', 'upper_stratum_a', 'lower_stratum_c']
            abundance_columns = ['cover_b', 'cover_a', 'cover_c']

            filtered_df['species'] = filtered_df[species_columns].bfill(axis=1).iloc[:, 0]
            filtered_df['cover'] = filtered_df[abundance_columns].bfill(axis=1).iloc[:, 0]

            # Create pivot table if detailed data is requested
            if include_detailed:
                arcpy.AddMessage("Creating pivot table for detailed data...")
                pivot_df = filtered_df.pivot_table(
                    index='species',
                    columns='plotnum',
                    values='cover',
                    aggfunc='first'
                )

                # Rename plotnum columns to include 'plot ' prefix
                pivot_df.columns = [f"plot {col}" for col in pivot_df.columns]
                pivot_df = pivot_df.reset_index()
            else:
                # Just get unique species for the species list
                pivot_df = filtered_df[['species']].drop_duplicates().reset_index(drop=True)

            # Join BioNet species data
            arcpy.AddMessage("Loading BioNet reference data...")
            if bionet_file and arcpy.Exists(bionet_file):
                xref_df = pd.read_excel(bionet_file, sheet_name="Flora_species_powerQuery")
                xref_df = xref_df.rename(columns={"scientificName": "species"})

                # Clean species names
                pivot_df["species"] = pivot_df["species"].str.strip()
                xref_df["species"] = xref_df["species"].str.strip()

                # Join on 'species'
                joined_df = pivot_df.merge(
                    xref_df[
                        ["species", "family", "vernacularName", "stateConservation",
                         "countryConservation", "establishmentMeans",
                         "primaryGrowthForm", "primaryGrowthFormGroup", "highThreatWeed"]
                    ],
                    on="species",
                    how="left"
                )
            else:
                arcpy.AddWarning("BioNet reference file not found. Proceeding without species details.")
                joined_df = pivot_df.copy()
                # Add empty columns for consistency
                for col in ["family", "vernacularName", "establishmentMeans"]:
                    joined_df[col] = ""

            # Rename 'species' to 'Scientific Name'
            joined_df = joined_df.rename(columns={"species": "Scientific Name"})

            if include_detailed:
                # Reorder columns — move 'plot ' columns to the end
                plot_cols = [col for col in joined_df.columns if col.startswith("plot ")]
                other_cols = [col for col in joined_df.columns if col not in plot_cols]
                joined_df = joined_df[other_cols + plot_cols]

            # Create species list worksheet
            arcpy.AddMessage("Creating species list...")
            unique_species_df = joined_df[['Scientific Name', 'vernacularName', 'establishmentMeans']].drop_duplicates()

            # Add asterisk to scientific names where establishmentMeans is 'Introduced'
            unique_species_df['Scientific Name'] = unique_species_df.apply(
                lambda row: row['Scientific Name'] + '*' if row['establishmentMeans'] == 'Introduced' else row[
                    'Scientific Name'],
                axis=1
            )

            # Keep only Scientific Name and vernacularName for the final output
            species_list_df = unique_species_df[['Scientific Name', 'vernacularName']].copy()
            species_list_df = species_list_df.rename(columns={'vernacularName': 'Common Name'})
            species_list_df = species_list_df.sort_values('Scientific Name')

            # Write to Excel
            arcpy.AddMessage(f"Writing results to: {output_excel}")
            with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
                # Always include the Species List worksheet
                species_list_df.to_excel(writer, sheet_name='Species List', index=False)

                # Optionally include the detailed Flora Data worksheet
                if include_detailed:
                    # move the family col to first position
                    try:
                        cols = list(joined_df.columns)
                        if 'family' in cols:
                            cols = ['family'] + [c for c in cols if c != 'family']
                            joined_df = joined_df[cols]
                    except Exception:
                        pass
                    joined_df.to_excel(writer, sheet_name='Flora Data', index=False)

                # Format the Species List worksheet - make Scientific Name column italic
                workbook = writer.book
                species_worksheet = workbook['Species List']

                # Apply italic formatting to Scientific Name column (column A)
                italic_font = Font(italic=True)

                # Start from row 2 (skip header) and format all data rows
                for row in range(2, len(species_list_df) + 2):
                    species_worksheet[f'A{row}'].font = italic_font

            arcpy.AddMessage("Flora master listing created successfully!")
            arcpy.AddMessage(f"Species list contains {len(species_list_df)} unique species")
            if include_detailed:
                arcpy.AddMessage(f"Detailed data contains {len(joined_df)} species records")

        except Exception as e:
            arcpy.AddError(f"Error: {str(e)}")
            import traceback
            arcpy.AddError(traceback.format_exc())
