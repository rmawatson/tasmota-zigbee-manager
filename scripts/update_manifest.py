import json
from pathlib import Path
from collections import defaultdict
from itertools import islice

class ManifestError (Exception):...
class ManifestError(Exception):...

MAX_MANIFEST_SCHEMAS = 50

def update_manifest():

    script_dir = Path(__file__).resolve().parent
    manifest_path = script_dir / "../schema/"
    all_mappings = {}
    all_manifest_files = []
    manifest_schemas_by_file = {}   # manifest filename -> its "schemas" entries, in "next" order
    entry_manifest_file = {}        # manifest entry name -> manifest filename it is listed in
    index = 0
    manifest_filename = "manifest.json"
    last_manifest_filename = manifest_filename

    if (manifest_path / manifest_filename).exists():
        while True:
            all_manifest_files.append(manifest_filename)
            with open(manifest_path / manifest_filename, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            manifest_schemas = manifest.get("schemas", {})

            all_mappings.update(manifest_schemas)
            manifest_schemas_by_file[manifest_filename] = manifest_schemas
            for entry_name in manifest_schemas:
                entry_manifest_file[entry_name] = manifest_filename
            last_manifest_filename = manifest_filename

            if manifest.get("next") is None:
                break

            next_manifest_file = manifest.get("next")

            if not (manifest_path / next_manifest_file).exists():
                raise ManifestError(f"Manifest file '{next_manifest_file}' referenced_by '{manifest_filename}' not found")
            
            manifest_filename = next_manifest_file
            index += 1
    else:
        manifest_schemas_by_file[manifest_filename] = {}
    

    new_manifest_entries = {}
    schema_files = {}
    unverified_includes = defaultdict(list)
    for json_file in sorted(filename for filename in manifest_path.iterdir() if \
                        filename.name not in all_manifest_files and \
                        filename.name != "index.json" and \
                        filename.suffix == ".json"):
        file_mappings = []
        file_includes = []
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except:
            raise ManifestError(f"Failed to load JSON file '{json_file.name}'")

        if not "version" in json_data or not json_data["version"] == 1:
            raise ManifestError(f"JSON file '{json_file.name}' missing required 'version' field")

        if "schemas" in json_data:
            for schema_name in json_data["schemas"].keys():
                if schema_name in schema_files:
                    raise ManifestError(f"Schema '{schema_name}' in file '{json_file.name}' is already defined in '{schema_files[schema_name]}'")
                schema_files[schema_name] = json_file.name

                # every file is processed on each run, so the includes are recorded for
                # schemas already listed in a manifest as well as for new ones
                if "include" in json_data["schemas"][schema_name]:
                    for include in json_data["schemas"][schema_name]["include"]:
                        unverified_includes[include].append(json_file.name)
                        if include not in file_includes:
                            file_includes.append(include)
             
        if "mappings" in json_data:
            for mapping in json_data["mappings"]:
                print("Mapping",mapping)
                file_mappings.append(mapping)
        
        new_manifest_entries[json_file.stem] = {"mappings":file_mappings,"includes":file_includes}

    available_includes = list(all_mappings.keys()) + list(new_manifest_entries.keys())

    for include, refernced_by in unverified_includes.items():
        if include not in available_includes:
            raise ManifestError(f"Included schema '{include}' referenced by files {refernced_by} not found in manifest")
    

    # entries already listed in a manifest are updated in place, only new entries are
    # appended to the last manifest (starting another one when it is full)
    remaining_entries = {}
    for entry_name, entry in new_manifest_entries.items():
        if entry_name in entry_manifest_file:
            manifest_schemas_by_file[entry_manifest_file[entry_name]][entry_name] = entry
        else:
            remaining_entries[entry_name] = entry

    current_manifest_filename = last_manifest_filename
    while remaining_entries:
        current_manifest_schemas = manifest_schemas_by_file[current_manifest_filename]
        space_available = MAX_MANIFEST_SCHEMAS - len(current_manifest_schemas)
        
        if space_available > 0:
            entries_to_add = dict(islice(remaining_entries.items(), space_available))
            remaining_entries = dict(islice(remaining_entries.items(), space_available, None))
            current_manifest_schemas.update(entries_to_add)

        if remaining_entries:
            index += 1
            new_manifest_filename = f"manifest.{index}.json" if index else "manifest.json"
            manifest_schemas_by_file[new_manifest_filename] = {}
            current_manifest_filename = new_manifest_filename

    manifest_filenames = list(manifest_schemas_by_file.keys())
    for position, filename in enumerate(manifest_filenames):
        next_filename = manifest_filenames[position + 1] if position + 1 < len(manifest_filenames) else None
        with open(manifest_path / filename, 'w', encoding='utf-8') as f:
            manifest_data = {
                "schemas": manifest_schemas_by_file[filename],
                "next": next_filename
            }
            json.dump(manifest_data, f, indent=4)

    index_path = manifest_path / "index.json"
    if index_path.exists():
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                index_data = json.load(f)
        except:
            index_data = {"manifests": []}
    else:
        index_data = {"manifests": []}

    existing_manifests = index_data.get("manifests", [])

    for manifest_file in manifest_filenames:
        if manifest_file not in existing_manifests:
            existing_manifests.append(manifest_file)
    
    index_data["manifests"] = existing_manifests
    
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=4)
    

if __name__ == "__main__":
    update_manifest()