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
    index = 0
    manifest_filename = "manifest.json"
    last_manifest_filename = manifest_filename
    last_manifest_schemas = {}

    while True:
        all_manifest_files.append(manifest_filename)
        with open(manifest_path / manifest_filename, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        manifest_schemas = manifest.get("schemas", {})

        all_mappings.update(manifest_schemas)
        last_manifest_filename = manifest_filename
        last_manifest_schemas = manifest_schemas

        if manifest.get("next") is None:
            break

        next_manifest_file = manifest.get("next")

        if not (manifest_path / next_manifest_file).exists():
            raise ManifestError(f"Manifest file '{next_manifest_file}' referenced_by '{manifest_filename}' not found")
        
        manifest_filename = next_manifest_file
        index += 1

    new_manifest_entries = {}

    unverified_includes = defaultdict(list)
    for json_file in (filename for filename in manifest_path.iterdir() if \
                        filename.name not in all_manifest_files and \
                        filename.name != "index.json" and \
                        filename.suffix == ".json"):
        file_mappings = []

        if json_file.stem in all_mappings.keys():
            continue
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except:
            raise ManifestError(f"Failed to load JSON file '{json_file.name}'")

        if not "version" in json_data or not json_data["version"] == 1:
            raise ManifestError(f"JSON file '{json_file.name}' missing required 'version' field")

        if "schemas" in json_data:
            for schema_name in json_data["schemas"].keys():
                if schema_name in all_mappings:
                    raise ManifestError(f"Schema '{json_data['schemas'][schema_name]}' in file '{json_file.name}' already exists in manifest")
                
                if "include" in json_data["schemas"][schema_name]:
                    for include in json_data["schemas"][schema_name]["include"]:
                        unverified_includes[include].append(json_file.name)

        if "mappings" in json_data:
            for mapping in json_data["mappings"]:
                file_mappings.append(mapping)
        
        new_manifest_entries[json_file.stem] = file_mappings

    available_includes = list(all_mappings.keys()) + list(new_manifest_entries.keys())

    for include, refernced_by in unverified_includes.items():
        if include not in available_includes:
            raise ManifestError(f"Included schema '{include}' referenced by files {refernced_by} not found in manifest")
    

    remaining_entries = dict(new_manifest_entries)
    current_manifest_schemas = dict(last_manifest_schemas)
    current_manifest_filename = last_manifest_filename
    created_manifests = []
    while remaining_entries:
        space_available = MAX_MANIFEST_SCHEMAS - len(current_manifest_schemas)
        
        if space_available > 0:
            entries_to_add = dict(islice(remaining_entries.items(), space_available))
            remaining_entries = dict(islice(remaining_entries.items(), space_available, None))
            current_manifest_schemas.update(entries_to_add)

            with open(manifest_path / current_manifest_filename, 'w', encoding='utf-8') as f:
                manifest_data = {
                    "schemas": current_manifest_schemas,
                    "next": None
                }
                json.dump(manifest_data, f, indent=4)
        
        if remaining_entries:
            index += 1
            new_manifest_filename = f"manifest.{index}.json"
            created_manifests.append(new_manifest_filename)
            with open(manifest_path / current_manifest_filename, 'r', encoding='utf-8') as f:
                prev_manifest = json.load(f)
            prev_manifest["next"] = new_manifest_filename
            with open(manifest_path / current_manifest_filename, 'w', encoding='utf-8') as f:
                json.dump(prev_manifest, f, indent=4)

            current_manifest_filename = new_manifest_filename
            current_manifest_schemas = {}

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

    for manifest_file in created_manifests:
        if manifest_file not in existing_manifests:
            existing_manifests.append(manifest_file)
    
    index_data["manifests"] = existing_manifests
    
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=4)
    

if __name__ == "__main__":
    update_manifest()