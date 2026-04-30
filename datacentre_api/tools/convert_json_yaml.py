#!/usr/bin/env python3

import requests
import yaml
import copy

# Add any paths you want to remove here:
TO_REMOVE_PATHS = ["/users", "/clubs", "/players"]

def convert_3_1_to_3_0(obj):
    """
    Recursively walk the OpenAPI spec (loaded as nested dict/list structures)
    and convert any 3.1-style "type: 'null'" or "anyOf"/"oneOf" combos with 'null'
    into 3.0.0-friendly forms. We do this by removing 'type: null' and adding
    'nullable: true' if appropriate.
    """

    if isinstance(obj, dict):
        # If at the top level, change openapi from 3.1.0 to 3.0.0 if needed
        if obj.get("openapi") == "3.1.0":
            obj["openapi"] = "3.0.0"

        # Convert anyOf / oneOf if containing 'type: null'
        for comb in ("anyOf", "oneOf"):
            if comb in obj and isinstance(obj[comb], list):
                has_null = any(
                    (isinstance(x, dict) and x.get("type") == "null")
                    for x in obj[comb]
                )
                if has_null:
                    # Remove 'type: null' subschemas
                    new_list = [
                        x for x in obj[comb] 
                        if not (isinstance(x, dict) and x.get("type") == "null")
                    ]
                    obj[comb] = new_list
                    obj["nullable"] = True

                # If only one item remains, flatten it into current object
                if len(obj[comb]) == 1 and isinstance(obj[comb][0], dict):
                    single_schema = obj[comb][0]
                    obj.pop(comb)
                    for k, v in single_schema.items():
                        if k == "nullable" and k in obj:
                            obj[k] = obj[k] or v  # unify nullables
                        else:
                            obj[k] = v

        # Convert direct "type": "null"
        if obj.get("type") == "null":
            obj.pop("type")
            obj["nullable"] = True

        # Recurse deeper
        for k, v in list(obj.items()):
            obj[k] = convert_3_1_to_3_0(v)

    elif isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = convert_3_1_to_3_0(obj[i])

    return obj

def add_examples_to_params(spec):
    if "paths" not in spec:
        return spec

    for path_name, path_item in spec["paths"].items():
        for method_name, operation in path_item.items():
            if not isinstance(operation, dict):
                continue
            params = operation.get("parameters", [])
            for p in params:
                param_name = p.get("name", "")

                # Add examples
                if path_name == "/players/detailed" and param_name == "player_id":
                    p["example"] = 1100
                elif path_name == "/clubs/detailed" and param_name == "club_id":
                    p["example"] = 50
                elif param_name == "country_id":
                    p["example"] = "ENG"
                    if p.get("description"):
                        p["description"] += " (Examples: ENG, ESP, ITA)"
                    else:
                        p["description"] = "Filter by country ID (Examples: ENG, ESP, ITA)"
                elif param_name == "division":
                    p["example"] = 1

                # (Optional) If you want to remove certain unnecessary parameters, 
                # you could do it here:
                # if param_name in ["age_min", "age_max", "wages_min", "wages_max"]:
                #     params.remove(p)

    return spec

def main():
    url = "http://10.0.5.215:8080/openapi.json"
    response = requests.get(url)
    response.raise_for_status()

    openapi_spec = response.json()

    # Force top-level openapi to 3.0.0 if it's 3.1.0
    if "openapi" in openapi_spec and openapi_spec["openapi"].startswith("3.1"):
        openapi_spec["openapi"] = "3.0.0"

    # Ensure we have a servers list
    if "servers" not in openapi_spec:
        openapi_spec["servers"] = [{"url": "http://10.0.5.215:8080"}]
    else:
        if not openapi_spec["servers"]:
            openapi_spec["servers"].append({"url": "http://10.0.5.215:8080"})

    # Remove unwanted paths
    paths_dict = openapi_spec.get("paths", {})
    for path_to_remove in TO_REMOVE_PATHS:
        if path_to_remove in paths_dict:
            paths_dict.pop(path_to_remove, None)

    # Recursively convert from 3.1 to 3.0
    converted_spec = convert_3_1_to_3_0(openapi_spec)

    # **Add examples to certain parameters**:
    converted_spec = add_examples_to_params(converted_spec)

    # Write out final YAML
    with open("soccerverse.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(converted_spec, f, sort_keys=False, allow_unicode=True)

if __name__ == "__main__":
    main()
