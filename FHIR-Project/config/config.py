import json

config_path = "/Volumes/workspace/default/fhir_lakehouse/config/config.json"

with open(config_path, "r") as f:
    config = json.load(f)

print("✅ Loaded config:", config["api"]["base_url"])

