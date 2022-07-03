import yaml

with open('params.yaml') as f:
    file = yaml.safe_load(f)

track_id = [2154, 3333]

file['track_id'] = track_id

with open('params.yaml', 'w') as f:
    yaml.dump(file, f)



