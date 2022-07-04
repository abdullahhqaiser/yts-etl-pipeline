import yaml


class ProcessParams():
    @staticmethod
    def get_track_ids(params_path: str) -> list:
        with open(params_path) as f:
            file = yaml.safe_load(f)

        return file['track_id']

    @staticmethod
    def update_track_ids(params_path: str, track_id: list) -> None:
        with open(params_path) as f:
            file = yaml.safe_load(f)

        file['track_id'] = track_id

        with open(params_path, 'w') as f:
            yaml.dump(file, f)
