import yaml
import pandas as pd

class ProcessParams():
    def get_track_ids(meta_path:str) -> list:
        meta_file = pd.read_json(meta_path)
        if len(meta_file['track_ids']) == 0:
            return None
        else:
            return list(meta_file['track_ids'])

    @staticmethod
    def update_track_ids(meta_path: str, track_id: list) -> None:
        meta_file = pd.read_json(meta_path)
        meta_file['track_ids'] = track_id
        meta_file.to_json(meta_path)

    @staticmethod
    def validate(movie:dict, key:str):
        if key in movie.keys():
            return movie.get(key)
        else:
            return 'None'