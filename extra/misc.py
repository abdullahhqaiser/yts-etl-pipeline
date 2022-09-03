import yaml
import pandas as pd
import json


class Helpers():
    def get_track_ids(meta_path: str) -> list:
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
    def validate(movie: dict, key: str):

        if key in movie.keys():

            return movie.get(key)

        elif key == 'date_uploaded':
            return movie['torrents'][0].get(key)
        else:
            return 'None'

    @staticmethod
    def save_meta_data(metapath:str, ids:list, page_no:int):
        print('saving meta data.................................')
        with open(metapath, 'r') as f:
            file = json.load(f)


        file['track_ids'] = ids
        file['page_no']   = page_no
        print(file)

        with open(metapath, 'w') as f:
            json.dump(file, f)
        
