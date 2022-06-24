import requests
from bs4 import BeautifulSoup


class Exractor():
    @staticmethod
    def get_cast_list(imdb_code: str):
        """This methods generates 

        Args:
            imdb_code (str): _description_

        Returns:
            _type_: _description_
        """

        url = f'https://www.imdb.com/title/{imdb_code}/fullcredits'

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        cast = {
            'actor': [actor.get_text() for actor in soup.find_all('a', class_='sc-11eed019-1 jFeBIw')],
            'cast': [role.get_text() for role in soup.find_all('span', class_='sc-11eed019-4 esZWnh')]
        }

        return cast

    @staticmethod
    def get_movie_data(movie_object : dict):
        pass






