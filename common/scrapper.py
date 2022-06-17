import requests
from bs4 import BeautifulSoup


class ScrapeCast():
    @staticmethod
    def get_cast_list(imdb_code: str):

        url = f'https://www.imdb.com/title/{imdb_code}/'

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        cast = {
            'actor': [actor.get_text() for actor in soup.find_all('a', class_='sc-11eed019-1 jFeBIw')],
            'cast': [role.get_text() for role in soup.find_all('span', class_='sc-11eed019-4 esZWnh')]
        }

        return cast




