import requests
from bs4 import BeautifulSoup


class Exractor():
    @staticmethod
    def get_cast(imdb_id : str):
    
        url = f'https://www.imdb.com/title/{imdb_id}/fullcredits'
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find('table', attrs={'class' : 'cast_list'})
        cast = table.find_all('a')   
        
        return re.findall(r'title="(.*?)"', str(cast))

    @staticmethod
    def get_movie_data(movie_object : dict):
        pass

    @staticmethod
    def get_cast(imdb_id : str):
    
        url = f'https://www.imdb.com/title/{imdb_id}/fullcredits'
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find('table', attrs={'class' : 'cast_list'})
        cast = table.find_all('a')   
        
        return re.findall(r'title="(.*?)"', str(cast))






