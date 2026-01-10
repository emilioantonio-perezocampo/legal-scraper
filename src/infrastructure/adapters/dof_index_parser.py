from bs4 import BeautifulSoup
from typing import List, Dict

BASE_URL = "https://dof.gob.mx/"

def parse_dof_index(html_content: str) -> List[Dict[str, str]]:
    """
    Scans the daily summary page and returns a list of dictionaries 
    containing the URL and the TITLE of each document.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    items = []

    # DOF links to articles usually have class "enlaces"
    links = soup.find_all('a', class_='enlaces')

    for link in links:
        href = link.get('href')
        text = link.get_text(" ", strip=True) # Get the link text (The Title)
        
        if href and "nota_detalle.php?codigo=" in href:
            # Handle relative paths
            if not href.startswith("http"):
                full_url = BASE_URL + href
            else:
                full_url = href
            
            # Create a robust item with both URL and Title
            item = {
                "url": full_url,
                "title": text
            }
            
            # Basic deduplication
            if item not in items:
                items.append(item)
                
    return items