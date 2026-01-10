import re
from datetime import datetime
from bs4 import BeautifulSoup
from src.domain.entities import FederalLaw, Article

def parse_dof_html(html_content: str) -> FederalLaw:
    """
    Parses DOF HTML. Supports both 'Laws' (Structured) and 'Notices' (Unstructured).
    """
    soup = BeautifulSoup(html_content, 'lxml')

    # --- 1. Extract Date ---
    # Strategy A: Look for explicit ID (Common in Laws)
    pub_date = None
    date_tag = soup.find('span', id='lblFecha')
    
    # Strategy B: Look for "DOF: dd/mm/yyyy" text (Common in Notices)
    if not date_tag:
        # Find any text containing "DOF:" followed by numbers
        date_text_tag = soup.find(string=re.compile(r"DOF:\s*\d{2}/\d{2}/\d{4}"))
        if date_text_tag:
            date_tag = date_text_tag

    if date_tag:
        try:
            # Clean up string: "DOF: 04/03/2021" -> "04/03/2021"
            raw_date = date_tag.get_text(strip=True).replace("DOF:", "").strip()
            pub_date = datetime.strptime(raw_date, "%d/%m/%Y").date()
        except ValueError:
            pass

    # --- 2. Extract Title ---
    title = "Unknown Title"
    
    # Strategy A: Standard Law Title
    header_tag = soup.find('h3', class_='titulo')
    if header_tag:
        title = header_tag.get_text(strip=True)
    else:
        # Strategy B: First Bold Centered Text (Common in Notices)
        # We look inside the main content div
        content_div = soup.find('div', id='DivDetalleNota')
        if content_div:
            # Find the first bold tag inside a centered paragraph
            # The HTML you sent has: <p align="center">...<b>INSTITUTO...</b></p>
            first_bold = content_div.find('b')
            if first_bold:
                title = first_bold.get_text(strip=True)

    # --- 3. Extract Articles / Content ---
    articles = []
    
    # Locate the main container
    content_div = soup.find('div', id='DivDetalleNota')
    if content_div:
        # Strategy A: Explicit "Article" divs
        article_divs = content_div.find_all('div', class_='Articulo')
        
        if article_divs:
            for index, div in enumerate(article_divs, 1):
                text = div.get_text(" ", strip=True)
                identifier = f"Art. {index}"
                if ".-" in text:
                    parts = text.split(".-", 1)
                    identifier = parts[0] + ".-"
                    content = parts[1]
                else:
                    content = text
                
                articles.append(Article(identifier=identifier, content=content.strip(), order=index))
        else:
            # Strategy B: Fallback for Notices (just grab paragraphs)
            # This handles the ISSSTE document you scraped
            paragraphs = content_div.find_all('p', align='justify')
            for index, p in enumerate(paragraphs, 1):
                text = p.get_text(strip=True)
                if len(text) > 10: # Ignore empty lines
                    articles.append(Article(
                        identifier=f"Paragraph {index}", 
                        content=text, 
                        order=index
                    ))

    return FederalLaw(
        title=title,
        publication_date=pub_date,
        jurisdiction="Federal",
        articles=articles
    )