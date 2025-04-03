import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os

# URL da página com os anexos
url = 'https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos'

# Faz a requisição da página
response = requests.get(url)
response.raise_for_status() 
soup = BeautifulSoup(response.text, 'html.parser')

output_dir = "pdf_anexos"

# Procura links que contenham '.pdf' no atributo href
pdf_links = [a['href'] for a in soup.find_all('a', href=True) if '.pdf' in a['href'].lower()]

pdf_links = pdf_links[:2]

for link in pdf_links:
    # Se o link for relativo, converte para URL absoluta
    pdf_url = urljoin(url, link)
    file_name = os.path.join(output_dir, pdf_url.split("/")[-1])
    
    print(f"Baixando: {pdf_url}")
    r = requests.get(pdf_url)
    r.raise_for_status()
    with open(file_name, 'wb') as f:
        f.write(r.content)
    print(f"Salvo em: {file_name}")
