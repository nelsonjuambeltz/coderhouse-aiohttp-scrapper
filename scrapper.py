import requests
import csv
import asyncio
import aiohttp
from bs4 import BeautifulSoup

TARGET_URL = 'https://exportargentina.org.ar/companies/'

def parse_company_details(response):
    url, status, response = response
    company_id = url.split("/")[-1]
    if (status == 'OK'):
        soup = BeautifulSoup(response, 'html.parser')
        details = soup.find(id="company-details")
        name = soup.title.text.replace(" - ExportArgentina.org.ar", "")
        exporter_level = details.div.div.div.div.find(class_="col-md-8").p.text

        text_muted = soup.find_all(class_="text-muted")
        contact_info = text_muted[0].text.split("\n")
        if "restantes" in contact_info[1].strip():
            contact_info = text_muted[1].text.split("\n")
        try:
            contact_email = soup.find(id="contact_email").text.strip()
        except Exception:
            contact_email = None
        try:
            public_email = soup.find(id="public_email").text.strip()
        except Exception:
            public_email = None
        try:
            phone_number = soup.find(id="contact_phone").text.strip()
        except Exception:
            phone_number = None
        try:
            address = text_muted[1].text.strip()
            if "Mostrar correo electrónico" in address:
                address = text_muted[2].text.strip()
        except Exception:
            address = None
        try:
            phone_number = text_muted[4].text.split("\n")[2].strip()
        except Exception:
            phone_number = None
        try:
            contact_name = contact_info[1].strip()
        except Exception:
            contact_name = None
        try:
            contact_position = contact_info[2].strip()
        except Exception:
            contact_position = None
        try:
            links = soup.find_all("a")
            sectors = [link.text for link in links if link['href'].startswith("https://exportargentina.org.ar/companies?category_id=")]
            if not sectors:
                sector = None
            else:
                sector = sectors
        except Exception as e:
            sector = None

        return {
            "id": company_id,
            "url": url,
            "sector": sector,
            "name": name,
            "contact_name": contact_name,
            "contact_position": contact_position,
            "contact_email": contact_email,
            "public_email": public_email,
            "address": address,
            "phone": phone_number,
            "level": exporter_level
        }
    else:
        return None

def collect_data(fetch_number, concurrent_requests):
    with open('data.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(["id","url","sector","nombre","contacto","posicion contacto","email_contacto","email publico","direccion","telefono","nivel exportador"])
        start = 8
        for i in range(fetch_number,concurrent_requests):
            id_list = list(range(i, start+concurrent_requests))
            url_list = [TARGET_URL + str(company_id) for company_id in id_list]
            loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)
            task = asyncio.ensure_future(run(url_list))
            loop.run_until_complete(task)
            request_list = task.result().result()
            for details in request_list:
                details = parse_company_details(i)
                if details:
                    spamwriter.writerow(details.values())

async def fetch(url, session):
    
    try:
        async with session.get(
            url,
            ssl = False, 
            timeout = aiohttp.ClientTimeout(
                total=None, 
                sock_connect = 10, 
                sock_read = 10
            )
        ) as response:
            if response.status == 200:
                content = await response.text()
                return (url, 'OK', content)
            else:
                return (url, 'ERROR', None)
    except Exception as e:
        print(e)
        return (url, 'ERROR', str(e))

async def run(url_list):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in url_list:
            task = asyncio.ensure_future(fetch(url, session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses
    return responses

def test():
  ids = list(range(20,30))
  url_list = [TARGET_URL + str(company_id) for company_id in ids]
  loop = asyncio.get_event_loop()
  asyncio.set_event_loop(loop)
  task = asyncio.ensure_future(run(url_list))
  loop.run_until_complete(task)
  results = task.result().result()
  return results



if __name__ == "__main__":
    start = 2422
    concurrent_requests = 3000
    with open('data.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(["id","url","sector","nombre","contacto","posicion contacto","email_contacto","email publico","direccion","telefono","nivel exportador"])
        id_list = list(range(start, start+concurrent_requests))
        url_list = [TARGET_URL + str(company_id) for company_id in id_list]
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        task = asyncio.ensure_future(run(url_list))
        loop.run_until_complete(task)
        request_list = task.result().result()
        for response in request_list:
            details = parse_company_details(response)
            if details:
                spamwriter.writerow(details.values())
