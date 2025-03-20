
# source: https://stackoverflow.com/questions/26393231/using-python-requests-with-javascript-pages

from io import StringIO
from requests_html import HTMLSession, AsyncHTMLSession
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from time import time
import json
from bs4 import BeautifulSoup
import asyncio

def replace_key(key: str):
    key = key.replace(':', '').replace('#', '').replace(' ', '').lower()
    
    match key:
        case 'ac\ntype':
            return 'actype'
        case 'cn/ln':
            return 'cn_ln'
        case _:
            return key
        
async def record_page(url):
    session = AsyncHTMLSession()
    r = await session.get(url)
    await r.html.arender()
    soup = BeautifulSoup(r.html.html, "html.parser")

    # Find the first table
    table = soup.find("table")
    if not table:
        return dict([])
    
    # Extract rows
    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            cells[0] = replace_key(cells[0])
            cells[1] = str(cells[1]).replace("\xa0", '')
            if cells[0] == 'date':
                cells[1] = f"{cells[1]}"

            rows.append(cells)

    return dict(rows)

async def get_rec(year):
    base_url = f"https://www.planecrashinfo.com/{year}"
    # Fetch the HTML content
    url = f"{base_url}/{year}"
    session = AsyncHTMLSession()
    r = await session.get(f"{url}.htm")
    await r.html.arender()  # this call executes the js in the page

    soup = BeautifulSoup(r.html.html, "html.parser")

    # Find the first table
    table = soup.find("table")

    if table is None:
        return None

    # Extract headers
    headers = [td.text.strip() for td in table.find_all("tr")[0]]

    # Extract rows
    rows = []
    cnt = 0
    # print(table)
    for tr in table.find_all("tr")[1:]:  # Skip header row
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            rows.append(cells)
            cnt += 1

    # # Convert to DataFrame
    # df = pd.DataFrame(rows, columns=headers)

    # # Display DataFrame
    # df.head()
    
    urls = []
    tasks = []
    for i in range(1, cnt+1):
        urlStr = f"{url}-{i}.htm"
        urls.append(urlStr)
        tasks.append(record_page(urlStr))

    # Testing...
    # results = await record_page(urls[0])
    
    # with ThreadPoolExecutor() as executor:
    #     results = executor.map(record_page, urls)

    results = await asyncio.gather(*tasks)

    await asyncio.sleep(10)  
    
    # print(results)
    return results


years = range(1990, 2001)
# years  = [1990]
for yr in years:
    arr = []
    start = time()
    # records = get_rec(yr)
    records = asyncio.run(get_rec(yr))

    if records is None:
        print(f'failed: {yr}')
    else:
        print(yr)
        for rec in records:
            # print(rec)
            arr.append(rec)

        json_string = json.dumps(arr)
        df = pd.read_json(StringIO(json_string))

        print('total time {:.2f}s'.format(time() - start))    

        df.to_json(
            f'output/{yr}.json', 
            orient = 'records', 
            # lines=True
        )
