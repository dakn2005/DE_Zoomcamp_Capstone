import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from time import time
import json

def replace_key(key: str):
    key = key.replace(':', '').replace('#', '').replace(' ', '').lower()
    
    match key:
        case 'ac\ntype':
            return 'actype'
        case 'cn/ln':
            return 'cn_ln'
        case _:
            return key
        
def record_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the first table
    table = soup.find("table")
    
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

def get_rec(year):
    base_url = f"https://www.planecrashinfo.com/{year}"
    # Fetch the HTML content
    url = f"{base_url}/{year}"
    response = requests.get(f"{url}.htm", headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    })
    # response = requests.get('https://example.com')
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the first table
    table = soup.find("table")

    if table is None:
        return None

    # Extract headers
    headers = [td.text.strip() for td in table.find_all("tr")[0]]

    # Extract rows
    rows = []
    cnt = 0
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
    for i in range(1, cnt+1):
        urls.append(f"{url}-{i}.htm")

    # results = record_page(urls[0])
    
    with ThreadPoolExecutor() as executor:
        results = executor.map(record_page, urls)
    
    print(results)
    return results


years = range(1970, 2001)
for yr in years:
    arr = []
    start = time()
    records = get_rec(yr)

    if records is None:
        print(f'failed: {yr}')
    else:
        print(yr)
        for rec in records:
            # print(rec)
            arr.append(rec)

        json_string = json.dumps(arr)
        df = pd.read_json(json_string)

        print('total time {:.2f}s'.format(time() - start))    
        # print(arr[0])

        # df.head()
        df.to_json(
            f'output/{yr}.json', 
            orient = 'records', 
            # lines=True
        )