import time
from csv import DictWriter
from multiprocessing.dummy import Pool

import requests
from lxml import html
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import Chrome, ChromeOptions

from data.exceptions import LoadTimeoutExpired


class Application:
    load_timeout: int = 5

    def __init__(self, urls, filename='output.csv'):
        self.urls = urls
        self.filename = filename
        self.fieldnames = ['Номер телефона', 'Имя', 'Раздел']
        self.delimiter = ';'
        self.phones = []
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                                      'Chrome/91.0.4472.124 Safari/537.36'}

    def validate_phone(self, phone: str):
        for dig in ' ()-':
            phone = phone.replace(dig, '')
        if phone[0] == '0' and len(phone) >= 10:
            phone = '+38' + phone
        if phone.startswith('+380') and phone not in self.phones:
            self.phones.append(phone)
            return phone

    def parse_product(self, driver: Chrome, url):
        driver.get(url)
        try:
            cookie_btn = driver.find_element_by_xpath('//button[@data-cy="dismiss-cookies-overlay"]')
        except NoSuchElementException:
            pass
        else:
            cookie_btn.click()
        output = {'Раздел': driver.find_elements_by_xpath(
            '//li[@data-testid="breadcrumb-item"]')[-1].find_element_by_tag_name('a').get_attribute('href')}
        try:
            phone_show_btn = driver.find_element_by_xpath('//button[@data-testid="show-phone"]')
            phone_show_btn.click()
            start_time = time.time()
            while True:
                try:
                    phone_section = driver.find_elements_by_xpath('//ul[@class="css-1478ixo"]'
                                                                  '/li[@class="css-1petlhy-Text eu5v0x0"]')
                    if not phone_section:
                        raise NoSuchElementException
                    phone = ','.join(
                        filter(lambda p: p,
                               [self.validate_phone(ph.text) for ph in phone_section]))
                    break
                except NoSuchElementException:
                    if time.time() - start_time > self.load_timeout:
                        raise LoadTimeoutExpired
        except LoadTimeoutExpired:
            start_time = time.time()
            while True:
                try:
                    phone_show_btn = driver.find_element_by_xpath('//button[@data-cy="ad-contact-phone"]')
                    phone_show_btn.click()
                    phone = self.validate_phone(phone_show_btn.text)
                    break
                except NoSuchElementException:
                    if time.time() - start_time > self.load_timeout:
                        print(f'Failed to parse {url}')
                        return None
        if not phone:
            return None
        output['Номер телефона'] = phone
        output['Имя'] = driver.find_element_by_xpath('//h2[@class="css-owpmn2-Text eu5v0x0"]').text
        print(output)
        return output

    def parse_page(self, response):
        doc = html.fromstring(response.text)
        links = doc.xpath('//h3[@class="lheight22 margintop5"]/a/@href')
        options = ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        driver = Chrome(options=options)
        for link in links:
            info = self.parse_product(driver, link)
            if info:
                yield info
        print()
        driver.quit()

    @staticmethod
    def get_last_page(text):
        doc = html.fromstring(text)
        try:
            return int(doc.xpath('//a[@data-cy="page-link-last"]/span/text()')[0])
        except (ValueError, IndexError):
            return None

    def process(self, task):
        page = task['from'] - 1
        params = task['params']
        if 'page' in params:
            params.pop('page')
        for _ in range(task['from'], task['to'] + 1):
            page += 1
            response = requests.get(task['url'], params={'page': page, **params}, headers=self.headers)
            if not response:
                print(f'Failed to get {params["url"]}?page={page}')
                continue
            for row in self.parse_page(response):
                with open(self.filename, 'a', encoding='utf-8', newline='') as csv_file:
                    writer = DictWriter(csv_file, fieldnames=self.fieldnames, delimiter=self.delimiter)
                    writer.writerow(row)

    def start(self):
        for url in self.urls:
            params = {'page': 1}
            response = requests.get(url, params=params, headers=self.headers)
            if not response:
                return f'Failed to get {url}?page={params["page"]}'
            doc = html.fromstring(response.text)
            try:
                pages = int(doc.xpath('//a[@data-cy="page-link-last"]/span/text()')[0])
            except (IndexError, ValueError):
                return f'Failed to get last page of {url}'
            with open(self.filename, 'w', encoding='utf-8', newline='') as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.fieldnames, delimiter=self.delimiter)
                writer.writeheader()
            pages_per_process = 5
            tasks = [{'url': url, 'from': p, 'to': p + pages_per_process if p + pages_per_process <= pages
                      else p + (pages - p), 'params': params}
                     for p in range(1, pages + 1
                                    if pages / pages_per_process > pages // pages_per_process
                                    else pages, pages_per_process)]
            pool = Pool(processes=len(tasks))
            pool.map(self.process, tasks)
