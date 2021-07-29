import enum
from types import prepare_class
from aiohttp.helpers import current_task
from attr import dataclass
from bs4 import BeautifulSoup as bs, element
import aiohttp
import asyncio
import threading as thd
import re
import sys
import os
import argparse
from numpy import add, floor, insert, select
import sqlite3

class metrosCubicos(object):

    def __init__(self,state,elements):
        self.state = state
        self.elements = elements
        self.urls = None
        self.url_casas = []
        self.all_data = []
        self.master_dict = {}
        self.states=[]
        self.streets=[]
        self.settlements=[]
        self.towns = []
        self.numbers = []
        self.prices = []
        self.descriptions = []
        self.pictures = []
        self.currencies = []
        self.sizes = []
        self.links = []
        self.names = []

    async def create_urls(self):
        """
        Create a list of urls of pages to search info based on
        how many houses you want extract the info

        Args:
            state (string): state of the republic from you want to search houses
            elements (int): number of houses that you want to get info

        Returns:
            list: list of urls of pages to search houses
        """

        range_ = 50
        factor = floor(self.elements / range_)
        print(factor)
        urls = []
        self.state = self.state.lower()
        self.state = self.state.replace(" ","-")
        
        url = "https://inmuebles.metroscubicos.com/casas/venta/"+self.state
        urls.append(url)
    
        if self.elements > 48:
            for i in range(int(factor+1)):
                pattern = "/_Desde_" + str(50*(i+1)+1)
                urls.append(url+pattern)
        self.urls = urls
    
    async def fetch_urls_houses(self,session,url):
        """Obtain the urls for houses from one url
           searching page of metros cubicos

        Args:
            session: asynchronous http session
            url (string): url of the catalogue page in metros cubicos

        Returns:
            text: html raw string
            url: url of the catalogue page in metros cubicos
            house_urls = links to the houses pages
        """
        try:
            async with session.get(url) as response:
                text = await response.text()
                houses_urls = await self.extract_url_houses(text)
                return text, url, houses_urls
        except Exception as e:
            print(str(e))
            
    async def extract_url_houses(self,text):
        """Extract the urls of the house pages

        Args:
            text (string): html of the catalogue page in metros cubicos

        Returns:
            urls: list of urls of houses
        """
        try:
            soup = bs(text,'html.parser')
            casas = soup.find_all('li',class_="ui-search-layout__item")
            urls = list(map(lambda casa:casa.a.get("href"),casas))
            return urls
        

        except Exception as e:
            print(str(e)) 

    async def get_url_houses(self):
        """ Extends the list of urls of houses
            extracted from catalogues pages
        """

        tasks = []
        await self.create_urls()
        async with aiohttp.ClientSession() as session:
            for url in self.urls:
                tasks.append(self.fetch_urls_houses(session,url))
            
            htmls = await asyncio.gather(*tasks)
            for html in htmls:
                self.url_casas.extend(html[2])
            self.all_data.extend(htmls)

    async def fetch_house_info(self,session,url):
        """Obtain and set the data of one house page

        Args:
            session : asynchronous http session
            url (string): url of one house page

        Returns:
            text: html string of the page
            url : url of one house page
        """
        try:
            async with session.get(url) as response:
                text = await response.text()
                await self.extract_house_data(text,url)
                return text, url
        except Exception as e:
            print(str(e))    

    async def extract_house_data(self,text,url):
        """Sets the data of the house page

        Args:
            text (string): html of one house page
            url (string): url of the page
        """

        try:
            soup = bs(text,"html.parser")
            name = soup.find("h1",class_="ui-pdp-title").text
            address = soup.find("div",class_="ui-vip-location").find("p").text
            priceTag = soup.find("span",class_="price-tag-text-sr-only")
            price = priceTag.text.split(" ")[0]
            currency = priceTag.text.split(" ")[1]
            desc = soup.find("p",class_="ui-pdp-description__content").text.replace("'","")
            sizeTag = soup.find("div",class_="ui-pdp-container__row ui-pdp-container__row--highlighted-specs-res")
            size = sizeTag.find("div","ui-pdp-highlighted-specs-res__icon-label").text.split(" ")[1]
            firs_image = soup.find("figure",class_="ui-pdp-gallery__figure")
            url_first_img = next(firs_image.children).get("src")

            # Process address
            arrayAddress = address.split(",")
            if len(arrayAddress) > 1 :
                state = arrayAddress[-1]
                town = arrayAddress[-2]
                if len(arrayAddress) > 2 :
                    settlement = arrayAddress[-3]
                else:
                    settlement = "NULL"

                if len(arrayAddress) > 3:
                    street = arrayAddress[-4]
                    number = re.search(r'\d+',street)
                    if number != None:
                        number = number.group()
                    else:
                        number = "NULL"
                else:
                    street = "NULL"
                    number = "NULL"
                    
            else:
                state = address
                town = "NULL"
                settlement = "NULL"
                street = "NULL"
                number = "NULL"

            # address
            self.states.append(state)
            self.towns.append(town)
            self.settlements.append(settlement)
            self.numbers.append(number)
            self.streets.append(street)
            
            self.names.append(name)
            self.descriptions.append(desc)
            self.prices.append(price)
            self.currencies.append(currency)
            self.sizes.append(size)
            self.pictures.append(url_first_img)
            self.links.append(url)


        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)


    async def generate_house_data(self,elements):
        """Set all the data of all houses

        Args:
            elements (elements): number of house to search of
        """
        tasks = []
        async with aiohttp.ClientSession() as session:
            for url in self.url_casas[0:elements]:
                tasks.append(self.fetch_house_info(session,url))
            
            htmls = await asyncio.gather(*tasks)
            

class metrosCubicosDB():
    def __init__(self,name="DataBase",file=None):
        
        if len(name.split(".")) > 1:
            self.con = sqlite3.connect(name)
        else:
            self.con = sqlite3.connect(name+".db")
        self.cur = self.con.cursor()
        if file != None:
            print("Creating data base from script")
            sqlScript = open(file)
            sql_as_string = sqlScript.read()
            self.cur.executescript(sql_as_string)
        
    def insert_data(self,data):
        """Populate the database of houses with data

        Args:
            data (dictionary): dictionary containing info of the houses
        """
        self.insert_address(data)

        k = len(data["streets"])
        address = {}
        sentence = "INSERT INTO property (idstreet,idsettlement,idtown,idstate,idcurrency,number,name,url,price,description,size,first_picture)"

        for i in range(k):
            address["street"] = data["streets"][i]
            address["state"] = data["states"][i]
            address["town"] = data["towns"][i]
            address["settlement"] = data["settlements"][i]
            idaddress, idcurrency = self.query_ids(address,data["currencies"][i])
            
            valuesSentence = " VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(idaddress["street"],
                idaddress["settlement"],
                idaddress["town"],
                idaddress["state"],
                idcurrency,
                data["numbers"][i],
                data["names"][i],
                data["links"][i],
                data["prices"][i],
                data["descriptions"][i],
                data["sizes"][i],
                data["pictures"][i]
            )
            #print(sentence+valuesSentence)
            try:
                self.cur.execute(sentence+valuesSentence)
            except Exception as e:
                print(e)
                print(sentence+valuesSentence)
            print("{}/{} row(s) inserted".format(i,k))
                    
        self.con.commit()

    def query_ids(self,address,currency):
        """ get the ids of the tables state, town,
            street,settlement and currency

        Args:
            address (dictionary): data related to the address of the house
            currency (string): currency string

        Returns:
            idaddress (dictionary): data containing the ids related with the address
            idcurrency (dictionary): id of the currency
        """

        idaddress = {}
        state = address["state"].lower()
        town = address["town"].lower()
        street = address["street"].lower()
        settlement = address["settlement"].lower()
        currency = currency.lower()


        row = self.cur.execute("SELECT id FROM state WHERE state='{}'".format(state))
        idaddress["state"] = row.fetchone()[0]

        row = self.cur.execute("SELECT id FROM town WHERE town='{}'".format(town))
        idaddress["town"] = row.fetchone()[0]

        row = self.cur.execute("SELECT id FROM street WHERE street='{}'".format(street))
        idaddress["street"] = row.fetchone()[0]

        row = self.cur.execute("SELECT id FROM settlement WHERE settlement='{}'".format(settlement))
        idaddress["settlement"] = row.fetchone()[0]

        row = self.cur.execute("SELECT id FROM currency WHERE currency='{}'".format(currency))
        idcurrency = row.fetchone()[0]

        
        return idaddress,idcurrency



    def insert_address(self,data):
        """populate address tables and currency table
        Args:
            data (dict): dict with the elements
             {street,settlement,town,state,
             currency}
        """
        # check street, settlement,town,state,currency fields
        k = len(data["states"])

        for i in range(k):
            print("[INFO] {}/{} data processed".format(i,k),end='\r')
            state = data["states"][i]
            state = state.lower()
            
            if len(self.cur.execute("SELECT * FROM state WHERE state='{}'".format(state)).fetchall()) == 0:
                self.cur.execute("INSERT INTO state (state) VALUES ('{}')".format(state))
                self.con.commit()

            settlement = data["settlements"][i]
            settlement = settlement.lower()

            if len(self.cur.execute("SELECT * FROM settlement WHERE settlement='{}'".format(settlement)).fetchall()) == 0:
                self.cur.execute("INSERT INTO settlement (settlement) VALUES ('{}')".format(settlement))
                self.con.commit()
            
            town = data["towns"][i]
            town = town.lower()
            if len(self.cur.execute("SELECT * FROM town WHERE town='{}'".format(town)).fetchall()) == 0:
                
                self.cur.execute("INSERT INTO town (town) VALUES ('{}')".format(town))
                self.con.commit()
            

            street = data["streets"][i]
            street = street.lower()
            if len(self.cur.execute("SELECT * FROM street WHERE street='{}'".format(street)).fetchall()) == 0:
                
                self.cur.execute("INSERT INTO street (street) VALUES ('{}')".format(street))
                self.con.commit()
            
            currency = data["currencies"][i]
            street = currency.lower()
            if len(self.cur.execute("SELECT * FROM currency WHERE currency='{}'".format(currency)).fetchall()) == 0:
                self.cur.execute("INSERT INTO currency (currency) VALUES ('{}')".format(currency))
                self.con.commit()

 

def main(args):

    print("elements to extract {}".format(args.elements))
    #urls = ["https://inmuebles.metroscubicos.com/casas/venta/distrito-federal/"]
    scraper = metrosCubicos(args.state,args.elements)
    asyncio.run(scraper.get_url_houses())
    #for i,url in enumerate(scraper.url_casas):
    #    print("[{}] ",i,url)
    asyncio.run(scraper.generate_house_data(args.elements))

    newDB = metrosCubicosDB("properties2.db","casas.db.sql")


    #Insert the data extracted into the database
    newDB.insert_data(scraper.__dict__)
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-e","--elements",help="number of elements to extract",default=150,type=int,required=False)
    parser.add_argument("-s","--state",help="state you want to extract",default="Distrito Federal",required=False)
    args = parser.parse_args()
    main(args)