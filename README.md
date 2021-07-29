# metros-cubicos-crawler
### Webscrapping para poblar una base datos de casas

#### <b>1. Requerimientos </b>
``` shell
pip3 install aiohttp
pip3 install requests
pip3 install nest-asyncio
pip3 install beautifulsoup4
```
#### <b>2. Ejecución </b>
```shell
python3 adventure.py 
```
Cuenta con dos parametros opcionales
```shell
optional arguments:
  -h, --help            show this help message and exit
  -e ELEMENTS, --elements ELEMENTS
    number of elements to extract

  -s STATE, --state STATE
    state you want to extract
```
Ejemplo:
```shell 
python3 adventure.py -e 278 -s "Distrito Federal"
```
#### NOTAS:
* Agregue un campo que es de moneda ya que noté que algunos precios los marcaba en dolares

* El campo <i>county</i> lo elimine ya que en general la direccion de las casas solo tienen cuatro datos: calle,número(en algunos casos),colonia,delegacion/municipio,estado.

* Para obtener la informacion de amenidades requeriere de un click por parte del usuario. En ese caso se podría usar la biblioteca selenium sin embargo pienso que aumenta el tiempo de ejecución. Es por eso que lo elimine.