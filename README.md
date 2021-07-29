# metros-cubicos-crawler
### Webscrapping para poblar una base datos de casas

#### Requerimientos
``` shell
pip install aiohttp
pip install requests
pip install nest-asyncio
pip install beautifulsoup4
```

#### NOTAS:
* Agregue un campo que es de moneda ya que noté que algunos precios los marcaba en dolares

* El campo <i>county</i> lo elimine ya que en general la direccion de las casas solo tienen cuatro datos: calle,número(en algunos casos),colonia,delegacion/municipio,estado.

* Para obtener la informacion de amenidades requeriere de un click por parte del usuario. En ese caso se podría usar la biblioteca selenium sin embargo pienso que aumenta el tiempo de ejecución. Es por eso que lo elimine.