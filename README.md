# Repositorio de Extraccion de Datos Crudos de la API de XM S.A. E.S.P.
---
En este repositorio estan los siguientes archivos fuente:
1.  ```extraccion_datos_api_xm.ipynb```: Donde se realiza la extracción de datos desde la API REST de XM S.A. E.S.P.
2.  ```pydataxm.py```: Permite realizar consultas a la API de manera más directa y sencilla, devolviendo un *DataFrame* de Pandas con los datos de cada variable consultada.
3.  ```carga_postgres_DigitalOcean.py```: Flujo de trabajo en Prefect que toma el dataset extraido desde la API de XM y lo carga en una base de datos postgres en Digital Ocean.





