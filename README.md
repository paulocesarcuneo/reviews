# reviews

## Build & Start

```
docker-compose up --build
```

## Demo

Para ver el reporte abrir

http://localhost:8080/report

El reporte se mostrara vacio, o posiblemente un mensaje de que los servicios no estan habilitado,
en caso ultimo, refrescar el browser.

Para ver el rabbit (guest guest)

http://localhost:15672/

Para cargar datos se puede usar el script demo que recibe por parametro los archivo zipeados
y lanza un contendor de docker que carga el sistema.

```
./demo.sh <docker network name> <absolute path to business file.zip> <absolute path to reviews file.zip>
```

Si se invoca sin parametros, busca los archivos en :
"$(pwd)/yelp_academic_dataset_business.json.zip"
"$(pwd)/yelp_academic_dataset_review.json.zip"
y usa la network "reviews_default"

Durante la carga de datos o al final, se puede refrescar el reporte en el browser para ver los estados intermedios del reporte.
