# reviews

## Build & Start

```
docker-compose up --build
```

## Demo

Para cargar datos se puede usar el script demo que recibe por parametro los archivo zipeados
y lanza un contendor de docker que carga el sistema.

```
./demo.sh <absolute path to business file.zip> <absolute path to reviews file.zip>
```

Si se invoca sin parametros, busca los archivos en :
"$(pwd)/yelp_academic_dataset_business.json.zip"
"$(pwd)/yelp_academic_dataset_review.json.zip"
