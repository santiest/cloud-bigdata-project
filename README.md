# cloud-bigdata-project

Nota: recordad instalar dotenv con el siguiente comando
```
python -m pip install python-dotenv
```

## Dataset:
```
https://www.kaggle.com/datasets/dilwong/flightprices
```

## PySpark en Google Cloud
Primero, hay que dar los permisos de "Propietario de buckets heredados de almacenamiento" y "Propietario de objetos heredados de almacenamiento" a la cuenta de servicios que use vuestro cluster.

Hay que encontrar el email de la cuenta, que está en los detalles del master.

![image](https://user-images.githubusercontent.com/45616341/206900913-726c0f12-0f9a-44eb-b273-1ef4ab60f895.png)

Luego, ya podremos acceder al bucket con nuestro cluster.

### Comandos para usar PySpark
Como configuración inicial, necesitamos correr los siguientes comandos en la consola SSH del cluster:
```
BUCKET=gs://pacolo2
export FILENAME=$BUCKET/itineraries.csv
export OUTPUT_DIR=$BUCKET/scripts_output/
```

Finalmente, para correr scripts:
```
spark-submit --py-files $BUCKET/env_wrapper.py,$BUCKET/schema.py $BUCKET/scripts/example.py
```