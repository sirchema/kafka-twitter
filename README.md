# kafka-twitter
Proyecto personal para probar con colas kafka, tenemos un productor y un consumidor. El productor genera twits usando la librería Twitter4j que son puestos en una cola 
con el topic rawtweets, luego se consumirán estos twits usando Kafka Stream y se contabilizarán los hashtag.

### Pre-requisitos
Hay que pasar como argumentos a la aplicación la api key, api secret, token value y token secret de tu cuenta de desarrollador de twitter.
