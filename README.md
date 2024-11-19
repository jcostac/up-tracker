# UPTracker

UPTracker es una aplicación web desarrollada para el análisis de unidades de programación.

## Python
Son necesarios los requerimientos descritos en **requeriments.txt**

## Configuración

En el fichero code/back/config es necesario configurar las siguientes variables:
- **carpeta_data_lake**: indicando donte está la carpate data. Ejemplo: 
- **fichero_config**: indicando la "ruta\\code\\back\\utilidades\\config.yml"


### Demonio
Configurar correctamente las rutas de **daemon.bat**

### Aplicación
Configurar correctamente las rutas de **app.bat**

### Front
Si vamos a lanzar el front desde el **servidor de pruebas de NPM** es necesario lo siguiente:

- Con el CMD navegar hasta code/frontvue
- Ejecutar npm install para instalar las dependencias. Se creará la carpeta **code/frontvue/node_modules** pero esta la ignoramos para el repo
- Ejecutar **npm run serve** para lanzar el front en el servidor de pruebas
- Desde el navegador ir a http://127.0.0.1:8080/