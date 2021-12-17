## Airflow

Se utilizo la configuracion previa de airflow en Docker. 
Se creo un nuevo DAG, en el cual un archivo realiza el trigger a traves de un FileSensor(), ya que es necesario tener los 3 archivos (casos confirmados, recuperados y muertes)
para poder realizar el analisis y visualizacion de datos. El trigger realiza la lectura de los 3 archivos CSV, y con la funcion melt() de pandas, se indican como parametros
las columnas que se desean mantener, y el nombre que se le quiere dar a las columnas que se crearan a partir de las columnas que tiene el CSV, que en este caso eran las columnas
con la fecha y el valor en cada una, para lograr obtener una columna nueva de fechas, y una columna con el valor en cada fecha. Luego se realiza una coinexion a la base de datos,
se eliminan los datos existentes y se hace un insert del dataframe.

## Shiny Apps en R

Una vez creado el app, se realizo un archivo Dockerfile, en el cual se utiliza una imagen de rocker/verse, y se instalan las librerias correspondintes, asi como las dependencias 
y packages utilizados en R. La aplicacion corre en localhost, en el puerto 3838.
Para poder utilizar la base de datos del proyecto de Airflow en Shiny, se hace uso del network creado por el proyecto de airflow, con el nombre de airflow_default, y con el
comando docker run -d -p 3838:3838 --network airflow_default shinyproject se corre el proyecto dentro del network para tener acceso a la base de datos. Para hacer uso de la misma el host debe ser "172.22.0.3", no localhost. 
