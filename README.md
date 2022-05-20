# HaterCrawl
HaterCrawl es una herramienta para el rastreo de la web. Basada en la clasificación de cada texto de un recurso con un modelo de clasificación basado en BERT


Para el uso del rastreador se puede iniciar mediante:
python HaterCrawl.py -n <nombre> -r <results_file> -l <log_file> -d <database_name> -p <prioridad> --inc_w <intervalo_espera> --inc_n <numero_de_intervalos> -s <archivo_seeds> -m <directorio_modelo> 
  
 Donde:
  - <nombre> Se usara este nombre para construir la base de datos, el archivo de resultados y el log. Por defecto: "hatercrawl"
  - <prioridad> Prioridad a elegir entre: "bfs", "unvisited_domains", "link_surroundings", "link_domain_combined", "antbased". Por defecto, "link_domain_combined"
  - <intervalo_espera> Para la toma de resultados de ejecucion, intervalo de espera entre cada toma.
  - <intervalo_salida> Para la toma de resultados de ejecucion, número de intervalos que tomar.
  - <archivo_seeds> Archivo del que leer las páginas seed, por defecto: "seeds.txt"
  - <results_file> Archivo csv en el que guardar los resultados. Por defecto "hatercrawl.csv"
  - <database_name> Nombre de la base de datos a usar. Por defecto "hatercrawl"
  - <log_file> Archivo en el que guardar el registro de ejecución. Por defecto "hatercrawl"
  - <directorio_modelo> Directorio en el que se encuentra el modelo a usar. Por defecto "./model"
  
  
