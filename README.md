# 🧠 Proyecto ETL - Análisis de Casos de Dengue

### Maestría en Inteligencia Artificial y Ciencia de Datos  
**Autores:** García Muñoz, Marko D; Herrera Rivera, Jorge A; Torres Polanco, Cristhian A  

---

## 📌 Descripción del Proyecto
Este proyecto implementa un proceso **ETL (Extracción, Transformación y Carga)** para la gestión y análisis de datos relacionados con los **casos de dengue en Colombia**.  
El desarrollo se realizó en **Python**, utilizando **SQLite** como base de datos y una arquitectura modular que garantiza trazabilidad, control y reproducibilidad.  

Su propósito es consolidar, limpiar y cargar información sanitaria de manera eficiente, proporcionando una base sólida para análisis exploratorios posteriores.

---

## 🧱 Estructura del Proyecto

PROYECTO_ETL/
│
├── consultas/ # Scripts SQL para creación de tablas
│ ├── datos_dengue.sql
│ └── etl_monitor.sql
│
├── data/ # Datos fuente y base de datos
│ ├── github_files/
│ ├── dbProject.db
│
├── logs/ # Registros de ejecución
│ └── etl.log
│
├── src/ # Código fuente principal del proyecto
│ ├── extract.py # Extracción de datos
│ ├── transform.py # Limpieza y transformación
│ ├── load.py # Carga en SQLite
│ ├── logger.py # Registro de eventos
│ ├── monitor.py # Monitoreo del proceso ETL
│ ├── schema.py # Definición de estructuras de tablas
│ ├── main.py # Orquestador del flujo ETL
│ └── test.py # Pruebas unitarias (internas)
│
├── EDA_Dengue.ipynb # Notebook con análisis exploratorio de datos
├── environment.yaml # Configuración del entorno con Conda
├── Informe Segunda Entrega Proyecto ETL.pdf # Informe técnico del laboratorio
├── .env # Variables de entorno
├── .gitignore # Exclusiones para control de versiones
└── README.md # Documentación del proyecto


---

## ⚙️ Flujo ETL

El proceso sigue las tres etapas clásicas del ciclo **ETL:**

### 1. Extracción (`extract.py`)
- Lee el archivo fuente `Datos_Dengue.csv`.  
- Valida la estructura, columnas y tipos de datos.  
- Registra métricas iniciales y errores durante la lectura.  

### 2. Transformación (`transform.py`)
- Estandariza nombres de columnas y tipos de datos.  
- Elimina duplicados y valores nulos.  
- Aplica validaciones de consistencia (departamento, municipio, año, etc.).  

### 3. Carga (`load.py`)
- Inserta los datos transformados en la base de datos `dbProject.db`.  
- Crea las tablas según los esquemas definidos en `schema.py`.  
- Controla duplicados y garantiza la integridad referencial.  

### 4. Monitoreo (`monitor.py`)
- Registra métricas de ejecución en la tabla `etl_monitor`, incluyendo:
  - Fecha y hora de ejecución  
  - Registros leídos, válidos y descartados  
  - Duración total  
  - Errores encontrados  

### 5. Logging (`logger.py`)
- Captura eventos del proceso en tiempo real.  
- Guarda los registros en `logs/etl.log`, asegurando trazabilidad y depuración sencilla.  

---

## 📊 Análisis Exploratorio (EDA)

El archivo **`EDA_Dengue.ipynb`** contiene el análisis exploratorio de los datos cargados en `dbProject.db`.  
Allí se incluye:
- Revisión general del dataset.  
- Estadísticas descriptivas de las variables.  
- Visualizaciones de distribución y cruces relevantes (hospitalización, estado final, año, etc.).  

---

## 🚀 Ejecución del Proyecto

### 🔧 Requisitos
- **Conda / Miniconda**
- **Python 3.10+**
- **SQLite 3**

### ▶️ Configuración del Entorno
1. Crear el entorno Conda:

   conda env create -f environment.yaml

2. Activar el entorno:

    conda activate etl_project

▶️ Ejecución del Flujo ETL

Ejecutar el script principal:
    python src/main.py

### 📂 Resultados
### La Base de datos y los archivos de Data se ecuentran en el Enlace:

https://drive.google.com/drive/folders/1yXFmbg-YAtsJxkfk29I1_0KQDHAcgtmZ?usp=sharing

+ Base de datos generada: data/dbProject.db
+ Logs del proceso: logs/etl.log
+ Métricas del ETL: tabla etl_monitor

### 🧩 Tecnologías Utilizadas

+ Python: pandas, sqlite3, logging, datetime
+ SQLite: motor de base de datos local
+ Conda: gestión de entorno y dependencias
+ Visual Studio Code / Jupyter Notebook
+ Git y GitHub: control de versiones

### 🏁 Conclusiones

+ El proyecto presenta una arquitectura modular y trazable, que facilita mantenimiento y escalabilidad.
+ El uso de Conda y el archivo environment.yaml garantiza un entorno reproducible.
+ Los módulos de logging y monitoreo aseguran control total sobre el proceso.
+ El análisis EDA complementa el flujo ETL, proporcionando una visión inicial de los patrones y comportamientos de los datos.
+ La solución puede adaptarse fácilmente a otras fuentes o motores de base de datos.
