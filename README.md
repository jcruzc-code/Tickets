# Dashboard Gerencial RRHH

Aplicacion Streamlit separada del scraper y del programador de tareas.

Fuente de datos:

- Base SQLite principal para publicar: `data/ticketera.sqlite`
- Vista principal: `vw_requests_rrhh_analisis`
- Vista de apoyo: `vw_requests_detalles_web`

## Que muestra

- volumen de tickets
- cobertura de posiciones
- tiempos desde ticket hasta procesamiento RRHH, reclutamiento y cobertura
- alertas de tickets vencidos, alta prioridad y sin avance
- analisis por Lima vs Provincias
- top motivos, estados y ubicaciones
- narrativa ejecutiva para presentacion gerencial

## Ejecutar local

Desde la carpeta `dashboard_rrhh_streamlit`:

```powershell
pip install -r .\requirements.txt
streamlit run .\app.py
```

Tambien puedes abrirlo con:

```powershell
.\dashboard_rrhh_streamlit\run_dashboard.bat
```

## Publicar en Streamlit Web con GitHub

1. Sube este repositorio a GitHub.
2. En Streamlit, crea una app nueva apuntando al repo.
3. Como archivo principal usa:

```text
app.py
```

4. Para la base de datos tienes tres opciones:

- Opcion recomendada para publicar ya: incluir `data/ticketera.sqlite` en el repo.
- Opcion recomendada para pruebas: subir `ticketera.sqlite` manualmente desde el sidebar del dashboard.
- Opcion simple para demos: dejar tambien una copia en `artifacts/ticketera.sqlite`.
- Opcion configurable: definir `ticketera_db_path` en Streamlit Secrets o `TICKETERA_DB_PATH` como variable de entorno.

Si usas Streamlit Web y no subes la base al repo, el dashboard igual abre y te pedira cargar el archivo SQLite.

## Estructura recomendada

Todo lo del dashboard queda dentro de:

```text
dashboard_rrhh_streamlit/
```

Incluye:

- `app.py`
- `requirements.txt`
- `run_dashboard.bat`
- `data/ticketera.sqlite` si quieres subir la base junto con la app

## Definiciones usadas

- `Fecha del ticket`: `Fecha de creación`
- `Fecha de procesamiento RRHH`: `Fecha de Inicio de Búsqueda`
- `Fecha de reclutamiento`: `Fecha de Contrato del Destacado`
- `Fecha de cobertura de la posición`: `Fecha de Inicio del Destacado`

## Recomendacion de uso

- usar `vw_requests_rrhh_analisis` para la presentacion
- usar `vw_requests_detalles_web` cuando necesites validar algun campo tal como se ve en la web
