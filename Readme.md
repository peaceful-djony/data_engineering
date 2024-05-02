# HW 1

## venv activation
```bash
source /root/.virtualenvs/DataEngineering/bin/activate
```
Путь до активации переменной окружения может отличаться

## Install dependencies
```bash
pip install -r requirements.txt
```

## Run server with web UI
```bash
PYTHONPATH='.' luigid 
```
Интерфейс будет доступен [по ссылке](http://localhost:8082/)

## Run luigi script
```bash
PYTHONPATH='.' luigi --module hw1/main CleanupProjectTask --url "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file" --output-file "GSE68849_RAW.tar" 
```