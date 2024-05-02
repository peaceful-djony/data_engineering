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
PYTHONPATH='.' luigi --module main CleanupProjectTask --url "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file" --output-file "GSE68849_RAW.tar" 
```

## Tasks description:
1. DownloadTask - скачивает архив с отображением прогресса в консоли (под VPN файл скачивается ГОРАЗДО быстрей)
2. UntarTask - распаковывает tar файл на составляющие его архивы (gz файлы) и кладет все это в папку `./data/extracted_tar/*`
3. ExtractGzTask - распаковывает архивы по папкам внутри `./data/unzipped/`
4. ParseDatasetTask - производит разбор текстовых файлов и сохраняет каждый датасет в соответствующий `*.tsv` файл
5. CleanupProbesTask - создает копию файлов `./**/Probes.tsv` с названием `./**/Probes_fixed.tsv`, где отсутствуют лишние колонки
6. CleanupProjectTask - очищает проект, путем удаления разархивированных текстовых файлов