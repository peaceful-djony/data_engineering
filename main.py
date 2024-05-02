import glob
import gzip
import io
import os
import shutil
import tarfile
import luigi
import pandas as pd
import time
import wget

from config import (
    DATA_DIR,
    TAR_DIR,
    GZ_DIR,
)


class DownloadTask(luigi.Task):
    """
    Шаг 1. Загрузка исходного файла с архивом
    """
    url = luigi.Parameter()
    output_file = luigi.Parameter()
    _full_path = None
    _started = None

    def output(self):
        self._full_path = os.path.join(DATA_DIR, self.output_file)
        return luigi.LocalTarget(self._full_path)

    def run(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        DownloadTask._started = time.perf_counter()
        wget.download(self.url, self._full_path, bar=self._bar_info)
        print()  # to split bar information

    @staticmethod
    def _bar_info(current, total_size, width=80):
        """
        Отображение прогресса в консоли
        :param current:
        :param total_size:
        :param width:
        :return:
        """
        percent = min(current / total_size, 1.0)
        progress = "=" * int(64 * percent)
        print(
            f"\r[{progress}] {int(100 * percent)}%, took {time.perf_counter() - DownloadTask._started:.2f} sec",
            end="",
        )


class UntarTask(luigi.Task):
    """
    Шаг 2.1. Распаковывает tar файл в config.TAR_DIR
    """
    url = luigi.Parameter()
    output_file = luigi.Parameter()

    def output(self):
        self._full_path = os.path.join(DATA_DIR, TAR_DIR)
        return luigi.LocalTarget(self._full_path)

    def requires(self):
        return DownloadTask(url=self.url, output_file=self.output_file)

    def run(self):
        tar_file = self.input()
        with tarfile.open(tar_file.path, mode="r") as tar:
            tar.extractall(self._full_path)


class ExtractGzTask(luigi.Task):
    """
    Шаг 2.2. Распаковывает gz файлы в папки внутри config.GZ_DIR
    """
    url = luigi.Parameter()
    output_file = luigi.Parameter()
    _extension = ".gz"

    def output(self):
        txt_files = glob.glob(DATA_DIR + "/**/*.txt", recursive=True)
        res = [luigi.LocalTarget(path) for path in txt_files]
        return res

    def requires(self):
        return UntarTask(url=self.url, output_file=self.output_file)

    def run(self):
        gz_files = self._get_gz_files(self.input().path)
        full_path = os.path.join(DATA_DIR, GZ_DIR)
        os.makedirs(full_path, exist_ok=True)
        for item in gz_files:  # loop through items in dir
            if item.endswith(self._extension):  # check for ".gz" extension
                gz_full_name = os.path.abspath(item)  # get full path of files
                dir_name, ext, _ = (os.path.basename(gz_full_name)).rsplit('.', 2)
                full_dir_name = os.path.join(DATA_DIR, GZ_DIR, dir_name)
                file_name = os.path.join(full_dir_name, dir_name + "." + ext)
                os.makedirs(full_dir_name, exist_ok=True)
                with gzip.open(gz_full_name, "rb") as f_in, open(file_name, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

    def _get_gz_files(self, dir_name: str) -> list[str]:
        """
        Возвращает список путей к gz файлам в папке
        :param dir_name:
        :return:
        """
        res = [str(os.path.join(DATA_DIR, TAR_DIR, f)) for f in os.listdir(dir_name) if f.endswith(self._extension)]
        return res


class ParseDatasetTask(luigi.Task):
    """
    Шаг 2.3. Задача по разбору разархивированных текстовых файлов и созданию датасетов в формате tsv
    """
    url = luigi.Parameter()
    output_file = luigi.Parameter()

    def output(self):
        file_paths = glob.glob('**/*.tsv', recursive=True)
        return [luigi.LocalTarget(path) for path in file_paths]

    def requires(self):
        return ExtractGzTask(url=self.url, output_file=self.output_file)

    def run(self):
        extracted_files = self.input()
        dfs = {}
        for file_name in extracted_files:
            file_name = file_name.path
            with open(file_name, 'r') as f:
                write_key = None
                fio = io.StringIO()
                for line in f.readlines():
                    if line.startswith('['):
                        if write_key:
                            fio.seek(0)
                            header = None if write_key == 'Heading' else 'infer'
                            dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                        fio = io.StringIO()
                        write_key = line.strip('[]\n')
                        continue
                    if write_key:
                        fio.write(line)
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')
            gz_dir = os.path.dirname(file_name)
            for k, v in dfs.items():
                file_name = os.path.join(gz_dir, k + '.tsv')
                v.to_csv(file_name, sep='\t')


class CleanupProbesTask(luigi.Task):
    """
    Шаг 3. Удаляет ненужные колонки из Probes
    """
    url = luigi.Parameter()
    output_file = luigi.Parameter()
    file_name_to_fix = 'Probes.tsv'

    def requires(self):
        return ParseDatasetTask(url=self.url, output_file=self.output_file)

    def output(self):
        file_paths = glob.glob(f"**/{self.file_name_to_fix}", recursive=True)
        return [luigi.LocalTarget(path) for path in file_paths]

    def run(self):
        file_name_fixed = 'Probes_fixed.tsv'
        columns_to_remove = [
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Ontology_Function",
            "Synonyms",
            "Obsolete_Probe_Id",
            "Probe_Sequence"
        ]
        tsv_files = self.input()
        pobes_files = [tsv_file.path for tsv_file in tsv_files if tsv_file.path.endswith(self.file_name_to_fix)]
        for probes_file_path in pobes_files:
            df = pd.read_csv(probes_file_path, sep='\t')
            df = df.drop(columns_to_remove, axis=1)
            new_path = probes_file_path.replace(self.file_name_to_fix, file_name_fixed)
            df.to_csv(new_path, sep='\t')


class CleanupProjectTask(luigi.Task):
    """
    Шаг 4. Очищает проект, путем удаления разархивированных текстовых файлов, появившихся на этапе ExtractGzTask
    """
    url = luigi.Parameter()
    output_file = luigi.Parameter()

    def requires(self):
        return {
            "extracted_gz": ExtractGzTask(url=self.url, output_file=self.output_file),
            "cleanup_probes": CleanupProbesTask(url=self.url, output_file=self.output_file),
        }

    def run(self):
        extracted_gz = self.input()["extracted_gz"]
        for item in extracted_gz:
            os.remove(item.path)


if __name__ == "__main__":
    luigi.run()
