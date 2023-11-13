import glob

class CSVDataFile:
    def __init__(self, directory, csv_pattern):
        csv_path = f"{directory}/{csv_pattern}" if directory else csv_pattern
        csv_files = glob.glob(csv_path)
        self.file_path = csv_files[0] if csv_files else None