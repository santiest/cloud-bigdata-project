
import os
from dotenv import load_dotenv


class EnvVariables:
    def __init__(self):
        # Get environment variables from .env file
        load_dotenv("../.env")
        self._fileName = os.getenv('FILENAME')
        self._outputDir = os.getenv('OUTPUT_DIR')
        if (self._fileName == None):
            load_dotenv(".env")
            self._fileName = os.getenv('FILENAME')
            self._outputDir = os.getenv('OUTPUT_DIR')

    def getFileName(self):
        return self._fileName

    def getOutputDir(self):
        return self._outputDir
