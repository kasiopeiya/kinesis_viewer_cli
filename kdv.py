import sys

from src.kinesis_data_viewer import KinesisDataViewerCLI

if __name__ == "__main__":
    import jsonargparse

    if len(sys.argv) == 1:
        sys.argv.append("main")
    jsonargparse.CLI(KinesisDataViewerCLI)
