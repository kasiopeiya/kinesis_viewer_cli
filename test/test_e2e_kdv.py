import glob
import os
import subprocess
import test.util as util

import boto3

import src.const as const

REGION = "ap-northeast-1"
STREAM_NAME = "kdv-e2e-test-stream"
NUM_OF_TEST_RECORDS = 30


class TestKinesisDataViewer:
    def setup_class(cls) -> None:
        """テスト準備、実際にKDS DataStreamを構築する"""
        cls.stream_name = STREAM_NAME
        cls.region = REGION
        cls.client = boto3.client("kinesis", region_name=cls.region)

        # ストリームを作成
        cls.client.create_stream(
            StreamName=cls.stream_name, StreamModeDetails={"StreamMode": "ON_DEMAND"}
        )

        # ストリームがActive状態になるまで待機
        while True:
            status = cls.client.describe_stream(StreamName=cls.stream_name)["StreamDescription"][
                "StreamStatus"
            ]
            if status == "ACTIVE":
                break

        # シャードIDを取得
        cls.stream_arn = cls.client.describe_stream(StreamName=cls.stream_name)[
            "StreamDescription"
        ]["StreamARN"]
        shard_list = cls.client.list_shards(StreamARN=cls.stream_arn)["Shards"]
        cls.shard_ids = [d[const.SHARD_ID] for d in shard_list]

        # レコードを追加
        data = b"hello world"
        records = [
            {"Data": data, "PartitionKey": util.get_random_string()}
            for _ in range(NUM_OF_TEST_RECORDS)
        ]
        cls.client.put_records(
            Records=records,
            StreamARN=cls.stream_arn,
        )

    def teardown_class(cls):
        """テストの後片付け"""
        # distディレクトリ内のCSVファイルを削除
        for file in glob.glob(f"dist/kdv_output_{cls.stream_name}_*.csv"):
            os.remove(file)

        # データストリーム削除
        cls.client.delete_stream(StreamName=cls.stream_name)

    def test_summary(self):
        """正常: summaryコマンド実行"""
        # ツール実行
        command = "summary"
        process = subprocess.Popen(
            [
                "python",
                "-m",
                "kdv",
                "main",
                "--region",
                self.region,
                "--target_stream_name",
                self.stream_name,
                "--command",
                command,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # 結果を受け取る
        output, _ = process.communicate()

        assert "Data Stream Summary" in output
        assert const.SHARD_ID in output
        assert const.NUM_OF_RECORDS in output
        assert const.LAST_ADDED_TIME in output
        assert output.count("shardId-") == 4
