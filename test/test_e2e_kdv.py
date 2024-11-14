import glob
import os
import subprocess
import sys
import test.util as util

import boto3

import src.const as const
import src.msg as msg

REGION = os.getenv("KDV_REGION") or "ap-northeast-1"
PYTHON_VERSION = f"{sys.version_info.major}-{sys.version_info.minor}-{sys.version_info.micro}"
STREAM_NAME = f'{os.getenv("STREAM_NAME") or "kdv-e2e-test-stream"}-{PYTHON_VERSION}'
NUM_OF_TEST_RECORDS = int(os.getenv("NUM_OF_TEST_RECORDS") or 30)


class TestKinesisDataViewer:
    stream_name: str | None = None
    region: str | None = None
    client = None
    stream_arn: str | None = None
    shard_ids: list[str] | None = None

    @classmethod
    @util.error_handling
    def setup_class(cls) -> None:
        """テスト準備、実際にKDS DataStreamを構築する"""
        cls.stream_name = STREAM_NAME
        cls.region = REGION
        cls.client = boto3.client("kinesis", region_name=cls.region)
        # 出力幅が狭いと、文字が省略されて適切に出力されず、assert失敗するため必須
        os.environ["COLUMNS"] = "500"

        # ストリームを作成
        cls.client.create_stream(
            StreamName=cls.stream_name, StreamModeDetails={"StreamMode": "ON_DEMAND"}
        )
        cls._wait_for_stream_active(cls.client, cls.stream_name)

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

    @classmethod
    @util.error_handling
    def teardown_class(cls):
        """テストの後片付け"""
        # distディレクトリ内のCSVファイルを削除
        for file in glob.glob(f"dist/kdv_output_{cls.stream_name}_*.csv"):
            os.remove(file)

        # データストリーム削除
        cls.client.delete_stream(StreamName=cls.stream_name)

    @classmethod
    def _wait_for_stream_active(cls, client, stream_name) -> None:
        """ストリームがActive状態になるまで待機"""
        while (
            client.describe_stream(StreamName=stream_name)["StreamDescription"]["StreamStatus"]
            != "ACTIVE"
        ):
            pass

    def _run_command(self, command, extra_options=None) -> str:
        base_command = [
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
        ]
        if extra_options:
            base_command.extend(extra_options)
        process = subprocess.Popen(
            base_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # 結果を受け取る
        output, _ = process.communicate()
        return output

    def test_summary(self):
        """正常: summaryコマンド実行"""
        # ツール実行
        output = self._run_command(command="summary")

        assert msg.SUMMARY_TITLE in output
        assert const.SHARD_ID in output
        assert const.NUM_OF_RECORDS in output
        assert const.LAST_ADDED_TIME in output
        assert output.count("shardId-") == 4

    def test_dump_records_terminal(self):
        """正常: dump_recordsコマンド実行(ターミナル出力)"""
        # ツール実行
        extra_options = ["--target_shard", self.shard_ids[0], "--dump_output", "terminal"]
        output = self._run_command(command="dump_records", extra_options=extra_options)

        assert const.NUMBER in output
        assert const.SEQ_NUM in output
        assert const.PARTITION_KEY in output
        assert const.DATA in output
        assert const.TIMESTAMP in output
        assert "hello world" in output

    def test_dump_records_csv(self):
        """正常: dump_recordsコマンド実行(csv出力)"""
        # ツール実行
        extra_options = ["--target_shard", self.shard_ids[0], "--dump_output", "csv"]
        self._run_command(command="dump_records", extra_options=extra_options)

        # 出力ファイルが1つであることを確認
        files = [file for file in glob.glob(f"dist/kdv_output_{self.stream_name}_*.csv")]
        assert len(files) == 1

    def test_show_recent_records(self):
        """正常: show_recent_recordsコマンド実行"""
        # ツール実行
        extra_options = ["--target_shard", self.shard_ids[0]]
        output = self._run_command(command="show_recent_records", extra_options=extra_options)

        assert const.NUMBER in output
        assert const.SEQ_NUM in output
        assert const.PARTITION_KEY in output
        assert const.DATA in output
        assert const.TIMESTAMP in output
        assert output.count("hello world") > 0
        assert output.count("hello world") <= 100

    def test_search_record(self):
        """正常: search_keyコマンド実行"""
        # ツール実行
        extra_options = ["--search_key", "hello world"]
        output = self._run_command(command="search_record", extra_options=extra_options)

        assert f"{NUM_OF_TEST_RECORDS} record found" in output
