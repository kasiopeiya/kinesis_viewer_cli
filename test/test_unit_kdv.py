import glob
import os
import re
import test.util as util

import boto3
import pytest
from moto import mock_aws

import src.const as const
from src.kinesis_data_viewer import KinesisDataViewer

REGION = "ap-northeast-1"
STREAM_NAME = "kdv-unit-test-stream"
NUM_OF_TEST_RECORDS = 30


class TestKinesisDataViewer:
    def setup_class(cls) -> None:
        cls.stream_name = STREAM_NAME
        cls.region = REGION
        cls.client = boto3.client("kinesis", region_name=cls.region)

    def teardown_method(self):
        # distディレクトリ内のCSVファイルを削除
        for file in glob.glob(f"dist/kdv_output_{self.stream_name}_*.csv"):
            os.remove(file)

    def setup_kinesis(self) -> None:
        """Kinesis Data StreamsのDataStreamのMockリソースを作成する"""
        # ストリームを作成
        self.client.create_stream(
            StreamName=self.stream_name, StreamModeDetails={"StreamMode": "ON_DEMAND"}
        )

        # シャードIDを取得
        self.stream_arn = self.client.describe_stream(StreamName=self.stream_name)[
            "StreamDescription"
        ]["StreamARN"]
        shard_list = self.client.list_shards(StreamARN=self.stream_arn)["Shards"]
        self.shard_ids = [d[const.SHARD_ID] for d in shard_list]

    def setup_sample_records(self) -> None:
        """サンプルレコードを用意する"""
        # レコードを追加
        data = b"hello world"
        records = [
            {"Data": data, "PartitionKey": util.get_random_string()}
            for _ in range(NUM_OF_TEST_RECORDS)
        ]

        self.client.put_records(
            Records=records,
            StreamARN=self.stream_arn,
        )

    def test_main_no_streams(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
            kdv.main(region=self.region)

            captured = capsys.readouterr()
            assert "No data streams found" in captured.out
            assert exc_info.value.code == 0

    @mock_aws
    def test_main_exit(self, capsys, monkeypatch):
        self.setup_kinesis()

        def return_exit(self) -> str:
            """exitコマンドを返却するスタブ"""
            return "exit"

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        monkeypatch.setattr(kdv, "_select_command", return_exit.__get__(kdv, KinesisDataViewer))
        kdv.main()

        captured = capsys.readouterr()
        assert "exit kinesis viewer cli" in captured.out

    @mock_aws
    def test_main_invalid_command(self, monkeypatch):
        self.setup_kinesis()

        def return_invalid_command(self) -> str:
            """誤ったコマンド名を返却するスタブ"""
            return "Invalid Command Name"

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        monkeypatch.setattr(
            kdv, "_select_command", return_invalid_command.__get__(kdv, KinesisDataViewer)
        )

        with pytest.raises(ValueError):
            kdv.main()

    @mock_aws
    def test_summary(self, capsys):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv.summary()

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert "Data Stream Summary " in captured.out
        assert const.SHARD_ID in captured.out
        assert const.NUM_OF_RECORDS in captured.out
        assert const.LAST_ADDED_TIME in captured.out
        assert captured.out.count("shardId-") == 4

    @mock_aws
    @pytest.mark.no_records
    def test_summary_no_records(self, capsys):
        self.setup_kinesis()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv.summary()

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert "Data Stream Summary " in captured.out
        assert const.SHARD_ID in captured.out
        assert const.NUM_OF_RECORDS in captured.out
        assert const.LAST_ADDED_TIME in captured.out
        assert captured.out.count("shardId-") == 4

    @mock_aws
    def test_dump_records_terminal(self, capsys):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._dump_records(target_shard=self.shard_ids[0], output="terminal")

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert const.NUMBER in captured.out
        assert const.SEQ_NUM in captured.out
        assert const.PARTITION_KEY in captured.out
        assert const.DATA in captured.out
        assert const.TIMESTAMP in captured.out
        assert "hello world" in captured.out

    @mock_aws
    @pytest.mark.no_records
    def test_dump_records_terminal_no_records(self, capsys):
        self.setup_kinesis()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._dump_records(target_shard=self.shard_ids[0], output="terminal")

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert const.NUMBER in captured.out
        assert const.SEQ_NUM in captured.out
        assert const.PARTITION_KEY in captured.out
        assert const.DATA in captured.out
        assert const.TIMESTAMP in captured.out

    @mock_aws
    def test_dump_records_csv(self):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._dump_records(target_shard=self.shard_ids[0], output="csv")

        # csvファイル出力確認
        pattern = rf"dist/kdv_output_{self.stream_name}_{self.shard_ids[0]}_\d{{8}}_\d{{6}}\.csv"
        files = [f for f in glob.glob("dist/*.csv") if re.match(pattern, f)]

        # 出力ファイルが1つであることを確認
        assert len(files) == 1

    @mock_aws
    def test_show_recent_records(self, capsys):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._show_recent_records(target_shard=self.shard_ids[0])

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert const.NUMBER in captured.out
        assert const.SEQ_NUM in captured.out
        assert const.PARTITION_KEY in captured.out
        assert const.DATA in captured.out
        assert const.TIMESTAMP in captured.out
        assert captured.out.count("hello world") > 0
        assert captured.out.count("hello world") <= 100

    @mock_aws
    @pytest.mark.no_records
    def test_show_recent_records_no_records(self, capsys):
        self.setup_kinesis()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._show_recent_records(target_shard=self.shard_ids[0])

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert const.NUMBER in captured.out
        assert const.SEQ_NUM in captured.out
        assert const.PARTITION_KEY in captured.out
        assert const.DATA in captured.out
        assert const.TIMESTAMP in captured.out
        assert captured.out.count("hello world") == 0

    @mock_aws
    def test_search_record(self, capsys):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._search_record(key="hello world")

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert f"{NUM_OF_TEST_RECORDS} record found" in captured.out

    @mock_aws
    def test_search_record_key_blank(self, capsys):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._search_record(key="")

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert captured.out == ""

    @mock_aws
    def test_search_record_key_number(self, capsys):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._search_record(key=1)

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert captured.out != ""

    @mock_aws
    def test_search_record_not_found(self, capsys):
        self.setup_kinesis()
        self.setup_sample_records()

        kdv = KinesisDataViewer(region=self.region, target_stream_name=self.stream_name)
        kdv._search_record(key="hoge")

        # ターミナルへの出力内容の確認
        captured = capsys.readouterr()
        assert "Could not find record" in captured.out
