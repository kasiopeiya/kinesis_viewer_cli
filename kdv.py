import csv
import datetime
import os
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

import boto3
import questionary
import rich
import rich.progress
from rich.table import Table

import const


class KinesisDataViewer:
    def __init__(self, region: str = "", target_stream_name: str = "") -> None:
        # リージョンの選択
        ec2_client = boto3.client("ec2")
        regions = ec2_client.describe_regions()
        region_names = [region["RegionName"] for region in regions["Regions"]]
        region_name = (
            region
            or questionary.select(
                "Target Region?", choices=region_names, default="ap-northeast-1"
            ).ask()
        )
        self.kinesis_client = boto3.client("kinesis", region_name=region_name)

        # 操作対象のDataStreamの選択
        data_stream_names = self._get_stream_names()
        if not data_stream_names:
            print("No data streams found")
            exit(1)
        self.target_stream_name = (
            target_stream_name
            or questionary.select(
                "Target Stream Name?",
                choices=data_stream_names,
            ).ask()
        )

        self.shard_ids: tuple = ()
        self.all_records: dict = {}

    def main(self) -> None:
        # 操作コマンドの選択
        rich.print("select 'exit' for data refresh")
        commands = (
            "summary",
            "dump_records",
            "show_recent_records",
            "search_record",
            "exit",
        )
        command = questionary.select("Command?", choices=commands).ask()
        if not command or command == "exit":
            return
        if method := getattr(self, command, None):
            method()
        self.main()

    def summary(self):
        """シャード一覧とシャードごとの格納レコード数などの情報を出力する"""
        # レコード取得
        self.all_records = self.all_records or (self._get_records())

        #  出力
        table = Table(
            show_header=True, header_style="bold magenta", title="Data Stream Summary"
        )
        table.add_column(const.SHARD_ID, style="bold", width=25)
        table.add_column(const.NUM_OF_RECORDS)
        table.add_column(const.LAST_ADDED_TIME)
        for shard_id, records_in_shard in self.all_records.items():
            if not records_in_shard:
                # レコードが１件もない場合
                numOfRecords = "0"
                last_added_time = "-"
            else:
                numOfRecords = str(len(records_in_shard))
                maxSequenceNum = max(records_in_shard.keys())
                latest_record = records_in_shard[maxSequenceNum]
                last_added_time = latest_record[const.TIMESTAMP]
            table.add_row(shard_id, numOfRecords, last_added_time)
        rich.print(table)

    def dump_records(self) -> None:
        """選択したシャードのレコード一覧を出力する"""
        self._dump_records(
            target_shard=self._select_shard(), output=self._select_output()
        )

    def _dump_records(self, target_shard: str, output: str) -> None:
        """選択したシャードのレコード一覧を出力する"""
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self._list_shards())
        # レコード取得
        self.all_records = self.all_records or (self._get_records())

        records_in_shard: list[dict] = self._dict_to_list(
            self.all_records[target_shard]
        )

        # 結果を出力
        if output == "terminal":
            self._output_terminal(target_shard, records_in_shard)
        elif output == "csv":
            self._output_csv(target_shard, records_in_shard)

    def show_recent_records(self) -> None:
        """選択したシャードの最近100レコードを出力する"""
        self._show_recent_records(target_shard=self._select_shard())

    def _show_recent_records(self, target_shard: str) -> None:
        """選択したシャードの最近100レコードを出力する"""
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self._list_shards())
        # レコード取得
        self.all_records = self.all_records or (self._get_records())

        records_in_shard: list[dict] = self._dict_to_list(
            self.all_records[target_shard]
        )
        sorted_records = sorted(
            records_in_shard, key=lambda d: d[const.SEQ_NUM], reverse=True
        )
        recent_records = [record for i, record in enumerate(sorted_records) if i <= 100]

        # 結果を出力
        self._output_terminal(target_shard, recent_records)

    def search_record(self) -> None:
        """指定されたキーワードでレコードのData部を検索し、結果をターミナルに表示する"""
        self._search_record(key=self._enter_key())

    def _search_record(self, key: str) -> None:
        """指定されたキーワードでレコードのData部を検索し、結果をターミナルに表示する"""
        if not key:
            return

        # レコード取得
        self.all_records = self.all_records or (self._get_records())

        # 検索文字列を含むレコードを検索
        if not (target_records := self._find_records_by_key(str(key))):
            print("Could not find record")
            return

        for target_record in target_records:
            rich.print(target_record)
        print(f"{len(target_records)} record found")

    def _find_records_by_key(self, key: str) -> list[dict[str, str]]:
        """指定されたキーワードのレコードを検索して返す"""
        target_records = []
        for shard_id, records in self.all_records.items():
            for seqNum, data_item in records.items():
                if key in data_item[const.DATA]:
                    target_records.append(
                        {
                            const.SHARD_ID: shard_id,
                            const.SEQ_NUM: seqNum,
                            const.DATA: data_item[const.DATA],
                            const.PARTITION_KEY: data_item[const.PARTITION_KEY],
                            const.TIMESTAMP: data_item[const.TIMESTAMP],
                        }
                    )
        return target_records

    def _get_stream_names(self) -> tuple[str]:
        """対象アカウント、リージョンに存在するKinesis Data Streams DataStreamを全て取得する"""
        response = self.kinesis_client.list_streams(Limit=100)
        return tuple(response["StreamNames"])

    def _list_shards(self) -> tuple[str]:
        """処理対象DataStreamのシャードID一覧を取得する"""
        response = self.kinesis_client.list_shards(StreamName=self.target_stream_name)
        shard_ids = [shard[const.SHARD_ID] for shard in response["Shards"]]
        return tuple(shard_ids)

    def _get_records(self) -> dict[str, dict[int, dict[str, str]]]:
        """処理対象DataStreamに格納されている全てのレコードを取得する

        Return Example:
          {
            'shardId-000000000000': {
              '49657051368801430459340561020608066337892395447780114434': {
                'Data': '{"recordId":"RCS3ffmbiL","requestId":"1-3-diHOsUpMZsWnB5Bp",'
                'PartitionKey': 'RCS3ffmbiL'
                'ApproximateArrivalTimestamp': '2024-10-24 14:23:43 JST'
              }
              '49657051368801430459340561020609275263712010076954820610': {
                'Data': '{"recordId":"4l7RHBOOWj","requestId":"1-3-diHOsUpMZsWnB5Bp",'
                'PartitionKey': '4l7RHBOOWj'
                'ApproximateArrivalTimestamp': '2024-10-24 14:23:43 JST'
          │   },
            'shardId-000000000001': {
            ...
        """
        self.shard_ids = self.shard_ids or (self._list_shards())
        shard_map = {}

        # シャードからレコードの読み取り処理、マルチスレッドで実行
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = executor.map(self._read_shard_records, self.shard_ids)
        shard_map = dict(chain.from_iterable(d.items() for d in list(results)))
        return shard_map

    def _read_shard_records(
        self, shard_id: str
    ) -> dict[str, dict[int, dict[str, str]]]:
        """シャード内の全てのレコードを取得する

        Return Example:
          {
            'shardId-000000000000': {
              '49657051368801430459340561020608066337892395447780114434': {
                'Data': '{"recordId":"RCS3ffmbiL","requestId":"1-3-diHOsUpMZsWnB5Bp",'
                'PartitionKey': 'RCS3ffmbiL'
                'ApproximateArrivalTimestamp': '2024-10-24 14:23:43 JST'
              }
            }
          }
        """
        response = self.kinesis_client.get_shard_iterator(
            StreamName=self.target_stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )

        shard_iterator = response["ShardIterator"]
        shard_map = {}
        records_in_shard = {}

        while True:
            # レコードを取得
            response = self.kinesis_client.get_records(
                ShardIterator=shard_iterator, Limit=1000
            )
            if not response["Records"]:
                break

            for record in response["Records"]:
                records_in_shard[record[const.SEQ_NUM]] = {
                    const.DATA: record[const.DATA].decode("utf-8"),
                    const.PARTITION_KEY: record["PartitionKey"],
                    const.TIMESTAMP: record[const.TIMESTAMP].strftime(
                        "%Y-%m-%d %H:%M:%S %Z"
                    ),
                }

            # 次のイテレーターを取得
            shard_iterator = response["NextShardIterator"]
        shard_map[shard_id] = records_in_shard
        return shard_map

    def _output_terminal(
        self, shard_name: str, records_in_shard: list[dict[str, str]]
    ) -> None:
        """レコードリストをターミナルに出力"""
        table = Table(
            show_header=True,
            header_style="bold magenta",
            title=f"List Records: {self.target_stream_name}: {shard_name}",
        )
        table.add_column(const.NUMBER, justify="center")
        table.add_column(const.SEQ_NUM, style="bold", width=60)
        table.add_column(const.PARTITION_KEY)
        table.add_column(const.DATA)
        table.add_column(const.TIMESTAMP)
        for index, record in enumerate(records_in_shard):
            table.add_row(
                str(index),
                record[const.SEQ_NUM],
                record[const.PARTITION_KEY],
                record[const.DATA],
                record[const.TIMESTAMP],
            )
        rich.print(table)

    def _output_csv(
        self, shard_name: str, records_in_shard: list[dict[str, str]]
    ) -> None:
        """レコードリストをcsvファイルに出力"""

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = (
            f"kdv_output_{self.target_stream_name}_{shard_name}_{timestamp}.csv"
        )
        output_path = os.path.join("dist", output_filename)
        os.makedirs("dist", exist_ok=True)
        with open(output_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=records_in_shard[0].keys())
            writer.writeheader()
            writer.writerows(records_in_shard)

        rich.print(f"Output written to CSV file '{output_filename}'.")

    def _dict_to_list(self, records_in_shard: dict[int, dict]) -> list[dict]:
        """dictionaryのkeyとvalueを分解し、dictionaryのlistとして再構成する"""
        return [
            dict(**{const.SEQ_NUM: seqNum}, **record)
            for seqNum, record in records_in_shard.items()
        ]

    def _select_shard(self) -> str:
        """ターミナルで対象のシャードを選択する"""
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self._list_shards())

        return questionary.select("Target Shard?", choices=self.shard_ids).ask()

    def _select_output(self) -> str:
        """ターミナルで結果の出力方法を選択する"""
        return questionary.select(
            "Output destination?", choices=["terminal", "csv"]
        ).ask()

    def _enter_key(self) -> str:
        """ターミナルでレコード検索に使うkeyを入力する"""
        return questionary.text("Key?").ask()


if __name__ == "__main__":
    import jsonargparse

    jsonargparse.CLI(KinesisDataViewer)
