import csv
import datetime
import os
import sys

import questionary
import rich
from rich.table import Table

import src.const as const
import src.msg as msg
from src.kinesis_client import KinesisClient


class KinesisDataViewer:
    def __init__(self, region: str = "", target_stream_name: str = "") -> None:
        self.region = region
        self.target_stream_name = target_stream_name
        if region:
            self.kds_client = KinesisClient(region, target_stream_name)
        self.shard_ids: tuple = ()
        self.all_records: dict = {}
        # 選択可能なコマンドリスト
        self.commands = (
            "summary",
            "dump_records",
            "show_recent_records",
            "search_record",
            "exit",
        )

    def main(
        self,
        region: str = "",
        target_stream_name: str = "",
        command: str = "",
        target_shard: str = "",
        dump_output: str = "",
        search_key: str = "",
    ) -> None:
        self.target_shard = target_shard
        self.dump_output = dump_output
        self.search_key = search_key

        # リージョンの選択
        region_names = KinesisClient.get_regions()
        region_name = (
            self.region
            or region
            or questionary.select(
                "Target Region?", choices=region_names, default="ap-northeast-1"
            ).ask()
        )

        # 操作対象のDataStreamの選択
        data_stream_names = KinesisClient.get_stream_names(region_name)
        if not data_stream_names:
            print(msg.NO_STREAM)
            sys.exit(0)
        self.target_stream_name = (
            self.target_stream_name
            or target_stream_name
            or questionary.select(
                "Target Stream Name?",
                choices=data_stream_names,
            ).ask()
        )
        self.kds_client = KinesisClient(region_name, self.target_stream_name)

        # 操作コマンドの選択
        command = command or self._select_command()
        if not command or command == "exit":
            print(msg.EXIT)
            return
        if (method := getattr(self, command, None)) is None:
            raise ValueError(msg.INVALID_COMMAND)
        method()
        self.main(region=region_name, target_stream_name=self.target_stream_name)

    def summary(self):
        """シャード一覧とシャードごとの格納レコード数などの情報を出力する"""
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self.kds_client.list_shards())
        # レコード取得
        self.all_records = self.all_records or (self.kds_client.get_records(self.shard_ids))

        #  出力
        table = Table(show_header=True, header_style="bold magenta", title=msg.SUMMARY_TITLE)
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
        target_shard = self.target_shard or self._select_shard()
        output = self.dump_output or self._select_output()
        self._dump_records(target_shard, output)

    def _dump_records(self, target_shard: str, output: str) -> None:
        """選択したシャードのレコード一覧を出力する"""
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self.kds_client.list_shards())
        # レコード取得
        self.all_records = self.all_records or (self.kds_client.get_records(self.shard_ids))

        records_in_shard: list[dict] = self._dict_to_list(self.all_records[target_shard])

        # 結果を出力
        if output == "terminal":
            self._output_terminal(target_shard, records_in_shard)
        elif output == "csv":
            self._output_csv(target_shard, records_in_shard)

    def show_recent_records(self) -> None:
        """選択したシャードの最近100レコードを出力する"""
        target_shard = self.target_shard or self._select_shard()
        self._show_recent_records(target_shard)

    def _show_recent_records(self, target_shard: str) -> None:
        """選択したシャードの最近100レコードを出力する"""
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self.kds_client.list_shards())
        # レコード取得
        self.all_records = self.all_records or (self.kds_client.get_records(self.shard_ids))

        records_in_shard: list[dict] = self._dict_to_list(self.all_records[target_shard])
        sorted_records = sorted(records_in_shard, key=lambda d: d[const.SEQ_NUM], reverse=True)
        recent_records = [record for i, record in enumerate(sorted_records) if i <= 100]

        # 結果を出力
        self._output_terminal(target_shard, recent_records)

    def search_record(self) -> None:
        """指定されたキーワードでレコードのData部を検索し、結果をターミナルに表示する"""
        key = self.search_key or self._enter_key()
        self._search_record(key)

    def _search_record(self, key: str) -> None:
        """指定されたキーワードでレコードのData部を検索し、結果をターミナルに表示する"""
        if not key:
            return

        # シャード情報取得
        self.shard_ids = self.shard_ids or (self.kds_client.list_shards())
        # レコード取得
        self.all_records = self.all_records or (self.kds_client.get_records(self.shard_ids))

        # 検索文字列を含むレコードを検索
        if not (target_records := self._find_records_by_key(str(key))):
            print(msg.NO_RECORD)
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

    def _output_terminal(self, shard_name: str, records_in_shard: list[dict[str, str]]) -> None:
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

    def _output_csv(self, shard_name: str, records_in_shard: list[dict[str, str]]) -> None:
        """レコードリストをcsvファイルに出力"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"kdv_output_{self.target_stream_name}_{shard_name}_{timestamp}.csv"
        output_path = os.path.join("dist", output_filename)
        os.makedirs("dist", exist_ok=True)
        with open(output_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=records_in_shard[0].keys())
            writer.writeheader()
            writer.writerows(records_in_shard)

        rich.print(f"{msg.OUTPUT_CSV} '{output_filename}'.")

    def _dict_to_list(self, records_in_shard: dict[int, dict]) -> list[dict]:
        """dictionaryのkeyとvalueを分解し、dictionaryのlistとして再構成する"""
        return [
            dict(**{const.SEQ_NUM: seqNum}, **record) for seqNum, record in records_in_shard.items()
        ]

    def _select_command(self) -> str:
        """ターミナルで結果の出力方法を選択する"""
        rich.print(msg.SELECT_EXIT)
        return questionary.select("Command?", choices=self.commands).ask()

    def _select_shard(self) -> str:
        """ターミナルで対象のシャードを選択する"""
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self.kds_client.list_shards())

        return questionary.select("Target Shard?", choices=self.shard_ids).ask()

    def _select_output(self) -> str:
        """ターミナルで結果の出力方法を選択する"""
        return questionary.select("Output destination?", choices=["terminal", "csv"]).ask()

    def _enter_key(self) -> str:
        """ターミナルでレコード検索に使うkeyを入力する"""
        return questionary.text("Key?").ask()
