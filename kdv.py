import csv
import datetime
import os

import boto3
import questionary
import rich
import rich.progress
from rich.table import Table


class KinesisDataViewer:
    def __init__(self):
        self.kinesis_client = boto3.client("kinesis", region_name="ap-northeast-1")

        self.commands = ("summary", "dump_records", "search_record", "Exit")

        self.shard_ids = []
        self.all_records = {}

        self.data_stream_names = self._get_stream_names()
        if not self.data_stream_names:
            print("No data streams found")
            exit(1)
        self.target_stream_name = questionary.select(
            "Target Stream Name?",
            choices=self.data_stream_names,
        ).ask()
        print(self.target_stream_name)

    def main(self) -> None:
        command = questionary.select(
            "Command?",
            choices=self.commands,
        ).ask()
        if command == "Exit":
            return
        if command:
            method = getattr(self, command, None)
        if method:
            method()
        self.main()

    def summary(self):
        # レコード取得
        self.all_records = self.all_records or (self._get_records())

        #  出力
        table = Table(
            show_header=True, header_style="bold magenta", title="Data Stream Summary"
        )
        table.add_column("ShardId", style="dim", width=25)
        table.add_column("NumOfRecords")
        table.add_column("LastAddedTime")
        for shard_id, records_in_shard in self.all_records.items():
            if not records_in_shard:
                numOfRecords = "0"
                last_added_time = "-"
            else:
                numOfRecords = str(len(records_in_shard))
                maxSequenceNum = max(records_in_shard.keys())
                latest_record = records_in_shard[maxSequenceNum]
                last_added_time = latest_record["approximateArrivalTimestamp"]
            table.add_row(shard_id, numOfRecords, last_added_time)
        rich.print(table)

    def dump_records(self):
        # シャード情報取得
        self.shard_ids = self.shard_ids or (self._list_shards())
        # レコード取得
        self.all_records = self.all_records or (self._get_records())

        # 出力対象のシャード選択
        target_shard = questionary.select(
            "Target Shard?",
            choices=self.shard_ids,
        ).ask()
        records_in_shard = self.all_records[target_shard]

        # 出力先を選択
        output = questionary.select(
            "Output destination?",
            choices=["terminal", "csv"],
        ).ask()

        # ターミナル出力
        if output == "terminal":
            table = Table(
                show_header=True,
                header_style="bold magenta",
                title=f"List Records: {self.target_stream_name}-{target_shard}",
            )
            table.add_column("SequenceNumber", style="dim", width=70)
            table.add_column("PartitionKey")
            table.add_column("Data")
            table.add_column("ApproximateArrivalTimestamp")
            for sequenceNum, record in records_in_shard.items():
                table.add_row(
                    sequenceNum,
                    record["partitionKey"],
                    record["data"],
                    record["approximateArrivalTimestamp"],
                )
            rich.print(table)
            return

        # ファイル出力
        data = [
            dict(**record, **{"sequenceNumber": seqNum})
            for seqNum, record in records_in_shard.items()
        ]

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = (
            f"kdv_output_{self.target_stream_name}_{target_shard}_{timestamp}.csv"
        )
        output_path = os.path.join("dist", output_filename)
        os.makedirs("dist", exist_ok=True)

        with open(output_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        print(f"CSVファイル '{output_filename}' に出力しました。")

    def search_record(self) -> None:
        # レコード取得
        self.all_records = self.all_records or (self._get_records())

        # 検索文字列
        key = questionary.text("Key?").ask()
        if not key:
            return

        target_records = []
        for shard_id, records in self.all_records.items():
            for seqNum, data_item in records.items():
                if key in data_item["data"]:
                    target_records.append(
                        {
                            "shardId": shard_id,
                            "SequenceNumber": seqNum,
                            "Data": data_item["data"],
                            "PartitionKey": data_item["partitionKey"],
                            "ApproximateArrivalTimestamp": data_item[
                                "approximateArrivalTimestamp"
                            ],
                        }
                    )

        if not target_records:
            print("Could not find record")
            return

        for target_record in target_records:
            rich.print(target_record)
        print(f"{len(target_records)} record found")

    def _get_stream_names(self) -> list[str]:
        response = self.kinesis_client.list_streams(Limit=100)
        return response["StreamNames"]

    def _list_shards(self):
        response = self.kinesis_client.list_shards(StreamName=self.target_stream_name)
        shard_ids = [shard["ShardId"] for shard in response["Shards"]]
        self.shard_ids = shard_ids
        return self.shard_ids

    def _get_records(self):
        self.shard_ids = self.shard_ids or (self._list_shards())
        shard_map = {}

        with rich.progress.Progress() as progress:
            task = progress.add_task(
                "[green]Getting Records...", total=len(self.shard_ids)
            )
            for shard_id in self.shard_ids:
                response = self.kinesis_client.get_shard_iterator(
                    StreamName=self.target_stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="TRIM_HORIZON",
                )
                shard_iterator = response["ShardIterator"]
                records_in_shard = {}

                while True:
                    # レコードを取得
                    response = self.kinesis_client.get_records(
                        ShardIterator=shard_iterator, Limit=1000
                    )

                    if not response["Records"]:
                        shard_map[shard_id] = records_in_shard
                        progress.update(task, advance=1)
                        break

                    for record in response["Records"]:
                        records_in_shard[record["SequenceNumber"]] = {
                            "data": record["Data"].decode("utf-8"),
                            "partitionKey": record["PartitionKey"],
                            "approximateArrivalTimestamp": record[
                                "ApproximateArrivalTimestamp"
                            ].strftime("%Y-%m-%d %H:%M:%S %Z"),
                        }

                    # 次のイテレーターを取得
                    shard_iterator = response["NextShardIterator"]
        return shard_map


if __name__ == "__main__":
    import jsonargparse

    jsonargparse.CLI(KinesisDataViewer)
