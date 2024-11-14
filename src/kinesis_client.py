from concurrent.futures import ThreadPoolExecutor
from itertools import chain

import boto3

import src.const as const


class KinesisClient:
    """AWSとの通信を行う処理をまとめたクラス"""

    def __init__(self, region: str, stream_name: str) -> None:
        self.region = region
        self.target_stream_name = stream_name
        self.kinesis_client = boto3.client("kinesis", region_name=region)
        self.shard_ids: tuple = ()
        self.all_records: dict = {}

    @classmethod
    def get_regions(cls) -> list[str]:
        ec2_client = boto3.client("ec2")
        regions = ec2_client.describe_regions()
        return [region["RegionName"] for region in regions["Regions"]]

    @classmethod
    def get_stream_names(cls, region: str) -> tuple[str]:
        """対象アカウント、リージョンに存在するKinesis Data Streams DataStreamを全て取得する"""
        response = boto3.client("kinesis", region_name=region).list_streams(Limit=100)
        return tuple(response["StreamNames"])

    def list_shards(self) -> tuple[str]:
        """処理対象DataStreamのシャードID一覧を取得する"""
        response = self.kinesis_client.list_shards(StreamName=self.target_stream_name)
        shard_ids = [shard[const.SHARD_ID] for shard in response["Shards"]]
        return tuple(shard_ids)

    def get_records(self, shard_ids: tuple[str]) -> dict[str, dict[int, dict[str, str]]]:
        """処理対象DataStreamに格納されている全てのレコードを取得する

        Return Example:
          {
            'shardId-000000000000': {
              '49657051368801430459340561020608066337892395447780114434': {
                'Data': '{"recordId":"RCS3ffmbiL","requestId":"1-3-diHOsUpMZsWnB5Bp",'
                'PartitionKey': 'RCS3ffmbiL'
                'ApproximateArrivalTimestamp': '2024-10-24 14:23:43.000'
              }
              '49657051368801430459340561020609275263712010076954820610': {
                'Data': '{"recordId":"4l7RHBOOWj","requestId":"1-3-diHOsUpMZsWnB5Bp",'
                'PartitionKey': '4l7RHBOOWj'
                'ApproximateArrivalTimestamp': '2024-10-24 14:23:43.001'
          │   },
            'shardId-000000000001': {
            ...
        """
        shard_map = {}

        # シャードからレコードの読み取り処理、マルチスレッドで実行
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = executor.map(self.read_shard_records, shard_ids)
        shard_map = dict(chain.from_iterable(d.items() for d in list(results)))
        return shard_map

    def read_shard_records(self, shard_id: str) -> dict[str, dict[int, dict[str, str]]]:
        """シャード内の全てのレコードを取得する

        Return Example:
          {
            'shardId-000000000000': {
              '49657051368801430459340561020608066337892395447780114434': {
                'Data': '{"recordId":"RCS3ffmbiL","requestId":"1-3-diHOsUpMZsWnB5Bp",'
                'PartitionKey': 'RCS3ffmbiL'
                'ApproximateArrivalTimestamp': '2024-10-24 14:23:43.000'
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
            response = self.kinesis_client.get_records(ShardIterator=shard_iterator, Limit=1000)
            if not response["Records"]:
                break

            for record in response["Records"]:
                records_in_shard[record[const.SEQ_NUM]] = {
                    const.DATA: record[const.DATA].decode("utf-8"),
                    const.PARTITION_KEY: record["PartitionKey"],
                    const.TIMESTAMP: record[const.TIMESTAMP].strftime("%Y-%m-%d %H:%M:%S.%f"),
                }

            # 次のイテレーターを取得
            shard_iterator = response["NextShardIterator"]
        shard_map[shard_id] = records_in_shard
        return shard_map
