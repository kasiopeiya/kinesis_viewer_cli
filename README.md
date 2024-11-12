# kinesis data viewer cli <!-- omit in toc -->

Kinesis Data Streams(KDS)のDataStreamに格納されたレコード情報を可視化する対話式CLIツール

![chrome-capture-2024-11-12](https://github.com/user-attachments/assets/259ea305-fd83-4df5-8a83-a9054c0c1280)

## 目次 <!-- omit in toc -->

- [提供機能](#提供機能)
- [使用上の注意点](#使用上の注意点)
- [ツールの使い方](#ツールの使い方)
  - [前提](#前提)
  - [ツール実行コマンド](#ツール実行コマンド)
  - [コマンドラインオプション](#コマンドラインオプション)
- [本ツールが必要な理由](#本ツールが必要な理由)
- [技術選定の理由](#技術選定の理由)
- [テストの考え方](#テストの考え方)
  - [Unitテスト](#unitテスト)
  - [E2Eテスト](#e2eテスト)


## 提供機能

| command | 対象 | 出力先 | 動作 |
| -- | -- | -- | -- |
| summary | 全シャード | ターミナル | シャードごとの格納レコード数や最後に追加された日時をテーブル形式で表示  |
| dump_records | シャード | ターミナル/csv | シャード内の全ての格納レコードをテーブル形式、またはcsvファイルに出力  |
| show_recent_records | シャード | ターミナル | 最近追加された最大100レコードをテーブル形式で表示  |
| search_record | 全シャード | ターミナル| 指定されたkeyをもとにDataの内容でレコードを検索しjson形式で出力  |

## 使用上の注意点

- 本ツールは対象のストリームの全てのレコードを取得するため、一時的に大量の読み取りキャパシティを消費する
- そのため本番稼働中のシステムに対しての使用は慎重に検討すること

## ツールの使い方

### 前提

- Python3.9以上、Poetryがインストールされていること
- AWSのアクセス権限があること（少なくともKDSの読み取り権限が必要）

### ツール実行コマンド

```bash
# 仮想環境の構築とパッケージインストール
poetry install
# 仮想環境に入る
poetry shell
# ツール実行
python -m kdv
```
### コマンドラインオプション

サブコマンドとそのオプションを指定することでNon-interactiveに実行することも可能

summary

```bash
python -m kdv main \
    --region ap-northeast-1 \
    --target_stream_name hoge \
    --command summary
```

dump_records

```bash
python -m kdv main \
    --region ap-northeast-1 \
    --target_stream_name hoge \
    --command dump_records \
    --target_shard shardId-000000000000 \
    --dump_output terminal
```

show_recent_records

```bash
python -m kdv main \
    --region ap-northeast-1 \
    --target_stream_name hoge \
    --command show_recent_records \
    --target_shard shardId-000000000000
```

search_record

```bash
python -m kdv main \
    --region ap-northeast-1 \
    --target_stream_name hoge \
    --command search_record \
    --search_key hello
```

## 本ツールが必要な理由

マネジメントコンソールのData Viewer機能では以下のような問題点がある

- 多くのケースでsequenceNumberやtimestampを指定した検索が必要となりやや不便
- ストリーム全体のメタ情報が見れない
  - 全部でいくつのレコードが保存されているかなどがわからない
- データの内容を使った検索機能の提供がない
  - Apache Flinkを使えば検索は可能だが費用が高い

-> これらData Viewerの問題を補うため、独自のCLIを開発し、より可視化しやすいようにした

## 技術選定の理由

- 開発言語: Python
  - 理由: AWS SDKが使いやすい、開発スピードが早い、pyinstaller等を使ったバイナリ配布も可能
- パッケージマネージャー・仮想環境管理: Poetry
  - 理由: 近年よく使用されている、Python公式の設定ファイルpyproject.tomlを使用して依存関係を管理できる

## テストの考え方

Unitテスト(Solitary Tests)とE2Eテストを実施、利用サービスはKDSのみのためIntegrationテストはなし

### Unitテスト

- pytestとmotoを使用
- motoはAWSのサービスをmockするライブラリで、実際にリソースを作成せずにAWSサービスのテストが可能
- 正常系、異常系ともにテスト
- coverage取得しているが、インタラクティブなツールのためテスト困難なケースもあり、100%にはできない

### E2Eテスト

- pytestとsubprocessを使用
- subprocessでツールをCLI実行させ出力を確認、これをE2Eとする
- Testing Pyramidの考えに則り、E2Eテストケースは最小限とし、正常系の基本動作（いわゆるHappy Path）のみをテスト対象
