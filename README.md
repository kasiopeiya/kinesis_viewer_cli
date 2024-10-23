# kinesis_data_viewer_cli(仮)

Kinesis Data Streams(KDS)のDataStreamに格納されたレコード情報を取得するためのCLIツール

## 利用イメージ

## 既存のレコード取得方法とその問題点

### Kinesis Data Viewer(マネコン)

- シャードを選択し、timestampやsequenceNumberを指定してレコードを取得
- trimHorizenで全てのレコードを取得できるはずだが、実際には取得が難しい
- 全体でどのくらいのレコードが保存されているかなどのメタ情報がわからない
- レコードの内容(data部)で検索できない

### KDS CLI

- 全てのレコードの取得が可能であるが
- list-shard, get-sharditelatorなどの複数コマンドを組み合わせないと、レコードが取得できないため不便
- レコードの内容(data部)で検索できない

### Apache Flink

- レコードの内容で検索できる唯一の手法だが、サービスの利用料金が高い


```bash
# 利用可能なバージョン確認
pyenv install --list
pyenv install 3.13.0
# インストールされたバージョン確認
pyenv versions
# バージョン切り替え
pyenv global 3.13.0
```

```bash
curl -sSL https://install.python-poetry.org | python -
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
poetry --version

# poetry自体のupdate
poetry self update
poetry init
poetry install
poetry shell
```

イメージ

```bash
# シャード一覧表示
kdv list-shard [データストリーム名]

# シャードごとにどのくらいの数のレコードが保存されているか表示
kdv summary [データストリーム名]

# シャードごとに保存されているレコード情報を一覧表示
# シャードIDは選択性
kdv list_records [データストリーム名]

```
