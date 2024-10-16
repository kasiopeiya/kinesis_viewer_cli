# kinesis_viewer_cli

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
kdv list [データストリーム名]

```
