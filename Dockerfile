FROM python:3.13-slim

# vimやcurlなどの必要パッケージのインストール
RUN apt-get update && apt-get install -y git curl vim unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    export PATH="$PATH:/aws/dist" && \
    rm awscliv2.zip

# 日本語入力設定
RUN apt-get install -y locales && locale-gen ja_JP.UTF-8
ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL=ja_JP.UTF-8
RUN localedef -f UTF-8 -i ja_JP ja_JP.utf8

# vimの設定
COPY .vimrc /root/.vimrc:ro

# 仮想環境設定、パッケージインストール
RUN pip install poetry && poetry config virtualenvs.create true

RUN apt-get clean && rm -rf /var/lib/apt/lists/*
