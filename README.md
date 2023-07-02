# このリポジトリについて

* Azure CosmosDB for MongoDB のPythonによる操作サンプルレポジトリです
* productsという仮想のjsonデータをpymongoを通じて書き込んだり取得したりします

# 使い方

## 1.  依存パッケージのインストール

```bash
pin install -r requirements
```

## 2.  接続文字列の設定

```bash
export COSMOS_CONNECTION_STRING="接続文字列"
```

接続文字列は CosmosDBインスタンス詳細画面の「クイックスタート」から確認できます

## 3. 実行

特に引数はありません。回数やクエリ等のパラメータを変更したい場合にはソースを直接変更してください

```bash
python define-db-collection-index.py
```

各ファイルの役割は以下の通りです



* define-db-collection-index.py

    CosmosDBにDB, adventureworksというDB、productsというcollection、_id列とname列へのインデックス追加を行います

* input-data.py

    productsに指定件数分のデータを生成しながら投入します。

* input-data-parallel.py

    productsに指定件数分のデータを生成しながら投入を並列に実行します。

* get-item-num.py

    products collection内のデータ件数を取得します

* find-with-index.py

    インデックスのあるname列に文字列を指定して要素取得を行います

* find-without-index.py

    インデックスのないcity列に文字列を指定して要素取得を行います

* truncate-data.py

    products collectionの全てのデータを削除します

* drop-collection.py

    products collectionをcollection定義ごと削除します

* run-all.py

    dbの作成、データの投入、データの検索を一度に行います