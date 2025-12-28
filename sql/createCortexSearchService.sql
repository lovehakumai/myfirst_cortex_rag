-- 検索サービスを作成する
-- AIはベクトル数値としてチャンクを読み取り、曖昧な検索においても類似した情報を引っ張ることができる
-- この検索サービスでは、AIがベクトル化したいテキストを指定するとともに、情報のフィルタリングに用いる情報, SnowFlakeのWarehouseを指定するのに使う、また高精度で的確なチャンクを得るための検索インデックスを内部的に作成している
create or replace CORTEX SEARCH SERVICE CC_QUICKSTART_CORTEX_SEARCH_DOCS.DATA.CC_SEARCH_SERVICE_CS
ON chunk -- 検索のメインとなる列は chunk列であると指定する
ATTRIBUTES category -- 検索の情報の絞り込みにはcategoryを使うと指定する
warehouse = COMPUTE_WH -- 利用するWarehouse
TARGET_LAG = '1 minute' -- 元テーブルが更新されたら最大1minで検索サービスに反映して欲しいという指定。ニアリアルタイムで最新情報にアクセス可能になる
as (
    select chunk,
        chunk_index,
        relative_path,
        file_url,
        category
    from CC_QUICKSTART_CORTEX_SEARCH_DOCS.DATA.DOCS_CHUNKS_TABLE
);
