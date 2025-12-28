-- This project is based on this training
-- https://www.snowflake.com/en/developers/guides/ask-questions-to-your-own-documents-with-snowflake-cortex-search/

-- create the environment
create database cc_quickstart_cortex_search_docs;
create schema data;
create or replace stage docs encryption = (type = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE=true); -- Stageにある docsがファイルカタログを維持するようになる、これにより directory(@docs)でファイルのメタデータを一括で取得できる
ls @docs;


-- Preprocessing of docs, extract text data from pdf format
-- we'll make the 3 objects below
-- 1. DOCS_CHUNK_TABLE
-- 2. DOCS STAGING 
-- 3. PDF_TEXT_CHUNKER function

-- STEP1 : read the text from pdf document in staging with layout mode
create or replace temporary table raw_text as -- temporaryはcteと異なり、セッション中のみ一時的に保存される処理。cteはクエリ実行後すぐに削除されるため、処理が重いが一時的にしかいらないテーブルを再利用できるように利用する
with file_table as (
    select
        relative_path,
        size,
        file_url,
        build_scoped_file_url(@docs, relative_path) as scoped_file_url, -- Snowflake内部専用のURLを作成している, 後で作成するテーブル（DOCS_CHUNKS_TABLE）に保存され、アプリケーション側からそのファイルを参照したり、ダウンロードリンクを作成したりする際の基点として使われる
        to_file('@docs', relative_path) as docs -- ai_parse_document関数で利用するためのファイル自体を渡すためにファイルオブジェクトに変換する関数
    from
        directory(@docs)
)
select
    relative_path,
    size,
    file_url,
    scoped_file_url,
    to_varchar(
        snowflake.cortex.ai_parse_document (
                docs,
                {'mode': 'LAYOUT'} -- docsのファイル内容から抽出したテキストを段落やリストなどを考慮した形で抽出する(これがないと文章の塊がそのまま返ってくる)
            ): content -- Snowflakeの辞書型データのキー指定方法。末尾に : キー名と指定する。
        ) as extracted_layout
from
    file_table;

-- STEP2: create the table for saving the chunks of each pdf files
create or replace TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CHUNK_INDEX INTEGER, -- Index for the text
    CATEGORY VARCHAR(16777216) -- Will hold the document category to enable filtering
);


-- STEP3: Separate the text into short strings in the case of that extracted too big text datas
insert into docs_chunks_table (relative_path, size, file_url, scoped_file_url, chunk, chunk_index)
    select relative_path,
            size,
            file_url,
            scoped_file_url,
            c.value::text as chunk, -- チャンク
            c.index::integer as chunk_index -- 元の文章の何番目にあったかを示すインデックス(文脈保持の目的)
    from
        raw_text, -- 抽出したテキスト文を含むがチャンク化していないテーブル
        lateral flatten(input => snowflake.cortex.split_text_recursive_character ( 
            -- 1. flatten : 1行にまとまっているリスト構造やJSON構造を各行に分ける処理
            -- 2. split_text_recursive_character : ルールに従い再起的にチャンク化する
            extracted_layout, -- 生の文章
            'markdown', -- 元のテキストがマークダウンであることを指定
            1512, -- 1つの最大チャンク数(推奨は512トークンだが、今回は文字ベース)
            256, -- OVerlap : チャンク前後の256文字を重複させて、文脈が失われるのを防ぐ
            ['\n\n', '\n', ' ', ''] -- 区切り文字の優先順位、ダブル改行 , 改行, スペース, 空白の順番
        )) c;

-- labeling the product categories
create or replace temporary table docs_categories as with unique_documents as (
    select
        distinct relative_path, chunk
    from
        docs_chunks_table
    where 
        chunk_index = 0 -- 各ファイルの0番目のチャンクを見て、カテゴライズさせる
),
docs_category_cte as ( 
    select
        relative_path,
        trim(snowflake.cortex.AI_CLASSIFY ( -- テキストを分類するための関数
            'Title:' || relative_path || 'Content:' || chunk, -- AIに判定して欲しい文章を生成している
            ['Bike', 'Snow'] -- どのカテゴリに分類して欲しいかを指定している
        )['labels'][0], '"') as category -- ['labels']の結果は ["Bike"]のように返ってくるので、[0]指定で要素のみ取得　＆　trim(, '"')でダブルクォーテーション排除
    from
        unique_documents
)
select
    *
from
    docs_category_cte;

-- check the table to see if the number of category and the categories are same or not
select * from docs_categories;

-- update the table using the chunk of text which is owned by cortex search service
update docs_chunks_table
    set category = docs_categories.category
    from docs_categories
    where docs_chunks_table.relative_path = docs_categories.relative_path;
