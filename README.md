
# bigqueryでUDFとwindow関数を使う　

## bigqueryへのpandasからのアップロード
pandasで読み取って、このpandasの構造のまま転送することができる  

(AnacondaのPythonがインストールされているという前提で勧めます)  
```console
$ conda install pandas-gbq --channel conda-forge
```
Kaggle Open Datasetの[data-science-for-good](https://www.kaggle.com/passnyc/data-science-for-good/home)という、ニューヨーク州の学校の情報のデータセットを利用します。  

デーブルデータはこの様になっています。全部は写っていなく、一部になります。  
<div align="center">
  <img width="700px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532097798726_image.png">
</div>

```python
import pandas as pd
pd.set_option("display.max_columns", 120)
df = pd.read_csv('./2016 School Explorer.csv')

# カラム名がアンダーバーと半角英数字以外認めないので、その他を消します
def replacer(c):
    for r in [' ', '?', '(', ')','/','%', '-']:
        c = c.replace(r, '')
    return c
df.columns = [replacer(c) for c in df.columns]

# BigQueryへアップロード
df.to_gbq('test.test2', 'gcp-project')
```

<div align="center">
  <img width="400px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532098422942_image.png">
</div>
<div align="center"> 図1. GCPのBigQueryにテーブルが表示される </div>

## Window関数
SQLは2011年から2014年までちょこちょことレガシーSQLを使っていた関係で、マジ、MapReduceより何もできなくてダメみたいなことをしばらく思っていたのですが、Standart SQLを一通り触って強い（確信）といたりました。  
具体的には、様々な操作を行うときに、ビューや一時テーブルを作りまくる必要があったのですが、window関数を用いると、そのようなものが必要なくなってきます。  

Syntaxはこのようなになり、要素の指定のところにそのまま書くことができます。  
```sql
RANK() OVER(partition by city order by PercentWhite desc) 
```
これは、pandasで書くとこのような意味です。
```python
def ranker(df):
    df = df.sort_values('PercentWhite', ascending=False)
    df['rank'] = np.arange(len(df)) + 1
    return df
df.groupby(by=['City']).apply(ranker)[['City', 'PercentWhite','rank']].head(200)
```
<div align="center">
  <img width="700px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532101462609_image.png">
</div>
<div align="center"> 図2. 処理フロー </div>


Aggは別にsumやmeanなどの集約である必要もなないのですが、処理フローとしてはこの様になっています。これはPandasに比べて圧倒的に早いらしいので、ビッグデータになるにつれて、BigQueryの優位性が活かせそうです。

**toy problem: ニューヨーク州の街毎の白人の大きさランキング**  
```sql
select
  SchoolName
  , RANK() over(partition by city order by PercentWhite desc)
  , city
  , PercentWhite
 from
  test.test
 ;
```
出力
<div align="center">
  <img width="750px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532102262039_image.png">
</div>


## Standerd SQLでUDF(UserDefinedFunction)を定義する

前項ではBigQueryに組み込み関数のRANK関数を用いましたが、これを含め、自身で関数をJavaScriptで定義可能です（可能ならばPythonとかのほうが良かった。。）。  

JavaScriptで記述するという側面さえ除けば、かなり万能に近い書き方も可能になりますので、こんな不思議なことを計算することもできます。(おそらく、もっと効率の良い方法があると思いますが)  

**window関数内部で値の最大値の大きさを1として変形を行う** 
白人のパーセンテージを最大にしめる割合を１としてノーマライズします。  

`CREATE TEMPORARY FUNCTION`で入出力の値と型決めて、このように書くことができます

```sql
CREATE TEMPORARY FUNCTION norm(xs ARRAY<STRING>, rank INT64)
RETURNS FLOAT64
LANGUAGE js AS """
  const xs2 = xs.map( x => x.replace("%", "") ).map( x => parseFloat(x) )
  const max = Math.max.apply(null, xs2)
  const xs3 = xs2.map( x => x/max ).map( x => x.toString() )
  return xs3[rank-1];
  """;
select 
  SchoolName
  ,norm( 
    ARRAY_AGG(PercentWhite) over(partition by city order by PercentWhite desc) ,
    Rank() over(partition by city order by PercentWhite desc) 
  )
  ,city
  , PercentWhite
 from
  test.test
 ;
```
計算結果をみると、正しく、計算できていることがわかります。  
```
```


## なかなかSQLでは難しい操作

## outer source

talking dataのtkmさんのbigqueryでの前処理　　

`https://gist.github.com/tkm2261/1b3c3c37753e55ed2914577c0f96d222`

