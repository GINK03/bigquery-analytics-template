
# bigqueryでUDFとwindow関数を使う　

転職してからMapReduceそのもののサービスや改良したサービスであるCloud DataFlowなどのサービスより、初手BigQueryが用いられることが増えてきました。分析環境でのプラットフォームを何にするかの文化でしょう。　

BigQueryの優れた面がLegacy　SQLを使っていたときほとんどなにもないのでは、と考えていたこともあったのですが、Standart　SQLならばWindow関数を利用し、さらに非構造化データに対してもUser Define Functionを用いることでJavaScriptをアドホックに用いることで、かなり良いところまで行けるということがわかりました。  

window関数の例と、User Define Functionとの組み合わを記します。

## bigqueryへのpandasからのアップロード
pandasで読み取って、このpandasの型情報のまま転送することができる  

(AnacondaのPythonがインストールされているという前提で勧めます)  
```console
$ conda install pandas-gbq --channel conda-forge
```
サンプルデータセットとして、Kaggle Open Datasetの[data-science-for-good](https://www.kaggle.com/passnyc/data-science-for-good/home)という、ニューヨーク州の学校の情報のデータセットを利用します。  

デーブルデータはこの様になっています。全部は写っていなく、一部になります。  
<div align="center">
  <img width="700px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532097798726_image.png">
</div>

```python
import pandas as pd
pd.set_option("display.max_columns", 120)
df = pd.read_csv('./2016 School Explorer.csv')

# BigQueryはカラム名がアンダーバーと半角英数字以外認めないので、その他を消します
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

Syntaxはこのようなになり、data-science-for-goodで街粒度で分割し、白人率でソートして、ランキングするとこのようなクエリになります。  
```sql
RANK() OVER(partition by city order by PercentWhite desc) 
```
より一般化すると、このようなもになります。  
<div align="center">
  <img width="400px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532143102283_image.png">
</div>
<div align="center"> 図2. </div>

これは、pandasで書くとこのような意味です。
```python
def ranker(df):
    df = df.sort_values('PercentWhite', ascending=False)
    df['rank'] = np.arange(len(df)) + 1
    return df
df.groupby(by=['City']).apply(ranker)[['City', 'PercentWhite','rank']].head(200)
```

BigQueryのwindow関数もpandasのgroupby.applyも似たようなフローになっています。  
<div align="center">
  <img width="700px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532101462609_image.png">
</div>
<div align="center"> 図3. 処理フロー </div>


Aggは別にsumやmeanなどの集約である必要もなないのですが、処理フローとしてはこの様になっています。BigQueryはPandasに比べて圧倒的に早いらしいので、ビッグデータになるにつれて、優位性が活かせそうです。

なお、window関数は他にもさまざまな機能があり、[GCPの公式ドキュメント](https://cloud.google.com/bigquery/sql-reference/functions-and-operators?hl=ja#analytic-functions)が最も整理されており、便利です。

**toy problem: ニューヨーク州の街毎の白人率の大きさランキング**  
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
<div align="center"> 図4. window+rank関数によるランキング </div>


## Standerd SQLでUDF(UserDefinedFunction)を定義する

前項ではBigQueryに組み込み関数のRANK関数を用いましたが、これを含め、自身で関数をJavaScriptで定義可能です（可能ならばPythonとかのほうが良かった。。）。  

JavaScriptで記述するという側面さえ除けば、かなり万能に近い書き方も可能になりますので、こんな不思議なことを計算することもできます。(おそらく、もっと効率の良い方法があると思いますが)  

**window関数で特定の値のノーマライズを行う** 
白人のパーセンテージをその街で最大にしめる大きさを１としてノーマライズします。  

UDFは`CREATE TEMPORARY FUNCTION`で入出力の値と型決めて、このように書きます

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
<div align="center">
  <img width="750px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532104857966_image.png">
</div>
<div align="center"> 図5. UDFによる任意の計算が可能 </div>

**lag関数を使わずに前のrowの値との差を計算する**   

学校の街ごとの収入に、自分よりも前のrowとの収入の差を求める。  

lag関数でも簡単に求めることができますが、JSの力とrank関数を使うことでこのようにして、rowベースの操作すらもできます。  

```sql
#standardSQL
CREATE TEMPORARY FUNCTION prev(xs ARRAY<STRING>, index INT64)
RETURNS FLOAT64
LANGUAGE js AS """
  const xs1 = xs.map( function(x) {
    if( x == null ) 
      return "0"; 
    else 
      return x;
  });
  const xs2 = xs1.map( x => x.replace(",", "") ).map( x => x.replace("$", "") ).map( x => parseFloat(x) );
  const ret = xs2[index-1-1] - xs2[index-1];
  if( ret == null || isNaN(ret)) 
    return 0.0;
  else
    return ret
  """;
select 
  SchoolName
  ,prev( 
    ARRAY_AGG(SchoolIncomeEstimate) over(partition by city order by SchoolIncomeEstimate desc) ,
    row_number() over(partition by city order by SchoolIncomeEstimate desc) 
  )
  ,city
  ,SchoolIncomeEstimate
from
  test.test;
```

<div align="center">
  <img width="750px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532139530095_image.png">
</div>
<div align="center"> 図6. 前のrowとの差を計算する </div>

このように列だけでな行方向にも拡張された操作ができ、万能とはこういう事を言うんでしょうか


## なかなかレガシーSQLでは難しかった操作ができる

window関数を用いることで、アグリゲートをする際、groupbyしてからビューを作りjoinをするというプロセスから開放されました。  

MapReduceを扱う際のモチベーションが、膨大なデータをHash関数で写像空間にエンベッティングして、シャーディングするという基本的な仕組みを理解していたので、どのようなケースにも応用しやすく、使っていました。  

MapReduceに比べて、BigQueryはcomplex　data processing（プログラミング等でアドホックな処理など）を行うことができないとされていますが、User Deine Functionを用いればJavaScriptでの表現に限定されますが行うことができます。  

<div align="center">
  <img width="600px" src="https://d2mxuefqeaa7sj.cloudfront.net/s_395C846F6BB54334ACB188FAC2F01C0FF7D15E56852EC0E8EFD1BA2A22439502_1532110445693_image.png">
</div>
<div align="center"> 図7. BigQuery(Dremel)とMapReduceの比較 </div>

 
## outer source

 - [talking dataのtkmさんのbigqueryでの前処理](https://gist.github.com/tkm2261/1b3c3c37753e55ed2914577c0f96d222)
 
 - [An Inside Look at Google BigQuery](https://cloud.google.com/files/BigQueryTechnicalWP.pdf)
 
 - [分析関数](https://cloud.google.com/bigquery/sql-reference/functions-and-operators?hl=ja#analytic-functions)

## codes
[https://github.com/GINK03/bigquery-analytics-template]
