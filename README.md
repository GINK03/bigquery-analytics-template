
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
\
```

## Window関数

## Standerd SQLでUDF(UserDefinedFunction)を定義する

## なかなかSQLでは難しい操作

## outer source

talking dataのtkmさんのbigqueryでの前処理　　

`https://gist.github.com/tkm2261/1b3c3c37753e55ed2914577c0f96d222`

