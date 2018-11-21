## Run 
```
$ sbt "runMain com.iuriisusuk.Main"
```
```
~/spark-2.3.1-bin-hadoop2.7/bin/spark-submit \
  --class "com.iuriisusuk.QueryRecommendationApp" \
  --master local \
  target/scala-2.11/wikimedia-listings_2.11-0.1.0.jar
```


## Test
```sh
$ grep -c '{{do' src/main/resources/enwikivoyage-20170620-pages-articles.xml
$ 36406

$ grep -c '{{see' src/main/resources/enwikivoyage-20170620-pages-articles.xml
$ 66317

$ wc -l pois.csv
$ 102382

```

**102382 ~ 36406 + 66317**

_There are some edge casses which must be found with deeper analysis_
