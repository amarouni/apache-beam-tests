## How to run the tests

```
mvn compile exec:java -Dexec.mainClass=fr.marouni.apache.beam.wordcount.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner
```
