# WordCount

> Count the words in a text.

Install dependencies with [mvn](https://mvnrepository.com/)

```sh
$ mvn install
```

## To running with spark-submit

```sh
$ mvn package
$ spark-submit --class br.com.rivendel.spark.Main ./target/map-reduce-word-count.jar <your-text-file>
```

## Author

**Leonardo A. Santos**

+ [github/Almeidaleo08](https://github.com/Almeidaleo08)

## License

Copyright © 2017 Leonardo A. Santos
