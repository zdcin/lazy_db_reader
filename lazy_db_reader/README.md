# lazy_db_reader

这本来是一个导数据的程序，从mysql中读数据，更新到hbase中，主要有价值的地方是实现了遍历数据表，并且懒加载的功能，
这样整个输入数据集就可以作为一个seq来使用了，构造lazy seq的过程中我参考了line-seq的实现方式。 再通过partition
函数，还可以实现批量写入，这是我第一个真正实用的clojure程序，clojure写程序感觉很好 ：）

## Usage

FIXME

## License

Copyright © 2014 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
