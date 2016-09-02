# Elasticsearch demo

Demo for Elasticsearch v.4.0 (might run on subsequent 2.x releases, too; does not work on Elasticsearch 0.x and 1.x; I have not tested it on Elasticsearch 5, which is still in beta).

All documents and qrels are property of the [National Institute of Standards and Technology](http://www.nist.gov) and have been released from research purposes as part of [TREC](http://trec.nist.gov).

## What you need to run this demo

1. Python 3 (if you're still using Python 2, I strongly encourage you to switch. Check out [this page](https://eev.ee/blog/2016/07/31/python-faq-why-should-i-use-python-3/) for why I suggest the switch).
2. A fairly recent JVM version. At the time of writing, "fairly recent" equals to Oracle JVM v.1.7u55 or above, or OpenJDK v.1.7.55 or above. Check [this page](https://www.elastic.co/support/matrix) for more infos about requirements for Elasticsearch.
3. Elasticsearch binaries. You can get them from [the official website](https://www.elastic.co/downloads/elasticsearch).
4. *Optional*: a C compiler. I tried to include pre-compiled versions of `trec_eval` in the `bin` folder (macOS, Debian/Ubunutu, and Windows); This program will try to use the appropriate one based on the platform your running it. If it fails, please download the source code for `trec_eval` from the [NIST website](http://trec.nist.gov/trec_eval/trec_eval_latest.tar.gz) and compile it by yourself (it should be as easy as navigating to the deflated directory and typing `make`). Then copy the compiled binary to the bin folder.

## Usage

1. Install requirements in `requirements.txt`; that is, run `pip3 install -r requirements.txt`.
2. Download the data files (documents, queries, and qrels) from [this page](https://github.com/lucasoldaini/elasticsearch-demo/releases/tag/data-v.1.0). Unzip them in the root of this project (i.e., where this file is).
2. Start Eilasticsearch. Assuming that you have unzipped Elasticsearch to the folder where this file is located, you can execute `./elasticsearch-2.4.0/bin/elasticsearch` if you are on a UNIX system, or `./elasticsearch-2.4.0/bin/elasticsearch.bat` if you are on Windows. For more information on how to install and run Elasticsearch, please visit [this page](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html).
3. Execute `index.py`. This will index the collection.
4. Execute `search.py`. This will search the collection and evaluate the results.
