# Elasticsearch demo

Demo for Elasticsearch; developed against v.2.2.

All documents and qrels in `data` are property of the [National Institute of Standards and Technology](http://www.nist.gov) and have been released from research purposes as part of [TREC](http://trec.nist.gov).

## Usage

1. install requirements in `requirements.txt`
2. set up and initialize an Elasticsearch cluster
3. run `index.py`
4. run `search.py`

## Caveats

- the binary of trec_eval in `bin` has been compiled on OS X 10.11 using `llvm`. Depending on your system, you might need to recompile it from scratch. The source code is avaliable from [the NIST website](http://trec.nist.gov/trec_eval/trec_eval_latest.tar.gz). 
