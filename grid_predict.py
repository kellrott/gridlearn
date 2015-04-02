#!/usr/bin/env python

import sys
import os
import argparse
import pandas
import numpy
import json
import yaml
import logging

import sklearn.linear_model
import sklearn.svm
import sklearn.lda
import sklearn.naive_bayes
import sklearn.ensemble

from sklearn import cross_validation
from sklearn import metrics

from pyspark import SparkConf, SparkContext


class LogisticRegressionModel:

    def __init__(self, model):
        self.model_data = model
        self.coef =  pandas.Series(self.model_data['coef'])
        self.intercept = self.model_data['intercept']

    def predict(self, sample):
        common = self.coef.index.union(sample.index)
        if len(common) == 0:
            raise Exception("No model overlap")
        left = self.coef.reindex(index=common, copy=False, fill_value=0.0)
        right = sample.reindex(index=common, copy=False, fill_value=0.0)

        margin = left.dot(right) + self.intercept
        try:
            score = 1.0/ (1.0 + numpy.exp(-margin))
            return score
        except OverflowError:
            return numpy.nan


method_config = {
    'LogisticRegression' : {
        'method' : sklearn.linear_model.LogisticRegression,
        'model' : LogisticRegressionModel,
        'attributes' : {
            "coef" : "coef_",
            "intercept" : "intercept_"
        }
    },
    'SVC' : {
        'method' : sklearn.svm.SVC,
        'attributes' : {
            "coef" : "coef_",
            "intercept" : "intercept_"
        }
    },
    'LDA' : {
        'method' : sklearn.lda.LDA
    },
    'GaussianNB' : {
        'method' : sklearn.naive_bayes.GaussianNB
    },
    'RandomForestClassifier' : {
        'method' : sklearn.ensemble.RandomForestClassifier
    }
}

def dict_remove(source, rm):
    result=dict(source)
    for i in rm:
        del result[i]
    return result

def predict_partition(partition):
    cur_feature_path = None
    cur_feature_matrix = None

    for model_name, task_tuple in partition:
        models = list(task_tuple[0])
        model = models[0]
        tasks = list(task_tuple[1])
        for task in tasks:
            if task['feature_path'] != cur_feature_path:
                cur_feature_matrix = pandas.read_csv(task['feature_path'], sep="\t", index_col=0)
                if task['feature_transpose']:
                    cur_feature_matrix = cur_feature_matrix.transpose()
                cur_feature_path = task['feature_path']
            yield predict_label(
                feature_matrix=cur_feature_matrix,
                model=model,
                **dict_remove(task, ["feature_transpose", "feature_path", "model_name"]))

def predict_label(feature_matrix, model, sample):

    if 'model' not in method_config[ model['method'] ]:
        return None

    inst = method_config[ model['method'] ]['model'](model)
    pred = inst.predict( feature_matrix.ix[sample] )
    out = {
        'model_name' : model['name'],
        'sample' : sample,
        'prediction' : pred
    }
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--models")
    parser.add_argument("-f", "--features", action="append", default=[])
    parser.add_argument("-tf", "--trans-features", action="append", default=[])

    parser.add_argument("--single", default=None)
    parser.add_argument("--spark-master", "-s", default="local")
    parser.add_argument("--max-cores", default=None)
    parser.add_argument("--blocks", type=int, default=100)
    parser.add_argument("--test", action="store_true", default=False)
    parser.add_argument("-o", "--out", default="predictions")
    parser.add_argument("--limit", type=int, default=None)

    args = parser.parse_args()
    args.out = os.path.abspath(args.out)

    logging.basicConfig(level=logging.INFO)

    feature_files = []
    for f in args.features:
        feature_files.append( (os.path.abspath(f), False) )
    for f in args.trans_features:
        feature_files.append( (os.path.abspath(f), True) )

    conf = (SparkConf()
             .setMaster(args.spark_master)
             .setAppName("GridLearn")
             .set("spark.executor.memory", "1g"))
    if args.max_cores is not None:
        conf = conf.set("spark.mesos.coarse", "true").set("spark.cores.max", args.max_cores)

    sc = SparkContext(conf = conf)

    model_rdd = sc.textFile( args.models ).map( json.loads ).map( lambda x: (x['name'], x) ).cache()
    model_names = model_rdd.map( lambda x: x[0] ).collect()

    print "Models", model_names

    #grid: label_prefix, label, label_file_path, feature_file_path
    grid = []
    for feature_path, feature_transpose in feature_files:
        logging.info("Scanning: %s" % (feature_path))
        feature_matrix = pandas.read_csv(feature_path, sep="\t", index_col=0)
        if feature_transpose:
            feature_matrix = feature_matrix.transpose()
        for f in feature_matrix.index:
            for model in model_names:
                grid.append({
                    'feature_path' : feature_path,
                    'feature_transpose' : feature_transpose,
                    'model_name' : model,
                    'sample' : f
                })
    if args.test:
        for g in grid:
            print g
        sys.exit(0)
    task_rdd = sc.parallelize(list(grid), len(grid) if args.blocks is None else args.blocks ).map(
        lambda x: (x['model_name'], x)
    )

    model_rdd.cogroup( task_rdd ).mapPartitions( predict_partition ).saveAsTextFile(args.out)
