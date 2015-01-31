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


method_config = {
    'LogisticRegression' : {
        'method' : sklearn.linear_model.LogisticRegression
    },
    'SVC' : {
        'method' : sklearn.svm.SVC
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


def dict_update(source, diffs):
    result=dict(source)
    result.update(diffs)
    return result

def dict_remove(source, rm):
    result=dict(source)
    for i in rm:
        del result[i]
    return result


def learn_partition(partition):
    #rdd format: label_prefix, label, label_file_path, feature_file_path, fold_number
    cur_label_path = None
    cur_label_matrix = None
    cur_feature_path = None
    cur_feature_matrix = None

    #label_prefix, label, label_file_path, feature_file_path, fold_number
    for task in partition:
        if task['label_path'] != cur_label_path:
            cur_label_matrix = pandas.read_csv(task['label_path'], sep="\t", index_col=0)
            if task['transpose']:
                cur_label_matrix = cur_label_matrix.transpose()
            cur_label_path = task['label_path']
        if task['feature_path'] != cur_feature_path:
            cur_feature_matrix = pandas.read_csv(task['feature_path'], sep="\t", index_col=0)
            if task['transpose']:
                cur_feature_matrix = cur_feature_matrix.transpose()
            cur_feature_path = task['feature_path']
        yield learn_label(
            label_matrix=cur_label_matrix,
            feature_matrix=cur_feature_matrix,
            **dict_remove(task, ["transpose", "feature_path", "label_path"]))


def learn_label_path(label_prefix, label_path, feature_path, label, method, params, fold=None, fold_count=None, transpose=False):
    feature_matrix = pandas.read_csv(feature_path, sep="\t", index_col=0).fillna(0.0)
    label_matrix = pandas.read_csv(label_path, sep="\t", index_col=0)

    if transpose:
        feature_matrix = feature_matrix.transpose()
        label_matrix = label_matrix.transpose()

    return learn_label(label_prefix=label_prefix,
        label_matrix=label_matrix,
        feature_matrix=feature_matrix, label=label,
        fold=fold, fold_count=fold_count, method=method, params=params)


def learn_label(label_prefix, label_matrix, feature_matrix, label, method, params, fold=None, fold_count=None):

    isect = feature_matrix.index.intersection(label_matrix.index)

    labels = pandas.DataFrame(label_matrix[label]).reindex(isect)
    features = feature_matrix.reindex(isect)

    if fold is None or fold_count is None:
        train_label_set = numpy.ravel(labels)
        train_obs_set = features
        test_label_set = train_label_set
        test_obs_set = train_obs_set
    else:
        kf = cross_validation.KFold(len(isect), n_folds=fold_count, shuffle=True, random_state=42)
        train_idx, test_idx = list(kf)[fold]
        train_label_set = numpy.ravel(labels.iloc[train_idx])
        train_obs_set = features.iloc[train_idx]
        test_label_set = numpy.ravel(labels.iloc[test_idx])
        test_obs_set = features.iloc[test_idx]

    train_pos_label_count = sum(numpy.ravel(train_label_set != 0))
    test_pos_label_count = sum(numpy.ravel(test_label_set != 0))
    train_neg_label_count = sum(numpy.ravel(train_label_set == 0))
    test_neg_label_count = sum(numpy.ravel(test_label_set == 0))

    rval = {
        'train_pos_label_count' : train_pos_label_count,
        'test_pos_label_count' : test_pos_label_count,
        'train_neg_label_count' : train_neg_label_count,
        'test_neg_label_count' : test_neg_label_count,
    }
    if label_prefix is not None:
        rval['label'] = label_prefix + ":" + label
    else:
        rval['label'] = label

    if fold is not None and fold_count is not None:
        rval['fold'] = fold
        rval['fold_count'] = fold_count
        rval['name'] = rval['label'] + ":" + str(fold)
    else:
        rval['name'] = rval['label']

    if len(set(train_label_set)) > 1 and train_pos_label_count > 2 and test_pos_label_count > 2:
        lr = method_config[method]['method']( **params )
        lr.fit(train_obs_set, train_label_set)

        pred=lr.predict_proba( test_obs_set )
        fpr, tpr, thresholds = metrics.roc_curve(test_label_set, list( a[1] for a in pred ))
        try:
            roc_auc = metrics.auc(fpr, tpr)
        except ValueError:
            roc_auc = None

        predictions = zip( test_label_set, list( a[1] for a in pred ) )

        prec, recall, thresholds = metrics.precision_recall_curve(test_label_set, list( a[1] for a in pred ))
        pr_auc = metrics.auc(prec, recall, reorder=True)

        coef = dict(list(a for a in zip(features.columns, lr.coef_[0]) if a[1] != 0 ))

        non_zero = sum( list( i != 0.0 for i in lr.coef_[0]) )
        rval['roc_auc'] = roc_auc
        rval['pr_auc'] = pr_auc
        rval['coef']  = coef
        rval['intercept'] = lr.intercept_[0]
        rval['non_zero'] = non_zero
        rval['method'] = method
        rval['params'] = params
        rval['predictions'] = predictions

    return rval

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--labels", action="append", default=[])
    parser.add_argument("-f", "--features", action="append", default=[])
    parser.add_argument("-ln", "--labels-named", nargs="*", action="append", default=[])
    parser.add_argument("--transpose", "-t", action="store_true", default=False)
    parser.add_argument("--single", default=None)
    parser.add_argument("--grid", default=None)
    parser.add_argument("--spark-master", "-s", default="local")
    parser.add_argument("--max-cores", default=None)
    parser.add_argument("--folds", type=int, default=10)
    parser.add_argument("--blocks", type=int, default=None)
    parser.add_argument("--test", action="store_true", default=False)
    parser.add_argument("-o", "--out", default="models")

    args = parser.parse_args()
    args.out = os.path.abspath(args.out)

    logging.basicConfig(level=logging.INFO)

    with open(args.grid) as handle:
        txt = handle.read()
        grid_config = yaml.load(txt)

    label_files = []
    feature_files = []

    for l in args.labels:
        label_files.append( (None, os.path.abspath(l)) )
    for lset in args.labels_named:
        lname = lset[0]
        for l in lset[1:]:
            label_files.append( (lname, (l)) )

    for f in args.features:
        feature_files.append(os.path.abspath(f))

    #grid: label_prefix, label, label_file_path, feature_file_path
    grid = []
    for feature_path in feature_files:
        logging.info("Scanning: %s" % (feature_path))
        feature_matrix = pandas.read_csv(feature_path, sep="\t", index_col=0)
        if args.transpose:
            feature_matrix = feature_matrix.transpose()

        for label_prefix, label_path in label_files:
            logging.info("Scanning: %s" % (label_path))
            label_matrix = pandas.read_csv(label_path, sep="\t", index_col=0)
            if args.transpose:
                label_matrix = label_matrix.transpose()

            sample_intersect = label_matrix.index.intersection(feature_matrix.index)
            if len(sample_intersect) > 5:
                #label_set = []
                #for l in label_matrix.columns:
                #    logging.info("Checking: %s" % (l))
                #    if args.single is None or l == args.single:
                #        if sum( numpy.ravel(label_matrix[l] != 0) ) > 20:
                #            label_set.append(l)
                label_set = label_matrix.columns

                for method in grid_config.get('methods'):
                    logging.info("Setting up: %s" % (method['name']))
                    method = {
                        'label_prefix' : label_prefix,
                        'label_path' : label_path,
                        'feature_path' : feature_path,
                        'method' : method['name'],
                        'params' : method.get('params', {}),
                        'transpose' : args.transpose
                    }
                    for l in label_set:
                        n = dict(method)
                        n['label'] = l
                        grid.append(n)

    if args.test:
        for line in grid:
            print line
        sys.exit(0)

    if args.single:
        for learn_request in grid:
            if learn_request['label'] == args.single:
                print json.dumps(
                    learn_label_path(**learn_request)
                )
    else:
        from pyspark import SparkConf, SparkContext

        conf = (SparkConf()
                 .setMaster(args.spark_master)
                 .setAppName("GridLearn")
                 .set("spark.executor.memory", "1g"))
        if args.max_cores is not None:
            conf = conf.set("spark.mesos.coarse", "true").set("spark.cores.max", args.max_cores)

        sc = SparkContext(conf = conf)

        label_rdd = sc.parallelize(list(grid), len(grid) if args.blocks is None else args.blocks )
        if args.folds > 0:
            task_rdd = label_rdd.flatMap( lambda x: list( dict_update(x, {"fold" : i, "fold_count" : args.folds}) for i in range(args.folds) + [None] ) )
        else:
            task_rdd = label_rdd

        #rdd format: label_prefix, label, label_file_path, feature_file_path, fold_number
        counts = task_rdd.mapPartitions(
            lambda x: learn_partition(
                partition=x)
            )

        counts.map( json.dumps ).saveAsTextFile(args.out)
