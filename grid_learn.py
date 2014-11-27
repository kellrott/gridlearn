#!/usr/bin/env python

import sys
import os
import argparse
import pandas
import numpy
import json
from sklearn.linear_model import LogisticRegression
from sklearn import cross_validation
from sklearn import metrics


def get_count(path, label):
    matrix = pandas.read_csv(path, sep="\t", index_col=0)
    return (label, sum( matrix[label] != 0 ))

def learn_label(label_prefix, label_path, feature_path, label, fold=None, fold_count=None, transpose=False, penalty="l2", C=1.0):
    observation_matrix = pandas.read_csv(feature_path, sep="\t", index_col=0).fillna(0.0)
    label_matrix = pandas.read_csv(label_path, sep="\t", index_col=0)

    print "Learning", label, fold

    if transpose:
        observation_matrix = observation_matrix.transpose()
        label_matrix = label_matrix.transpose()

    isect = observation_matrix.index.intersection(label_matrix.index)

    labels = pandas.DataFrame(label_matrix[label]).reindex(isect)
    observations = observation_matrix.reindex(isect)

    if fold is None or fold_count is None:
        train_label_set = numpy.ravel(labels)
        train_obs_set = observations
        test_label_set = train_label_set
        test_obs_set = train_obs_set
    else:
        kf = cross_validation.KFold(len(isect), n_folds=fold_count, shuffle=True, random_state=42)
        train_idx, test_idx = list(kf)[fold]
        train_label_set = numpy.ravel(labels.iloc[train_idx])
        train_obs_set = observations.iloc[train_idx]
        test_label_set = numpy.ravel(labels.iloc[test_idx])
        test_obs_set = observations.iloc[test_idx]

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
        rval['label'] = rval['label'] + ":" + str(fold)

    if train_pos_label_count > 2 and test_pos_label_count > 2:
        lr = LogisticRegression(penalty=penalty, C=C)
        lr.fit(train_obs_set, train_label_set)

        pred=lr.predict_proba( test_obs_set )
        fpr, tpr, thresholds = metrics.roc_curve(test_label_set, list( a[1] for a in pred ))
        roc_auc = metrics.auc(fpr, tpr)
        
        predictions = zip( test_label_set, list( a[1] for a in pred ) )

        prec, recall, thresholds = metrics.precision_recall_curve(test_label_set, list( a[1] for a in pred ))
        pr_auc = metrics.auc(prec, recall, reorder=True)

        coef = dict(list(a for a in zip(observations.columns, lr.coef_[0]) if a[1] != 0 ))

        non_zero = sum( list( i != 0.0 for i in lr.coef_[0]) )
        rval['roc_auc'] = roc_auc
        rval['pr_auc'] = pr_auc
        rval['coef']  = coef
        rval['intercept'] = lr.intercept_[0]
        rval['non_zero'] = non_zero
        rval['method'] = 'LogisticRegression'
        rval['predictions'] = predictions

    return rval

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--labels", action="append", default=[])
    parser.add_argument("-f", "--features", action="append", default=[])
    parser.add_argument("-ln", "--labels-named", nargs=2, action="append", default=[])
    parser.add_argument("--transpose", "-t", action="store_true", default=False)
    parser.add_argument("--single", default=None)
    parser.add_argument("--penalty", "-p", default="l2")
    parser.add_argument("-C", type=float, default=1.0)
    parser.add_argument("--spark-master", "-s", default="local")
    parser.add_argument("--max-cores", default=None)
    parser.add_argument("--folds", type=int, default=10)
    parser.add_argument("-o", "--out", default="models")

    args = parser.parse_args()
    args.out = os.path.abspath(args.out)

    label_files = []
    feature_files = []

    for l in args.labels:
        label_files.append( (None, os.path.abspath(l)) )
    for lname, l in args.labels_named:
        label_files.append( (lname, (l)) )

    for f in args.features:
        feature_files.append(os.path.abspath(f))

    #grid: label_prefix, label, label_file_path, feature_file_path
    grid = []
    for label_prefix, label_path in label_files:
        print "Scanning", label_path
        label_matrix = pandas.read_csv(label_path, sep="\t", index_col=0)
        if args.transpose:
            label_matrix = label_matrix.transpose()
        for feature_path in feature_files:
            feature_matrix = pandas.read_csv(feature_path, sep="\t", index_col=0)
            if args.transpose:
                feature_matrix = feature_matrix.transpose()

            sample_intersect = label_matrix.index.intersection(feature_matrix.index)
            if len(sample_intersect) > 5:
                label_set = []
                for l in label_matrix.columns:
                    if sum( numpy.ravel(label_matrix[l] != 0) ) > 20:
                        grid.append( [label_prefix, l, label_path, feature_path] )

    if args.single:
        for label_prefix, label, label_file_path, feature_file_path in grid:
            if label == args.single:
                print json.dumps(
                    learn_label(
                        label_path=label_file_path, feature_path=feature_file_path, label=args.single,
                        fold=0, fold_count=args.folds, transpose=args.transpose, label_prefix=label_prefix,
                        penalty=args.penalty, C=args.C )
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

        label_rdd = sc.parallelize(list(grid), len(grid))
        if args.folds > 0:
            task_rdd = label_rdd.flatMap( lambda x: list( x + [i] for i in range(args.folds) + [None] ) )
        else:
            task_rdd = label_rdd.map( lambda x: x + [None] )

        #rdd format: label_prefix, label, label_file_path, feature_file_path, fold_number
        counts = task_rdd.map(
            lambda x: learn_label(
                label_path=x[2], feature_path=x[3], label=x[1], fold=x[4],
                fold_count=args.folds, transpose=args.transpose, label_prefix=x[0],
                penalty=args.penalty, C=args.C)
            )

        counts.map( json.dumps ).saveAsTextFile(args.out)
