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

def learn_label(label_path, obs_path, label, fold=None, fold_count=None, transpose=False, penalty="l2", C=1.0):
    observation_matrix = pandas.read_csv(obs_path, sep="\t", index_col=0).fillna(0.0)
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

    train_label_count = sum(numpy.ravel(train_label_set != 0))
    test_label_count = sum(numpy.ravel(test_label_set != 0))

    rval = {
        'train_label_count' : train_label_count,
        'test_label_count' : test_label_count
    }
    if fold is not None and fold_count is not None:
        rval['fold'] = fold
        rval['fold_count'] = fold_count

    if train_label_count > 2 and test_label_count > 2:
        lr = LogisticRegression(penalty=penalty, C=C)
        lr.fit(train_obs_set, train_label_set)

        pred=lr.predict_proba( test_obs_set )
        fpr, tpr, thresholds = metrics.roc_curve(test_label_set, list( a[1] for a in pred ))
        roc_auc = metrics.auc(fpr, tpr)

        coef = dict(zip(observations.columns, lr.coef_[0]))

        non_zero = sum( list( i != 0.0 for i in lr.coef_[0]) )

        rval['roc_auc'] = roc_auc
        rval['coef']  = coef
        rval['intercept'] = lr.intercept_[0]
        rval['non_zero'] = non_zero

    return rval

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("observations")
    parser.add_argument("labels")
    parser.add_argument("--transpose", "-t", action="store_true", default=False)
    parser.add_argument("--single", default=None)
    parser.add_argument("--penalty", "-p", default="l2")
    parser.add_argument("-C", type=float, default=1.0)
    parser.add_argument("--spark-master", "-s", default="local")
    parser.add_argument("--max-cores", default=None)
    parser.add_argument("--folds", type=int, default=10)
    parser.add_argument("-o", "--out", default="models")

    args = parser.parse_args()

    args.observations = os.path.abspath(args.observations)
    args.labels = os.path.abspath(args.labels)
    args.out = os.path.abspath(args.out)


    if args.single:
        print json.dumps(
            learn_label(
                args.labels, args.observations, args.single,
                0, args.folds, transpose=args.transpose,
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

        observation_matrix = pandas.read_csv(args.observations, sep="\t", index_col=0)
        label_matrix = pandas.read_csv(args.labels, sep="\t", index_col=0)

        if args.transpose:
            observation_matrix = observation_matrix.transpose()
            label_matrix = label_matrix.transpose()

        label_set = []
        for l in label_matrix.columns:
            if sum( numpy.ravel(label_matrix[l] != 0) ) > 20:
                label_set.append(l)

        label_rdd = sc.parallelize(list(label_set), len(label_set))
        if args.folds > 0:
            task_rdd = label_rdd.flatMap( lambda x: list( (x,i) for i in range(args.folds) ) )
        else:
            task_rdd = label_rdd.map( lambda x: (x,None) )

        counts = task_rdd.map(
            lambda x: learn_label(
                args.labels, args.observations, x[0], fold=x[1],
                fold_count=args.folds, transpose=args.transpose,
                penalty=args.penalty, C=args.C)
            )

        counts.map( json.dumps ).saveAsTextFile(args.out)
