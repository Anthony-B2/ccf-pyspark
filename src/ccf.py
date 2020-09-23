#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from pyspark import SparkContext

def prepare_dataset(graph):
  graph = graph.filter(lambda x: "#" not in x)\
               .map(lambda x : x.split("\t"))\
               .map(lambda x : (x[0], [x[1]]))
  return graph


def countNewPair(x):
  global newPair
  start = 0
  for value in x[1]:
    if value != x[2]:
      start = start + 1
  newPair += start

  
def Calculate_CCF(graph):
  iteration = 0
  done = False
  while not done:
      startPair = newPair.value
      ccf_iterate_map = graph.union(graph.map(lambda x : (x[1][0], [x[0]])))
      ccf_iterate_reduce_pair = ccf_iterate_map.reduceByKey(lambda x,y : x+y).map(lambda x : (x[0], x[1], min(x[0], min(x[1])))).filter(lambda x: x[0] != x[2])
      ccf_iterate_reduce_pair.foreach(countNewPair) 
      ccf_iterate_reduce = ccf_iterate_reduce_pair.map(lambda x : (x[2], x[1] + [x[0]]))\
                    .flatMapValues(lambda x : x)\
                    .filter(lambda x: x[0] != x[1])\
                    .map(lambda x : (x[0], [x[1]]))
      ccf_dedup_map = ccf_iterate_reduce.map(lambda x : (((x[0], x[1][0]),None)))
      ccf_dedup_reduce = ccf_dedup_map.groupByKey().map(lambda x : (x[0][0], [x[0][1]]))
      ccf_dedup_reduce.persist()
      if startPair == newPair.value:
          done = True
      iteration += 1
      graph = ccf_dedup_reduce
  print("Nombre d'itération : ", iteration)
  return graph

sc = SparkContext.getOrCreate()
dataset = SparkContext.getOrCreate().textFile("/usr/usr335/web-Google.txt", use_unicode="False")
graph = prepare_dataset(dataset)
t1 = time.perf_counter()
newPair = sc.accumulator(0)
graph = Calculate_CCF(graph)
t2 = time.perf_counter()
print("calculation time :", t2 - t1)
