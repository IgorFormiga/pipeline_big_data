#!/bin/bash
# Este shell deve ser executado dentro  do SPARK SERVER

spark-submit \
    --master local[*] \
    --deploy-mode client \
    jobs_processor.py