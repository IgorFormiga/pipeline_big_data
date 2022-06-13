#!/bin/bash
# Este shell deve ser executado dentro  do SPARK SERVER

spark-submit \
    --master local[*] \
    --deploy-mode client \
    job_processor.py