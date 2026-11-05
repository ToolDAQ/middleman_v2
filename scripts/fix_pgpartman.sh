#!/bin/bash
# update create_time_partition function to fix subpartitioning
psql -f fix_sub_partitioning.sql
