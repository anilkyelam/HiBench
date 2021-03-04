#!/bin/bash

#
# Prepares HiBench application for running on Spark
# Calls on HiBench prepare to generate input files
#

# Parse command line arguments
for i in "$@"
do
case $i in
    -r | --rebuild)
    REBUILD=1
    ;;

    -wt=*|--wtype=*)      # Workload type: dal, graph, micro, ml, sql, streaming, websearch
    WTYPE="${i#*=}"
    ;;

    -wn=*|--wname=*)      # Workload name. Varies by wtype; for e.g., pagerank, nweight for graph type
    WNAME="${i#*=}"
    ;;

    -h | --help)
    echo -e $usage
    exit
    ;;

    *)                  # unknown option
    echo "Unkown Option: $i"
    echo -e $usage
    exit
    ;;
esac
done

dir=$(dirname "$0")
ls ${dir}/bin/workloads/ | grep -e "^${WTYPE}$" &> /dev/null
if [[ $? -ne 0 ]]; then 
    echo "Specify valid spark workload type with -wt=. Valid values are:" `ls $dir/bin/workloads`
    exit 1
fi

ls ${dir}/bin/workloads/${WTYPE}/ | grep -e "^${WNAME}$" &> /dev/null
if [[ $? -ne 0 ]]; then 
    echo "Specify valid spark workload name with -wn=. Valid values for ${WTYPE} type are:" `ls $dir/bin/workloads/${WTYPE}`
    exit 1
fi

# Check hdfs is running
hdfs dfsadmin -report  &> /dev/null
if [[ $? -ne 0 ]]; then
    echo "ERROR! HDFS cluster not running or in a bad state. Exiting!"
    exit -1
fi

# Rebuild workload type
if [[ $REBUILD ]]; then
    pushd "$dir"
    mvn -Psparkbench -Dmodules -P${WTYPE} -Dspark=3.1 -Dscala=2.12 clean package -nsu
    popd
fi

# Prepare input
echo "Preparing input for ${WTYPE}/${WNAME}"
bash ${dir}/bin/workloads/${WTYPE}/${WNAME}/prepare/prepare.sh

# TODO: Cache hibench files?
# Adding file to hdfs cache
# if [ "${CACHE_HDFS_FILE}" == 1 ]; then
#     echo "Checking if the hdfs file is cached"
#     hdfs cacheadmin -listDirectives | grep "${FILE_PATH_HDFS}"
#     if [ $? != 0 ]; then
#             echo "File not in cache, refreshing the cache and adding the file"
#             hdfs cacheadmin -removeDirectives -path "/user/ayelam"
#             hdfs cacheadmin -addPool cache-pool
#             hdfs cacheadmin -addDirective -path "${FILE_PATH_HDFS}" -pool cache-pool
            
#             # This initiates caching process. Ideally we would want to wait and check until it finishes, but for now
#             # we just wait and hope that it's done. (TODO: Configure this time for larger inputs)
#             sleep 300

#             hdfs cacheadmin -listDirectives
#     else
#             echo "File is cached, moving on."
#     fi
# fi
