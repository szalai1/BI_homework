docker run --name zookeeper -d szalai/zookeeper
docker run --link zookeeper:zookeeper -d --name nimbus szalai1/nimbus
docker run --link nimbus:master -p 8080:8080 -d  szalai1/ui
docker run --link nimbus:master -d szalai1/supervisor
