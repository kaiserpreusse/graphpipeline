#!/usr/bin/env bash

docker run -d -p 9474:7474 -p 9473:7473 -p 9687:7687 -e NEO4J_AUTH=neo4j/test --name neo4j_graphio_test_35 neo4j:3.5
docker run -d -p 8474:7474 -p 8473:7473 -p 8687:7687 -e NEO4J_AUTH=neo4j/test --name neo4j_graphio_test_41 neo4j:4.1
docker run -d -p 8474:7474 -p 10473:7473 -p 10687:7687 -e NEO4J_AUTH=neo4j/test --name neo4j_graphio_test_42 neo4j:4.2
python -m pytest
docker stop neo4j_graphio_test_35
docker stop neo4j_graphio_test_41
docker stop neo4j_graphio_test_42
docker rm neo4j_graphio_test_35
docker rm neo4j_graphio_test_41
docker rm neo4j_graphio_test_42