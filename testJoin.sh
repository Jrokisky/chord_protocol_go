urlprefix="http://localhost:8080/nodes/"
urlsuffix="/join"

n1=$(curl -X POST http://localhost:8080/nodes)
curl -X POST "$urlprefix$n1$urlsuffix"
echo "node 1"

n2=$(curl -X POST http://localhost:8080/nodes)
curl -X POST "$urlprefix$n2$urlsuffix"
echo "node 2"

n3=$(curl -X POST http://localhost:8080/nodes)
curl -X POST "$urlprefix$n3$urlsuffix"
echo "node 3"

curl -X GET http://localhost:8080/nodes

