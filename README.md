
# 接收
./mqttbenchmark -mode sub -pool 100 -qos 1 -server 172.18.30.225:1883
# 推送
./mqttbenchmark -mode pub -pool 20000 -qos 1 -server 172.18.30.225:1883
