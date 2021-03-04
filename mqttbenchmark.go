package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	uuid "github.com/satori/go.uuid"
)

type mqttClient struct {
	hostname string
	server   string
	username string
	password string
	mode     string
	topic    string
	qos      int
	retained bool

	poolCount int
	wg        sync.WaitGroup

	sendCount    float64
	errorCount   float64
	successCount float64
}

var (
	server    *string
	username  *string
	password  *string
	mode      *string
	topic     *string
	qos       *int
	retained  *bool
	poolCount *int

	successCh = make(chan bool)
	errorCh   = make(chan bool)

	ec = &mqttClient{}
)

func init() {
	// 解析命令行参数
	server = flag.String("server", "tcp://172.18.30.225:1883", "Full URL of the MQTT server")
	username = flag.String("username", "username", "username to authenticate MQTT server")
	password = flag.String("password", "password", "Password to match username")
	mode = flag.String("mode", "pub", "Mode choose: pub or sub")
	topic = flag.String("topic", "opsTopic", "Topic to publish or subscribe")
	qos = flag.Int("qos", 1, "The QoS to send the messages at")
	retained = flag.Bool("retained", false, "Are the messages sent with the retained flag")
	poolCount = flag.Int("pool", 1, "Connect pool size")

	flag.Parse()

	ec.hostname, _ = os.Hostname()
	ec.server = *server
	ec.username = *username
	ec.password = *password
	ec.mode = *mode
	ec.topic = *topic
	ec.qos = *qos
	ec.retained = *retained
	ec.poolCount = *poolCount
}


// mqtt 发送消息
func mqttPubMessage(ec *mqttClient, poolCh chan bool, stopCh chan bool) {
	clientid := strings.Join([]string{ec.hostname, uuid.NewV4().String()}, "-")

	defer func() {
		ec.wg.Done()
		<-poolCh
		//fmt.Printf("Mqtt Pub Message exit: %s\n", clientid)
	}()

	ec.wg.Add(1)
	connOpts := MQTT.NewClientOptions()
	connOpts.AddBroker(ec.server)
	connOpts.SetClientID(clientid)
	connOpts.SetCleanSession(false)
	connOpts.SetUsername(ec.username)
	connOpts.SetPassword(ec.password)
	//connOpts.SetKeepAlive(800 * time.Second)

	c := MQTT.NewClient(connOpts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("Publish Connect Error: %s\n", token.Error())
		errorCh <- true
		return
	}

	// 循环发送消息
	tck := time.NewTicker(time.Second * 1)

	message := "7e00ad44628200ad0675771ac10048008d75771ac10048001d000006ca92f002afd468002aeaee35809400010000000200000000000300810004000000050000060000070000080000000900000a00000b0000000c0f000d00000e01000f0000000000000000001000001100001200001300000014000015000016000000170000184d50555f56312e342e3030360019504d555f56312e322e313820001a303030303030303030303030001b01001c24"
	pubMessage, err := hex.DecodeString(message)
	if err != nil {
		panic(err)
	}

	for true {
		select {
		case <-stopCh:
			return
		case <-tck.C:
			if token := c.Publish(ec.topic, byte(ec.qos), ec.retained, pubMessage); token.Wait() && token.Error() != nil {
				fmt.Printf("Publish Message Error: %s\n", token.Error())
				errorCh <- true
				continue
			}
			successCh <- true
		}
	}
}


func onMessageReceived(_ MQTT.Client, _ MQTT.Message) {
	successCh <- true
	//fmt.Printf("Received message on topic: %s\nMessage: %s\n", message.Topic(), message.Payload())
}

// mqtt 接收消息
func mqttSubMessage(ec *mqttClient, poolCh chan bool, stopCh chan bool) {
	defer func() {
		ec.wg.Done()
		<-poolCh
	}()

	ec.wg.Add(1)
	clientid := strings.Join([]string{ec.hostname, uuid.NewV4().String()}, "-")

	connOpts := MQTT.NewClientOptions()
	connOpts.AddBroker(ec.server)
	connOpts.SetClientID(clientid)
	connOpts.SetCleanSession(false)
	connOpts.SetUsername(ec.username)
	connOpts.SetPassword(ec.password)
	//connOpts.SetKeepAlive(800 * time.Second)

	connOpts.OnConnect = func(c MQTT.Client) {
		if token := c.Subscribe(ec.topic, byte(ec.qos), onMessageReceived); token.Wait() && token.Error() != nil {
			fmt.Printf("Subscribe SubMessage Error: %s\n", token.Error())
			errorCh <- false
			return
		}
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("Subscribe Connect Error: %s\n", token.Error())
		errorCh <- false
		return
	}

	//fmt.Printf("Success Subscribe, Clientid: %s\n", clientid)
	<-stopCh
	//fmt.Printf("Exit Subscribe, Clientid: %s\n", clientid)
}


func output(ec *mqttClient, start time.Time) {
	now := time.Now()
	savg := ec.successCount / now.Sub(start).Seconds()
	eavg := ec.errorCount / now.Sub(start).Seconds()

	fmt.Println("---------------------------------------------------------------------")
	fmt.Printf("SuccessCount: %7.0f SuccessAvg: %7.0f/s\n", ec.successCount, savg)
	if ec.mode == "sub" {
		fmt.Printf("errorCount  : %7.0f ErrorAvg  : %7.0f/s\n", ec.errorCount, eavg)
		return
	}
	fmt.Printf("errorCount  : %7.0f ErrorAvg  : %7.0f/s ErrorPercent: %2.2f%s\n",
		ec.errorCount, eavg, 100*ec.errorCount/(ec.successCount+ec.errorCount), `%`)
}

func collectingAndOutput(ec *mqttClient, start time.Time)  {
	printTck := time.NewTicker(time.Second * 5)

	for true {
		select {
		// 打印收集统计信息
		case <-printTck.C:
			go output(ec, start)
		// 增加成功次数
		case <-successCh:
			ec.successCount++
		// 	增加失败次数
		case <-errorCh:
			ec.errorCount++
		}
	}
}

func CollectingAndOutput(ec *mqttClient, start time.Time, sigs chan os.Signal, stopCh chan bool, collectDoneCh chan bool) {
	go collectingAndOutput(ec, start)

	// 等待接收系统SIGTERM信号
	 <-sigs
	fmt.Println("\n\n\n^ 收到信号SIGTERM, 启动退出流程 .")

	// 通知 发送/接收 Goroutine退出
	close(stopCh)
	//fmt.Println("^ 关闭 stopCh .")

	// 等待 发送/接收 Goroutine退出
	ec.wg.Wait()
	//fmt.Println("^ 所有Goroutine已经全部退出")

	// 通知main函数退出
	collectDoneCh <- true
}


func Benchmark(sigs chan os.Signal, stopCh chan bool, collectDoneCh chan bool, start time.Time) {
	// 初始化控制并发通道
	poolCh := make(chan bool, ec.poolCount)

	// 循环创建发送, 接收goroutine
	switch ec.mode {
	case "sub":
		// 多个客户端共享订阅
		ec.topic = fmt.Sprintf("$share/mqttbenchmark/%s/#", ec.topic)

		go CollectingAndOutput(ec, start, sigs, stopCh, collectDoneCh)

		for true {
			select {
			case _, ok := <-stopCh:
				if !ok {
					fmt.Println("^ 退出 Benchmark ..")
					return
				}
			case poolCh <- true:
				go mqttSubMessage(ec, poolCh, stopCh)
			}
		}
	case "pub":
		go CollectingAndOutput(ec, start, sigs, stopCh, collectDoneCh)

		for true {
			select {
			case <-stopCh:
				fmt.Println("^ 退出 Benchmark ..")
				return
			case poolCh <- true:
				go mqttPubMessage(ec, poolCh, stopCh)
			}
		}
	}
}


func main() {
	sigs := make(chan os.Signal, 1)
	stopCh := make(chan bool, 1)
	collectDoneCh := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	start := time.Now()
	Benchmark(sigs, stopCh, collectDoneCh, start)

	<-collectDoneCh

	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>最终结果<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	output(ec, start)
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> End <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
}
