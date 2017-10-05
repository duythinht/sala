# sala
My Kafka consumer wrapper, based on  sarama-cluster

```
  config := sala.NewConfig("localhost:2020,remotehost:3222", "hello,world", "testgroup")
  config.Decoder = YourDecoder //should declare first
  worker := sala.NewWorker(config)
  worker.Bind(func(message map[string]interface{}) {
    fmt.Println(message)
  })
  worker.Run()
```
