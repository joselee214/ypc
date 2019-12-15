package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
	"time"
)




func  noop(c *gin.Context)  {
}


var dataChan chan InsertData
var chanlen int = 1000000
var mongodbconfig string = "mongodb://localhost:27017"
var databaseconfig string = "mygood"
var collectiontableconfig string = "mytest"

type InsertData struct {
	Name string
	Age  int
	City string
}


func clickst (c *gin.Context){
	test := InsertData{"test",10,c.Query("test") }
	select {
	case dataChan <- test:
		//do sth
	case <- time.After(100*time.Microsecond):
		//to sth
	}
	fmt.Println("clickst ")
	fmt.Println("=============>",len(dataChan))

	c.String(http.StatusOK,"ok")
}

func stat (c *gin.Context){
	numofchan := len(dataChan)
	str := fmt.Sprintf("num fo chan : %d",numofchan)
	//fmt.Println(str)
	c.String( http.StatusOK, str )
}

func main()  {

	gin.SetMode(gin.ReleaseMode)

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("捕获到的错误：%s\n", r)
		}
	}()

	dataChan = make(chan InsertData,chanlen)

	go wmongo()

	r := gin.Default()
	r.GET("/favicon.ico",noop )//注册接口
	r.Any("/clickst",clickst )//注册接口
	r.Any("/stat",stat )//注册接口
	r.Run(":84") // listen and serve on 0.0.0.0:8080
}

func wmongo() {

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("exception recover：%s\n", r)
		}
	}()

	//连接mongodb
	for {
		fmt.Println("connect to mongodb ============>")
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongodbconfig))
		if err != nil {
			fmt.Println("mongo.Connect err",err)
			time.Sleep(1*time.Second)
			continue
		}
		// Check the connection
		err = client.Ping(context.TODO(), nil)
		if err != nil {
			fmt.Println("client.Ping err:",err)
			time.Sleep(1*time.Second)
			continue
		}

		//var insetRest *mongo.InsertOneResult
		collection := client.Database(databaseconfig).Collection(collectiontableconfig)
		//end 连接mongodb

		for {
			data := <-dataChan  //读chan
			// if insetRest,err =
			if _, err = collection.InsertOne(context.TODO(), &data); err != nil{
				fmt.Println("collection.InsertOne err:",err)
				time.Sleep(1*time.Second)
				break
			}
			//id := insetRest.InsertedID
			//fmt.Println("insert id :", id)
		}
	}

}



func getApi (c *gin.Context){

	//allget := c.Request.URL.Query()
	//for k,v := range allget {
	//	fmt.Println("kkk:",k)
	//	fmt.Println("vvv:",v)
	//}

	//c.Request.ParseForm() //x-www-form-urlencoded
	//for k,v := range c.Request.PostForm{
	//	fmt.Println("kkk:",k)
	//	fmt.Println("vvv:",v)
	//}
	//
	//data, _ := ioutil.ReadAll(c.Request.Body)
	//fmt.Printf("ctx.Request.body: %v", string(data))
	//
	//fmt.Println("requestMethod:",c.Request.Method)
	//path , _ := c.Params.Get("requestPath")
	//fmt.Println("path:",path)
	//
	//fmt.Println("get:",c.Query("pget")) //get传参数
	//rowdata , _ := c.GetRawData()
	//fmt.Println("form rowdata:",string(rowdata[:]) ) //form-data x-www-form-urlencoded
	//fmt.Println("header:",c.GetHeader("pheader")) //header
	//fmt.Println("form post:",c.PostForm("ppost")) //x-www-form-urlencoded
	//fmt.Println("form data:",c.PostForm("pformdata")) //form-data


	fmt.Println("")
	fmt.Println("")
	fmt.Println("=============>")

	c.String(http.StatusOK,"ok")
}