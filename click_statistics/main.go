package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)


var dataChan chan []interface {}
var chanlen int = 2000000  //CLICK_ST_DATA_BUFFER_LEN
var mongodbconfig string = "localhost:27017"  //CLICK_ST_MONGODB_URL
var httplisten string = ":84"  //CLICK_ST_SERVER_LISTEN
var databaseconfig string = "mygood"  //CLICK_ST_MONGODB_DB
var collectiontableconfig string = "mytest"  //CLICK_ST_MONGODB_TABLE


func clickst (c *gin.Context){

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("exception : %s\n", r)
		}
	}()

	datalist := c.PostForm("data_list")
	isgz := c.PostForm("gzip")

	//datalist = "H4sIAAAAAAAA/5SQQU/zMAyG/4vzHavpC1tpl9vYhjhwhQtGVWitNSJLrMSsQlP/O0olxBWOfl/7eSS/XEE+mcCAJNu/QwWcIlMSRxnMFf559wYGeGSolqG7UMouBjCgV/r/qim5y52PJxc6N4CR9EFzBeLOBavrpm6267bV63YLFQwuiwu9LKuwu9sfjvf6Zr2pb5ulX3y/9Zb8TDLGwurjQN/hQGKdLwJmxAcRRtzHICl6Tykj7tghPmvEgxX70ygllEWpo0H0NtlTDIjTNCFGpmDLkf0bccUjK1VvYK6ALhQEDDxlSo/lXzC/fgEAAP//AQAA///+E178gQEAAA=="
	//isgz = "1"

	var jsonbyte []byte

	if isgz == "1" {
		decodeBytes, _ := base64.StdEncoding.DecodeString(datalist)
		in := bytes.NewBuffer(decodeBytes)
		//var out bytes.Buffer
		ungz, err := gzip.NewReader(in)
		if err==nil {
			undatas, err1 := ioutil.ReadAll(ungz)
			if err1 == nil {
				//fmt.Println("ungzip size:", len(undatas))
				//fmt.Println(string(undatas))
				jsonbyte = undatas
			}
		}
	} else {
		jsonbyte = []byte(datalist)
	}

	//fmt.Println("----------->",string(jsonbyte))

	var indata []interface {}
	err2 := json.Unmarshal(jsonbyte, &indata)

	//fmt.Println("=======------>",err2,indata)

	if err2==nil {
		select {
		case dataChan <- indata:
			//do sth
		case <- time.After(5*time.Microsecond):
			//to sth
		}
	}

	//压缩
	//oristr := c.PostForm("ori")
	//var b bytes.Buffer
	//gz := gzip.NewWriter(&b)
	//if _, err := gz.Write([]byte(oristr)); err != nil {
	//	panic(err)
	//}
	//if err := gz.Flush(); err != nil {
	//	panic(err)
	//}
	//if err := gz.Close(); err != nil {
	//	panic(err)
	//}
	//encodeString := base64.StdEncoding.EncodeToString(b.Bytes())
	//fmt.Printf("====> %s \n",encodeString)

	//
	//select {
	//case dataChan <- test:
	//	//do sth
	//case <- time.After(5*time.Microsecond):
	//	//to sth
	//}
	//fmt.Println("clickst ")
	//fmt.Println("=============>",len(dataChan))

	c.String(http.StatusOK,"ok")
}

func stat (c *gin.Context){

	numofchan := len(dataChan)
	str := fmt.Sprintf("num fo chan : %d",numofchan)
	//fmt.Println(str)
	c.String( http.StatusOK, str )
}

func main()  {

	if cset:=os.Getenv("CLICK_ST_SERVER_LISTEN"); cset!="" {
		httplisten = cset
	}
	if cset:=os.Getenv("CLICK_ST_MONGODB_URL"); cset!="" {
		mongodbconfig = cset
	}
	mongodbconfig = fmt.Sprintf( "%s%s", "mongodb://",mongodbconfig)
	if cset:=os.Getenv("CLICK_ST_DATA_BUFFER_LEN"); cset!="" {
		cset1,err := strconv.Atoi(cset)
		if err==nil {
			chanlen = cset1
		}
	}
	if cset:=os.Getenv("CLICK_ST_MONGODB_DB"); cset!="" {
		databaseconfig = cset
	}
	if cset:=os.Getenv("CLICK_ST_MONGODB_TABLE"); cset!="" {
		collectiontableconfig = cset
	}


	gin.SetMode(gin.ReleaseMode)

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("exception : %s\n", r)
		}
	}()

	dataChan = make(chan []interface {},chanlen)

	go wmongo()

	r := gin.Default()
	r.GET("/favicon.ico",noop )//注册接口
	r.Any("/clickst",clickst )//注册接口
	r.Any("/stat",stat )//注册接口
	r.Run(httplisten) // listen and serve on 0.0.0.0:8080
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

		for {
			data := <-dataChan  //读chan


			now  := time.Now()
			collectionstr := fmt.Sprintf("%s-%d-%d-%d",collectiontableconfig,now.Year(),now.Month(),now.Day())
			//collectionstr := fmt.Sprintf("%s-%d-%d-%d-%d-%d-%d",collectiontableconfig,now.Year(),now.Month(),now.Day(),now.Hour(),now.Minute(),now.Second())
			//var insetRest *mongo.InsertOneResult
			collection := client.Database(databaseconfig).Collection(collectionstr)
			//end 连接mongodb

			// if insetRest,err =
			if _, err = collection.InsertMany(context.TODO(), data); err != nil{
				fmt.Println("collection.InsertOne err:",err)
				time.Sleep(1*time.Second)
				continue
			}
			//id := insetRest.InsertedID
			//fmt.Println("insert id :", id)
		}
	}

}





func  noop(c *gin.Context)  {
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