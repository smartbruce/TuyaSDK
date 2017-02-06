# Cloud-SDK, 涂鸦云对云API java-sdk


## 项目简介
Cloud-SDK是涂鸦云对云API的java封装, 对http方式提供的接口进行了简单的封装,包括
签名, 基础参数设置等. 方便使用java的客户进行接口调用.

API列表见: [Tuya Open API](http://api.tuya.com/)

http方式调用指南: [Tuya docs](http://docs.tuya.com/develop/cloudapi/api/)

SDK下载: [Cloud-SDK](https://github.com/TuyaInc/TuyaDemo/releases/download/1.0-SNAPSHOT/cloud-sdk-1.0-SNAPSHOT.jar)

## SDK使用

```java

    //创建 cloud client(accessId, accessKey由tuya这边提供)
    String endUri = "https://a1.tuyacn.com/api.json";
    String accessId = "xxxx";
    String accessKey = "xxxxxxxxxxx";
    TuyaCloudClient client = new TuyaCloudClient(accessId,accessKey, endUri);

    //新建请求对象
    RequestMessage request = new RequestMessage();
    request.setApi("tuya.m.user.uid.register");
    request.setApiVersion("1.0");
    request.setOs("centOS-6");
    request.setDeviceid("ppstrong");
    request.setLang("zh");

    //注:除注册及获取统计数据等少量接口,大部份接口都需要sessionId
    //可以从注册和登录接口返回结果得到,返回结果字段为sid
    request.setSession("asdfasdf");

    //如果接口需要入参,则通过 hashmap设置
    Map<String,String> params = new HashMap<String,String>();
    params.put("uid","mmmmmmmmmm");
    params.put("countryCode","86");
    params.put("passwd","faint");
    request.setParams(params);


    //发送请求, 得到响应
    //如果是成功请求, 则response里的result会是个JSON对象封装的请求结果.
    //如果失败, 请查看errorMsg和errorCode,进行相应的处理.
    ResponseMessage response=client.sendRequest(request);
    if (response.isSuccess()) {
        String result = JSONObject.toJSONString( response.getResult(),true);
        System.out.println(result);
    } else {
        String errorCode = response.getErrorCode();
        String errorMsg = response.getErrorMsg();
        System.out.println(errorMsg);
    }


```

网络连接超时可以根据服务器情况进行设置

```java

    //不设置的话,默认为50秒. 
    ClientConfig clientConfig=new ClientConfig();
    clientConfig.setSocketTimeout(2000);
    clientConfig.setConnectionTimeout(2000);

    TuyaCloudClient client = new TuyaCloudClient(accessId,accessKey, endUri, clientConfig);

```



注册接口成功返回的response.getResult()对象.
```json
{
	"uid":"ay1466135026676FK2vo",
	"sid":"ay14661305026676FfK2voB3f29196a14982b7b013b13163db55767f",
	"username":"mmmmmmmmmm",
	"sex":0,
	"ecode":"y5y611y4a6a11131",
	"phoneCode":"86",
	"nickname":"",
	"partnerIdentity":"p1000019",
	"snsNickname":"",
	"domain":{
		"regionCode":"AY",
		"gwApiUrl":"http://a.gw.tuyacn.com/gw.json",
		"gwMqttUrl":"mq.gw.tuyacn.com",
		"mobileMqttUrl":"mq.mb.tuyacn.com",
		"mobileApiUrl":"https://a1.tuyacn.com"
	},
	"headPic":"",
	"userType":1
}
```

注册接口返回的uid,sid,ecode,domain等信息在后续请求中还会用到,请妥善保存.
