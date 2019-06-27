生日，名称，签名，等级，大会员状态 = "https://api.bilibili.com/x/space/acc/info?mid={uid}"
收藏夹状态 = "https://api.bilibili.com/medialist/gateway/base/created?pn=1&ps=10&up_mid={uid}"
关注数，粉丝数 = "https://api.bilibili.com/x/relation/stat?vmid={uid}"
直播间信息 = "https://api.live.bilibili.com/room/v1/Room/getRoomInfoOld?mid=3036487"
投稿信息 =  "https://api.bilibili.com/x/space/navnum?mid=122879"
总计播放量 = "https://api.bilibili.com/x/space/upstat?mid=2"

视频信息 = "https://api.bilibili.com/x/web-interface/archive/stat?aid={aid}"
通过av号获取作者 = "https://api.bilibili.com/x/tag/archive/tags?aid={aid}"

_defult_item = {"uid": -1,
                "user_name" : "None",       #用户名陈
                "user_sign" : "",           #用户签名
                "level" : -1,               #用户等级
                "video" : -1,               #投稿视频
                "audio" : -1,               #音频
                "article" : -1,            #专栏
                "album" : -1,               #相册
                "follow" : 0,             #关注
                "coins" : -1,               #硬币
                "vip" : -1,
                "fans" : 0,               #粉丝
                "play_count" : 0,        #总播放量
                "read_count" : 0,
                "live_title" : "无",
                "favorite_list": -1 ,       #收藏夹数量
                "favorite_sum" : -1,        #总计收藏视频
                "birthday" : "01-01",       #生日
                "gender" : "保密",          #性别
                "time" : "1970-01-01 00:00:00",}          #数据获取时间