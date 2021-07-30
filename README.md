
# 节点区块同步程序satoblock

分析完整的区块数据，解析所有tx信息，并提取sensible相关数据到数据库/redis。为Sensible API Service提供数据。

通过监听节点zmq，获取新区块确认通知。

## 运行依赖

1. 需要节点开启zmq服务，至少启用hashblock队列。
2. 能够直接访问节点磁盘block文件。
3. 使用redis，clickhouse存放数据。目前redis占用15GB，clickhouse占用930GB。


## 配置文件

在conf目录有程序运行需要的多个配置文件。

* db.yaml

clickhouse数据库配置，主要包括address、database等。

* chain.yaml

节点配置，主要包括zmq地址、blocks文件路径。

* redis.yaml

redis配置，主要包括address、database等。

需要占用2个database号，database_block存放UTXO原始script，database存放UTXO集合key。需要和satomempool配置保持一致。

## Docker

使用docker-compose可以比较方便运行satoblock。首先设置好db/redis/node配置，然后运行初始化：

	$ docker-compose -f docker-compose-init.yaml up -d

当运行完毕之后(>6h)，可运行正常同步：

	$ docker-compose up -d

注意不要用docker-compose stop，因为停止超时(10s)会强制杀进程。要优雅停止请执行：

	$ docker-compose kill -s SIGINT


## 运行逻辑

首次运行前，需要先在clickhouse中创建db，无需创建table。

由于一次性全量同步将占用大量内存(>100GB)，以至于无法在普通机器成功执行。我们可采用分批执行、分段同步所有区块。

开始同步命令如下，表示执行初始同步，并在区块高度为100000时停止：

    $ ./satoblock -full -end 100000

当执行完毕后，可以进行批次同步，每批1000个块，如果内存较小，可适当减少每次同步的区块数量。在区块高度为690000时停止：

    $ ./satoblock -end 690000 -batch=1000

最后，可以启动连续同步模式，即同步完最近区块后不退出，将继续监听并同步新区块：

    $ ./satoblock

程序日志将直接输出到终端，可使用nohup或其他技术将程序放置到后台运行。

satoblock服务在等待新区块到来时可以重启，同步过程中不可随意重启(停止需要发送`SIGINT`触发)。
