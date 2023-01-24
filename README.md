# Streaming Stock Sentiment 


## Description
This project leverages PRAW and yfinance APIs to deliver near real-time stock sentiment analysis from the subreddit r/wallstreetbets. 


## Design Overview 

![Architecture Diagram](/assets/architecture_diagram.svg)

PRAW (Python Reddit API Wrapper) was used to stream live comments from r/wallstreetbets, while yfinance was used to retrieve the latest ticker quotes from Yahoo! Finance. Then, Apache Kafka was used to ingest incoming reddit and stock data under topics **comments** and **stocks**, respectively. For structured streaming, Apache Spark was used, subscribing to the aforementioned topics and processing data to be saved to a datastore. The datastore designated as the final destination for the processed data was Apache Cassandra, chosen for its scalability and availability as well as powerful time series capabilities. Finally, Apache Airflow was used to orchestrate the data pipeline to run at the desired times- between the time of market open and close on weekdays.

### Data Ingestion
PRAW offers streaming functionality, albeit limited, whereas yfinance does not. Thus, new stock quotes from yfinance were continuously retrieved and saved as a csv file to a folder read via stream by Spark. Thereafter, Spark was used to write the stream to Kafka. Contrastly, PRAW's streaming data was directly written to Kafka without the assistance of Spark. 

Following data retrieval from the APIs, the data was first ingested into the pipeline by Kafka. Kafka was accessed through the python kafka wrapper kafka-python. Before running the data pipeline in python, the Kafka broker was activated. For example, as shown in the following commands:

```
zookeeper-server-start.sh zookeeper.properties
```

```
kafka-server-start.sh server.properties
```

Kafka was chosen as the system of record for the pipeline for its ability to provide permanent storage for the streams in a distributed, fault-tolerant manner. Due to its durability and fault tolerance, if other components of the pipeline were to break down due to node fault or network failure, Kafka can be used as the authoritative version of the data and its record can be used to reprocess data if need be. However, as this is a streaming pipeline, the value is arguably in the near real-time reflection of stock sentiment, so cases in which reprocessing would be valuable would be for a historical or batch analysis. Moreover, due to Kafka’s high throughput capabilities, it would also ensure that near real-time streaming can be achieved. 

The Kafka topic **comments** was used to store streaming data from r/wallstreetbets, while the topic **stocks** was used to store streaming ticker quote data from Yahoo Finance. Other components of the pipeline are derived datasets from Kafka.

### Structured Streaming 
Structured Streaming in Spark allows stream processing to be expressed similarly to batch processing. Spark SQL engine treats stream processing incrementally, updating the final result as data arrives. Additionally, dependent on the implementation, Spark can provide end-to-end exactly-once fault tolerance guarantees with the use of checkpoints and WAL. Spark processing uses a micro-batch processing engine, although a Continuous Processing method is available albeit experimental. Moreover, Continuous Processing can only provide up to an at-least-once guarantee compared to the micro-batch processing engine. Consequently, for the purposes of this project, the micro-batching method was dispatched.

#### Stream-Stream Join 
Structured Streaming offers support for both a stream-table join as well as stream-stream join. For the purposes of this project, as there are two streams of data being processed, the stream-stream join was employed.

![Stream Diagram 1](/assets/stream_diagram1.png)
![Stream Diagram 2](/assets/stream_diagram2.png)

##### Watermarking
To conduct a stream-stream join, Watermarking conditions were also defined prior to the join. Due to network or other arbitrary failures, events can potentially arrive to the pipeline past event time in an unbounded manner (in this project's case- events being either a comment from r/wallstreetbets or a quote from the designated tickers). Thus to ensure a complete join without missing any events, all past input must be saved as new inputs may match with it. However, this would cause the system to become unsustainable to support as memory is finite. In the absence of Watermarking configuration, the streaming state would eventually become unbounded. To counteract this, Watermarking conditions are introduced to clear past inputs at a certain threshold to free up memory.

```python
# watermark
tickers = tickers.withWatermark('datetime', '2 hours')
lines = lines.withWatermark('created_est', '1 hours')
```
In this project, considering the throughput of stock quotes were demonstratively faster than reddit comments, a higher Watermarking threshold was used to retain tickers.

##### Streaming Deduplication
The stream-stream join was conducted using the conditions as stated below. The join filtered for comments that contained a given stock ticker in its body, while the quote for that given ticker was chosen within the appropriate time intervals. As stated below, the event time of the ticker quote had to be less than or equal to the event time of the comment, while the difference between the two event times could only equal up to a minute. With the use of Watermarking and the join's dependence on event time, clock issues were mitigated.

```python
lines = lines.join(
                tickers,
                expr(
                    '''
                    ticker = processed AND
                    datetime <= created_est AND
                    datetime >= created_est + INTERVAL -1 MINUTE
                    '''
                )
        )
```

Based on the conditions of the join, duplication of data arose as expected. A comment can join with the ticker quotes with the resulting types of duplications:
Same ticker at different times that satisfy the time condition
Different tickers at the same time
Different tickers at different times

The foremost is what we want to filter out. Desired filtering of duplicated join data was conducted using Watermarking from the previous step as well as using the built in function dropDuplicates as shown below. This will ensure that a given comment will only join with one stock quote of the same ticker. 

```python
# drop duplicates
lines = lines \
        .dropDuplicates([
            'id', 
            'author',
            'created_est', 
            'score', 
            'permalink', 
            'body', 
            'final', 
            'ticker'
    ])
```   

#### Sentiment Analysis
##### Pre-Processing
Following the stream-stream join, preprocessing was performed on the comments using native-spark functions. Handlers, URLs, special characters were filtered out, followed by tokenization to prepare for sentiment analysis. 

##### NLTK
Natural Language Toolkit (NLTK), a platform for language processing, was used to perform sentiment analysis on the comments. Through the use of NLTK's pretrained sentiment analyzer, Valence Aware Dictionary and Sentiment Reasoner (VADER), polarity scores were calculated for each of the comments. VADER is most appropriate for determining sentiment of language prevalent in social media usage, trained for short sentences with slang and abbreviations. The compound value retrieved from VADER ranges from -1 to 1, with -1 being a negative sentiment and 1 being a positive sentiment. 

##### UDF
To implement VADER to the streaming dataset in Spark, a User-Defined Function (UDF) was used. UDF is a custom function that can be called similarly to built-in functions in Spark, although there is a stark difference in performance between the two. In order to minimize the performance impact of a UDF, sentiment analysis was left at the end after filtering as well as the stream-join. Furthermore, the use of a UDF causes a common error in MacOS due to adjusted fork behavior. This was mitigated locally using the following command.

```
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES 
```

### Datastore
Apache Cassandra is a NoSQL distributed database with a data model ideal for time series. Due to the fact that data is sorted and written sequentially, disk seeks are minimized and data retrieval is efficient. Cassandra also offers time series capabilities for a given table at its initiation, with the command CLUSTERING. Although at this time, a time series for the project has not been completed, it is a roadmap to-do list and was considered at the time of choosing the final destination for the data. 

![Datastore Diagram](/assets/cassandra.png)

### Orchestration
Lastly, Apache Airflow was used to orchestrate the pipeline to run at a daily cadence during the weekdays from 9:30AM - 4:00PM EST. To carefully account for Airflow scheduler being designed as a directed graph and stream processing being continuous (although this project uses stream processing, it is only necessary to run at certain times), a stop clause was built into the pipeline to stop it automatically at market close- 4:00PM, while the 9:30AM start date on the weekday is executed by Airflow.


## Roadmap

### Time Series and Visualization
The next step for this project would be to perform the time series analysis with Cassandra and to visualize it. The end goal is to show the top trending stocks of the hour, day, week and month along with their reflected sentiment on r/wallstreetbets. 

### Deduplication
A challenge that arose with deduplication using Spark was the fact that the built-in function dropDuplicates does not offer a way to choose which duplicate to keep in streaming datasets. Ideally, the join would be best suited between a stock and comment event in which the time elapsed between the two is as minimized as possible. By ensuring this, it would help improve the quality of the data produced by the pipeline.

#### System Design and Scaling
##### Replication
Replication is increasingly important as a system scales. It ensures availability, low latency and allows disconnected operation. The ideal number of replicates for given data should be adjusted for the system. 

Cassandra employs a leaderless replication strategy, using last write wins to settle concurrent writes, a strict quorum to determine successful writes and reads, as well as hinted handoffs for when nodes are down. While last write wins strategy ensures a solution for concurrent writes in which the latest write is retained, it may lead to potential data loss. This is important to consider as the system scales. 

Furthermore, beyond settling concurrent writes and implementing the application in such a way to minimize data loss, replication lag is also important to consider with a distributed system. Replication lag can cause the disruption of read-after-write consistency, monotonic reads and consistent prefix reads. If a given user is accessing the data in an “old” replication of Cassandra, it would be confusing to see data "go back in time" by seeing an up-to-date replication or see data that is not causally supported.

##### Partitioning
Partitioning also becomes necessary as the data increases in a given system, becoming unfeasible for a single machine to store and process it. To distribute the data evenly and avoid hot spots, an appropriate partitioning scheme must be implemented. 

Cassandra employs consistent hash partitioning to partition data among nodes using a primary key. For this pipeline, a hybrid approach would make for an appropriate choice, with the use of a compound key. In the project's current implementation, the compound key is made up of the ticker, year, month, day and hour, with the ticker being the primary composite key while the others make up the clustering keys. The primary composite key is used to determine the partition key, while the clustering keys are used for sorting within a given partition. In future updates, this composite key should be changed, as using the ticker as the primary composite key may lead to hot spots as certain stocks are more popularly discussed than others.


## Getting Started
The data pipeline was built using python packages and wrappers for external services. 

### Dependencies
- python3.10 
- Apache Kafka
- Apache Spark
- Apache Cassandra
- Apache Airflow

In addition, to install all the required python packages to run the pipeline, run the following command.
```
pip install -r requirements.txt
```

### Installation
```
git clone git@github.com:monicahzchung/stock_sentiment.git