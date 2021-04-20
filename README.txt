# PageRank
perfomed on carrier data to find most popular airport
Implemented the algorithm from:
   https://lintool.github.io/MapReduceAlgorithms/MapReduce-book-final.pdf
   Page: 98 CHAPTER 5. GRAPH ALGORITHMS
------------------------
Input args for PageRank: InputFile numIterations OutputDir

execution on aws emr:
spark-submit --deploy-mode cluster --class PageRank /path/to/.jar /path/to/224142547_T_T100D_SEGMENT_US_CARRIER_ONLY.csv 50 /path/to/outputDir

--------***-----------

# Tweets
Sentiment analysis on Tweets using Spark mllib logistic regression, perfomed on Tweets data
-----------------------
Input args for Tweets: InputFile OutputDir

execution on aws emr:
spark-submit --deploy-mode cluster --class Tweets /path/to/.jar /path/to/Tweets.csv /path/to/outputDir


