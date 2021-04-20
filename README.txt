# PageRank
perfomed on the data: 
------------------------
Input args for PageRank: InputFile numIterations OutputDir

execution on aws emr:
spark-submit --deploy-mode cluster --class PageRank /path/to/.jar /path/to/224142547_T_T100D_SEGMENT_US_CARRIER_ONLY.csv 50 /path/to/outputDir

Input args for PageRank: InputFile OutputDir

execution on aws emr:
spark-submit --deploy-mode cluster --class Tweets /path/to/.jar /path/to/Tweets.csv /path/to/outputDir


