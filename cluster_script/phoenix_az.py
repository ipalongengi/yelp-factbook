import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

def extract_review(pid, lines):
    import csv
    if pid == 0:
        lines.next()

    reader = csv.reader(lines)
    for row in reader:
        if len(row) == 9:
            yield row

def extract_biz(pid, lines):
    import csv

    if pid == 0:
        lines.next()

    reader = csv.reader(lines)
    for row in reader:
        yield row

if __name__ == '__main__':
    REVIEW_FN = 'user/ipalong00/yelp_reviews/yelp_review.csv'
    BIZ_FN = 'user/ipalong00/yelp_reviews/yelp_business.csv'

    sc = SparkContext()

    reviews_rdd = sc.textFile(REVIEW_FN, use_unicode=False) \
                    .mapPartitionsWithIndex(extract_review)
    biz_rdd = sc.textFile(BIZ_FN, use_unicode = False) \
                .mapPartitionsWithIndex(extract_biz)

    from operator import add
    biz_rating_total = reviews_rdd \
                        .map(lambda x: (x[2], int(x[3]))) \
                        .map(lambda x: (x[0], 1)) \
                        .reduceByKey(add) \
                        .sortBy(lambda x: x[1], ascending=False)

    city_biz_rdd = biz_rdd \
                    .filter(lambda x: x[4] == 'Phoenix') \
                    .map(lambda x: (x[0], x[1]))

    city_biz_rating_rdd = city_biz_rdd \
                            .join(biz_rating_total) \
                            .sortBy(lambda x: x[1][1], ascending=False) \
                            .map(lambda x: x[1])

    city_biz_rating_rdd.saveAsTextFile('phoenix_biz_rating_overall')

    biz_rating_bottom = reviews_rdd \
                        .map(lambda x: (x[2], int(x[3]))) \
                        .filter(lambda x: x[1] < 3) \
                        .map(lambda x: (x[0], 1)) \
                        .reduceByKey(add) \
                        .sortBy(lambda x: x[1], ascending=False)

    city_biz_bottom = city_biz_rdd \
                        .join(biz_rating_bottom) \
                        .sortBy(lambda x: x[1][1], ascending=False) \
                        .map(lambda x: x[1])

    city_biz_bottom.saveAsTextFile('phoenix_biz_bottom')

    biz_rating_top = reviews_rdd \
                        .map(lambda x: (x[2], int(x[3]))) \
                        .filter(lambda x: x[1] >= 3) \
                        .map(lambda x: (x[0], 1)) \
                        .reduceByKey(add) \
                        .sortBy(lambda x: x[1], ascending=False)

    city_biz_top = city_biz_rdd \
                        .join(biz_rating_top) \
                        .sortBy(lambda x: x[1][1], ascending=False) \
                        .map(lambda x: x[1])

    city_biz_top.saveAsTextFile('phoenix_biz_top')
