from pyspark import SparkContext

def extract_review(pid, record):
    import csv

    if pid == 0:
        record.next()

    reader = csv.reader(record)
    for row in reader:
        if len(row) == 9:
            yield row


def extract_biz(pid, record):
    import csv

    if pid == 0:
        record.next()

    reader = csv.reader(record)
    for row in reader:
        yield row


if __name__ == '__main__':
    REVIEW_FN = 'user/ipalong00/yelp_reviews/yelp_review.csv'
    BIZ_FN = 'user/ipalong00/yelp_reviews/yelp_business.csv'

    sc = SparkContext()
    cities = ['Phoenix', 'Las Vegas', 'Toronto', 'Charlotte', 'Stuttgart', 'Edinburgh']

    # Parse the reviews rdd
    review_rdd = sc.textFile(REVIEW_FN, use_unicode=False) \
                   .mapPartitionsWithIndex(extract_review) \
                   .cache()

    # Parse the businesses rdd
    biz_rdd = sc.textFile(BIZ_FN, use_unicode=False) \
                .mapPartitionsWithIndex(extract_biz) \
                .cache()


    def find_stuff(city, biz_rdd, review_rdd):
        from operator import add

        # Create a new RDD containing only businesses from the specified city
        city_biz_rdd = biz_rdd.filter(lambda x: x[4] == city)

        ## What are the most common business categories in this city?
        top_biz_cat = city_biz_rdd.map(lambda x: (x[0], x[12].split(';'))) \
                                     .flatMapValues(lambda x: x) \
                                     .map(lambda x: (x[1], 1)) \
                                     .reduceByKey(add) \
                                     .sortBy(lambda x: x[1], ascending=False)

        top_biz_cat.saveAsTextFile('%s_top_biz_cat' % city)

        ## What are business establishments in this city with the most reviews,
        ## regardless of their business categories?
        city_biz_names = city_biz_rdd.map(lambda x: (x[0], x[1]))

        biz_rat_total = review_rdd.map(lambda x: (x[2], 1)) \
                                  .reduceByKey(add) 
        biz_rat_total = city_biz_names.join(biz_rat_total) \
                                      .map(lambda x: (x[1][0], x[1][1])) \
                                      .sortBy(lambda x: x[1], ascending=False)

        biz_rat_total.saveAsTextFile('%s_top_biz_review_total' % city)

        ## What are the business establishments in this city with the most positive reviews,
        ## regardless of their business categories?
        biz_rat_top = review_rdd.map(lambda x: (x[2], int(x[3]))) \
                                 .filter(lambda x: x[1] >= 3) \
                                 .map(lambda x: (x[0], 1)) \
                                 .reduceByKey(add) \
                                 .sortBy(lambda x: x[1][1], ascending=False)

        city_biz_top = city_biz_rdd.join(biz_rat_top) \
                              .sortBy(lambda x: x[1][1], ascending=False) \
                              .map(lambda x: x[1])

        city_biz_top.saveAsTextFile('%s_biz_top_rating' % city)

        ## What are the business establishments in this city with the most negative reviews,
        ## regardless of their business categories?
        biz_rat_bottom = review_rdd.map(lambda x: (x[2], int(x[3]))) \
                                 .filter(lambda x: x[1] < 3) \
                                 .map(lambda x: (x[0], 1)) \
                                 .reduceByKey(add) \
                                 .sortBy(lambda x: x[1][1], ascending=False)

        city_biz_bottom = city_biz_rdd.join(biz_rat_bottom) \
                                .sortBy(lambda x: x[1][1], ascending=False) \
                                .map(lambda x: x[1])

        city_biz_bottom.saveAsTextFile('%s_biz_bottom_rating' % city)


    for city in cities:
        find_stuff(city, biz_rdd, review_rdd)
