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


def find_city_biz_stats(city, biz_rdd, review_rdd):
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

    # Reduce the number of fields in city_biz_rdd to only include the business_id
    # and the name of the business establishment
    city_biz_names = city_biz_rdd.map(lambda x: (x[0], x[1]))

    ## What are the business establishments in this city with the most reviews,
    ## regardless of their business categories?
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
                             .sortBy(lambda x: x[1], ascending=False)

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
                             .sortBy(lambda x: x[1], ascending=False)

    city_biz_bottom = city_biz_rdd.join(biz_rat_bottom) \
                            .sortBy(lambda x: x[1][1], ascending=False) \
                            .map(lambda x: x[1])

    city_biz_bottom.saveAsTextFile('%s_biz_bottom_rating' % city)


def find_overall_review_stats(review_rdd):
    from operator import add
    
    ## What is the average number of words in reviews with low ratings?
    ## (i.e. less than 3 stars)
    avg_wc_low = review_rdd.filter(lambda x: int(x[3]) < 3) \
                           .map(lambda x: (x[0], x[5].split(' '))) \
                           .mapValues(lambda x: len(x)) \
                           .map(lambda x: int(x[1]))

    total_rec_wc_low = avg_wc_low.reduce(add)
    print "The avg. number of words for reviews with low ratings is: %f\n" \
            % (total_rec_wc_low / avg_wc_low.count())

    ## What is the average number of words in reviews with high ratings?
    ## (i.e. greater than or equal to 3 stars)
    avg_wc_high = review_rdd.filter(lambda x: int(x[3]) >= 3) \
                            .map(lambda x: (x[0], x[5].split(' '))) \
                            .mapValues(lambda x: len(x)) \
                            .map(lambda x: int(x[1]))

    total_rec_wc_high = avg_wc_high.reduce(add)
    print "The avg. number of words for reviews with high ratings is: %f\n" \
            % (total_rec_wc_high/ avg_wc_high.count())

    # Find the frequency of words in all of the reviews
    word_freq_reviews = review_rdd.flatMap(lambda x: x[5].split(' ')) \
                               .map(lambda x: (x, 1)) \
                               .reduceByKey(add) \
                               .sortBy(lambda row: row[1], ascending=False)

    word_freq_reviews.saveAsTextFile('yelp_most_used_words')


def find_overall_biz_stats(biz_rdd):
    from operator import add

    ## What are the most common business categories in this dataset?
    top_business_rdd = biz_rdd.flatMap(lambda x: x[12].split(';')) \
                              .map(lambda x: (x, 1)) \
                              .reduceByKey(add) \
                              .sortBy(lambda x: x[1], ascending=False)

    top_business_rdd.saveAsTextFile('yelp_top_biz_cat')


if __name__ == '__main__':
    REVIEW_FN = './yelp_reviews/yelp_review.csv'
    BIZ_FN = './yelp_reviews/yelp_business.csv'

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

    # Find some singleton facts about the review dataset
    find_overall_review_stats(review_rdd)

    # Find the statistics of the businesses dataset
    find_overall_biz_stats(biz_rdd)

    # Generate the set of statistics for every city in the list
    for city in cities:
        find_city_biz_stats(city, biz_rdd, review_rdd)
