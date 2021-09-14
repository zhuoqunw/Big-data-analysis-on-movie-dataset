'''
Lea Wei
uniqname: zhuoqunw
Data collection and preprocessing project

'''
import json 
#import requests
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, expr, lit, explode, split, udf, trim
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType


'''
# extract data from bechdel.com api and write to json file 'bechdel.json'
bechdel_url = "http://bechdeltest.com/api/v1/getAllMovies"
response = requests.get(bechdel_url)
results_obj = response.json()

with open('bechdel.json', 'w') as f:
    json.dump(results_obj, f)

# put datasets from local computer to hadoop
1. put it on cavium cluster first (drag in cyberduck)
2. hadoop fs -put project1
3. hadoop path: hdfs:///user/zhuoqunw/project1/Mojo_budget_update.csv

# submit
spark-submit --master yarn --num-executors 16 --executor-memory 1g --executor-cores 2 si618_project1_zhuoqunw.py

'''


sc = SparkContext(appName="Bechdel Project")
sqlContext = SQLContext(sc)

# load datasets
budget_df = sqlContext.read.csv("hdfs:///user/zhuoqunw/project1/Mojo_budget_update.csv", header=True, inferSchema=True)
imdb_movie_df = sqlContext.read.csv("hdfs:///user/zhuoqunw/project1/IMDb_movies.csv", header=True, inferSchema=True)
rating_df = sqlContext.read.csv("hdfs:///user/zhuoqunw/project1/IMDb_ratings.csv", header=True, inferSchema=True)
bechdel_df = sqlContext.read.json("hdfs:///user/zhuoqunw/project1/bechdel.json")
cpi_df = sqlContext.read.csv("hdfs:///user/zhuoqunw/project1/cpi.csv", header=True, inferSchema=True)


budget_df.printSchema()
imdb_movie_df.printSchema()
rating_df.printSchema()
bechdel_df.printSchema()
cpi_df.printSchema()


####### Clean and prepare for datasets #######

# remove the 'tt' in movie_id column in budget_df, create a new column 'imdbid'
budget_df = budget_df.withColumn("imdbid", expr("substring(movie_id, 3, length(movie_id))"))
imdb_movie_df = imdb_movie_df.withColumn("imdbid", expr("substring(imdb_title_id, 3, length(imdb_title_id))"))
rating_df = rating_df.withColumn("imdbid", expr("substring(imdb_title_id, 3, length(imdb_title_id))"))
# tmdb_df = tmdb_df.withColumn("imdbid", expr("substring(imdb_id, 3, length(imdb_title_id))"))

# for budget_df, convert the type of budget and profits to numeric
for i in ['budget', 'domestic', 'international', 'worldwide']:
    budget_df = budget_df.withColumn(i, budget_df[i].cast(FloatType()))

# register as tables
budget_df.registerTempTable('budget_tbl')
bechdel_df.registerTempTable('bechdel_tbl')
cpi_df.registerTempTable('cpi_tbl')
imdb_movie_df.registerTempTable('imdb_movie_tbl')
rating_df.registerTempTable('rating_tbl')


########### DF1: bechdel + budget + profits #############
# join the budget_df and bechdel_df on 'imdbid' and select the useful columns: 2139 movies
bech_bud = sqlContext.sql("SELECT bechdel_tbl.imdbid, bechdel_tbl.year, budget_tbl.title, bechdel_tbl.rating,\
    budget_tbl.release_date, budget_tbl.budget, \
    budget_tbl.domestic, budget_tbl.international, budget_tbl.worldwide\
    FROM bechdel_tbl JOIN budget_tbl ON (bechdel_tbl.imdbid = budget_tbl.imdbid) ORDER BY bechdel_tbl.year")

#bech_bud.count()

##### adjust for inflation to 2019 dollar: adjusted_value = (old_value * cpi_current) / cpi_old = (budget_2009 * cpi_2019) / cpi_2009
## cpt_df: 
## The adjustment is made using data provided by The Bureau of Labor Statistics at the U.S. Department of Labor.
## The inflation adjustments using series from the "All Urban Consumers (CU)" survey. 
## The so-called "CPI-U" survey is the default, which is an average of all prices paid by all urban consumers. It is not seasonally adjusted. 
## The dataset is identified by the BLS as "CUUR0000SA0." It is used as the default for most basic inflation calculations.

# join bech_bud with cpi_df
movie_df = bech_bud.join(cpi_df, bech_bud.year == cpi_df.Year).select(bech_bud["*"],cpi_df["Annual"])
movie_df = movie_df.withColumn("cpi_2019", lit(255.657)) # cpi_2019=255.657
# create new columns with adjusted budget and profits
def inflation_adj(old_value, cpi_current, cpi_old):
    return (old_value * cpi_current) / cpi_old

movie_df = movie_df.withColumn("adj_budget", inflation_adj(movie_df['budget'], movie_df['cpi_2019'], movie_df['Annual'])) #adjust budget
movie_df = movie_df.withColumn("adj_domgross", inflation_adj(movie_df['domestic'], movie_df['cpi_2019'], movie_df['Annual'])) #adjust domgross
movie_df = movie_df.withColumn("adj_intgross", inflation_adj(movie_df['international'], movie_df['cpi_2019'], movie_df['Annual'])) #adjust intgross
movie_df = movie_df.withColumn("adj_wldgross", inflation_adj(movie_df['worldwide'], movie_df['cpi_2019'], movie_df['Annual'])) #adjust world gross

# movie_df.registerTempTable('movie_tbl')
# sqlContext.sql("SELECT * FROM movie_df WHERE title='Avatar'")


##### convert bechdel rating to actual categories: 0: no two women; 1: no talking; 2: talking about a men; 3: pass the test
movie_df.registerTempTable('movie_df_tbl')

bechdel_budget_profit = sqlContext.sql("""SELECT *,
              CASE
                  WHEN rating = 0 THEN 'Pass 0:Fewer than two women'
                  WHEN rating = 1 THEN "Pass 1:Women don't talk to each other"
                  WHEN rating = 2 THEN 'Pass 2:Women only talk about men'
                  WHEN rating = 3 THEN 'Pass 3:Passes Bechdel Test'
               END AS test_result
               FROM movie_df_tbl""")

bechdel_budget_profit = bechdel_budget_profit.drop("release_date", "Annual", "cpi_2019", "country", "language")
bechdel_budget_profit.registerTempTable("bechdel_budget_profit_tbl")

##### calculate ROI: ROI = box office/budget #####
bechdel_budget_profit = sqlContext.sql("SELECT *, adj_domgross/adj_budget AS dom_roi, adj_intgross/adj_budget AS int_roi, adj_wldgross/adj_budget AS total_roi \
    FROM bechdel_budget_profit_tbl")


# output as a csv file and save to local cavium cluster
bechdel_budget_profit.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/bechdel_budget_profit.csv')



############# DF2: bechdel + basic information about movie ###########
##### join bechdel_df with imdb_movie_df and rating_df: 7683 movies in total
bech_imdb_movie = sqlContext.sql("SELECT bechdel_tbl.imdbid, bechdel_tbl.year, imdb_movie_tbl.title, bechdel_tbl.rating,\
    imdb_movie_tbl.genre, imdb_movie_tbl.duration, imdb_movie_tbl.country, imdb_movie_tbl.language, imdb_movie_tbl.avg_vote,\
    imdb_movie_tbl.director, imdb_movie_tbl.writer\
    FROM bechdel_tbl JOIN imdb_movie_tbl ON (bechdel_tbl.imdbid = imdb_movie_tbl.imdbid) ORDER BY bechdel_tbl.year")

bech_movie = bech_imdb_movie.join(rating_df, bech_imdb_movie.imdbid == rating_df.imdbid).select(bech_imdb_movie["*"], rating_df["total_votes"], rating_df["males_allages_avg_vote"], rating_df["females_allages_avg_vote"])
bech_movie.registerTempTable("bech_movie_tbl")

bech_movie = sqlContext.sql("""SELECT *,
              CASE 
                  WHEN rating = 0 THEN 'Pass 0:Fewer than two women'
                  WHEN rating = 1 THEN "Pass 1:Women don't talk to each other"
                  WHEN rating = 2 THEN 'Pass 2:Women only talk about men'
                  WHEN rating = 3 THEN 'Pass 3:Passes Bechdel Test'
               END AS test_result
               FROM bech_movie_tbl""")
 
#cast_df.join(tmdb_df, cast_df.id == tmdb_df.id).select(cast_df["director_name"], cast_df["director_gender"], tmdb_df["imdbid"])

##### convert 'avg_vote' to float
bech_movie = bech_movie.withColumn("avg_vote" , bech_movie["avg_vote"].cast(FloatType()))

##### explode 'genre', 'country', 'language' to achieve per genre, country, language, director, writer per row
##### 97659 entries
# genre: trim to make sure they are the same
bech_movie = bech_movie.withColumn("genre", split("genre", ","))
bech_movie = bech_movie.withColumn("genre", explode(bech_movie["genre"]))
bech_movie = bech_movie.withColumn("genre", trim(bech_movie.genre))

# country
bech_movie = bech_movie.withColumn("country", split("country", ","))
bech_movie = bech_movie.withColumn("country", explode(bech_movie["country"]))
bech_movie = bech_movie.withColumn("country", trim(bech_movie.country))

# language
bech_movie = bech_movie.withColumn("language", split("language", ","))
bech_movie = bech_movie.withColumn("language", explode(bech_movie["language"]))
bech_movie = bech_movie.withColumn("language", trim(bech_movie.language))

# director
bech_movie = bech_movie.withColumn("director", split("director", ","))
bech_movie = bech_movie.withColumn("director", explode(bech_movie["director"]))
bech_movie = bech_movie.withColumn("director", trim(bech_movie.director))

# writer
bech_movie = bech_movie.withColumn("writer", split("writer", ","))
bech_movie = bech_movie.withColumn("writer", explode(bech_movie["writer"]))
bech_movie = bech_movie.withColumn("writer", trim(bech_movie.writer))

bech_movie.registerTempTable("bech_movie_tbl")

##### create a new dummy column: binary_pass. if rating=0,1,2 -> 0; rating=3 -> 1
bech_movie = sqlContext.sql("""SELECT *,
              CASE 
                  WHEN rating < 3 THEN 0
                  WHEN rating = 3 THEN 1
               END AS binary_pass
               FROM bech_movie_tbl""")
bech_movie.registerTempTable("bech_movie_tbl")

# output as csv file to local cavium cluster
bech_movie.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/bech_movie.csv')



##### bechdel_df #####
bechdel_df = sqlContext.sql("""SELECT *,
              CASE 
                  WHEN rating = 0 THEN 'Pass 0:Fewer than two women'
                  WHEN rating = 1 THEN "Pass 1:Women don't talk to each other"
                  WHEN rating = 2 THEN 'Pass 2:Women only talk about men'
                  WHEN rating = 3 THEN 'Pass 3:Passes Bechdel Test'
               END AS test_result
               FROM bechdel_tbl""")
bechdel_df.registerTempTable("bechdel_tbl")



bechdel_df.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/bechdel.csv')


### Q0: percentage of movies passing the test over decades ###

# create year_group
def categorizer(year):
    if 1880<=year<1920:
        return '1880-1920'
    elif 1920<=year<1930:
        return '1920s'
    elif 1930<=year<1940:
        return '1930s'
    elif 1940<=year<1950:
        return '1940s'
    elif 1950<=year<1960:
        return '1950s'
    elif 1960<=year<1970:
        return '1960s'
    elif 1970<=year<1980:
        return '1970s'
    elif 1980<=year<1990:
        return '1980s'
    elif 1990<=year<2000:
        return '1990s'
    elif 2000<=year<2010:
        return '2000s'
    else: 
        return "2010s"
bucket_udf = udf(categorizer, StringType() )
bechdel_df = bechdel_df.withColumn("year_group", bucket_udf("year"))
bechdel_df.registerTempTable("bechdel_tbl")

# group by year_group and test_result
year_num = sqlContext.sql("SELECT year_group, test_result, COUNT(imdbid) AS num FROM bechdel_tbl GROUP BY year_group, test_result ORDER BY year_group, test_result")
year_num.registerTempTable("year_num_tbl")

year_trend = sqlContext.sql("SELECT year_group, sum(num) AS total FROM year_num_tbl GROUP BY year_group")
year_trend.registerTempTable("year_trend_tbl")

time_trend = year_num.join(year_trend, year_num.year_group==year_trend.year_group).select(year_num["*"], year_trend["total"])
time_trend.registerTempTable("time_trend_tbl")

trend = sqlContext.sql("SELECT year_group, test_result, num/total AS percent FROM time_trend_tbl ORDER BY year_group, test_result")

#trend.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/time_trend.csv')


### Q1: percentage of movies passing the test by production country (>50) ###
country_test = sqlContext.sql("SELECT country, binary_pass, imdbid FROM bech_movie_tbl")
country_test = country_test.drop_duplicates()
country_test.registerTempTable("country_test")
country_pass = sqlContext.sql("SELECT DISTINCT country, AVG(binary_pass) AS percent_pass, COUNT(DISTINCT imdbid) AS number_of_movies FROM country_test GROUP BY country HAVING number_of_movies>50 ORDER BY percent_pass desc")

#country_pass.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/country_pass.csv')


##### Q2: percentage of movies passing the test by genre
genre_pass = sqlContext.sql("SELECT genre, AVG(binary_pass) AS percent_pass FROM bech_movie_tbl GROUP BY genre ORDER BY percent_pass desc")

#genre_pass.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/genre_pass.csv')


##### Q3: percentage of movies passing the test by average imdb votes

def star_bin(imdb_star):
    if 0<=imdb_star<4:
        return 'less than 4'
    elif 4<=imdb_star<5:
        return '4 stars'
    elif 5<=imdb_star<6:
        return '5 stars'
    elif 6<=imdb_star<7:
        return '6 stars'
    elif 7<=imdb_star<8:
        return '7 stars'
    elif 8<=imdb_star<9:
        return '8 stars'

bin_udf = udf(star_bin, StringType())

movie_rating = sqlContext.sql("SELECT imdbid, avg_vote, total_votes,males_allages_avg_vote, females_allages_avg_vote, binary_pass FROM bech_movie_tbl")
movie_rating = movie_rating.drop_duplicates()

movie_rating = movie_rating.withColumn("rating_group", bin_udf("avg_vote"))
movie_rating = movie_rating.withColumn("male_group", bin_udf("males_allages_avg_vote"))
movie_rating = movie_rating.withColumn("female_group", bin_udf("females_allages_avg_vote"))
movie_rating.registerTempTable("movie_rating_tbl")

rating_pass = sqlContext.sql("SELECT rating_group, AVG(binary_pass) AS percent_pass FROM movie_rating_tbl WHERE rating_group IS NOT NULL GROUP BY rating_group ORDER BY rating_group, percent_pass desc")

#rating_pass.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/rating_pass.csv')


## males
male_rating_pass = sqlContext.sql("SELECT male_group, AVG(binary_pass) AS percent_pass FROM movie_rating_tbl WHERE male_group IS NOT NULL GROUP BY male_group ORDER BY male_group, percent_pass desc")

#male_rating_pass.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/male_rating_pass.csv')


## females 
female_rating_pass = sqlContext.sql("SELECT female_group, AVG(binary_pass) AS percent_pass FROM movie_rating_tbl WHERE female_group IS NOT NULL GROUP BY female_group ORDER BY female_group, percent_pass desc")

#female_rating_pass.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/female_rating_pass.csv')


##### Q5: median budget in 2019 dollars of different categories

budget_test = sqlContext.sql("SELECT test_result, percentile_approx(adj_budget, 0.5) AS median_budget_2019 FROM bechdel_budget_profit_tbl GROUP BY test_result ORDER BY median_budget_2019")

#budget_test.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/budget_test.csv')


##### Q6: median domestic & international box office in 2019 dollars of different categories

dom_box_test = sqlContext.sql("SELECT test_result, percentile_approx(adj_domgross, 0.5) AS median_domgross_2019 FROM bechdel_budget_profit_tbl GROUP BY test_result ORDER BY median_domgross_2019")

int_box_test = sqlContext.sql("SELECT test_result, percentile_approx(adj_intgross, 0.5) AS median_intgross_2019 FROM bechdel_budget_profit_tbl GROUP BY test_result ORDER BY median_intgross_2019")

#dom_box_test.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/dom_box_test.csv')
#int_box_test.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/int_box_test.csv')



##### Q7: median domestic ROI & international ROI of different categories

bechdel_budget_profit.registerTempTable("bechdel_budget_profit_tbl")
# domestic
dom_roi_test = sqlContext.sql("SELECT test_result, percentile_approx(dom_roi, 0.5) AS median_dom_roi FROM bechdel_budget_profit_tbl GROUP BY test_result ORDER BY median_dom_roi")

# international
int_roi_test = sqlContext.sql("SELECT test_result, percentile_approx(int_roi, 0.5) AS median_int_roi FROM bechdel_budget_profit_tbl GROUP BY test_result ORDER BY median_int_roi")

#dom_roi_test.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/dom_roi_test.csv')
#int_roi_test.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:///home/zhuoqunw/project1/int_roi_test.csv')
