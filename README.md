# Big Data Analysis on Movie Dataset
SI 618 final project 

# Main Tools: 
**💻Data Analysis: Spark SQL, PySpark, Hadoop**  
**🎨Data visualization: Python Altair**

## Motivation
It’s not a secret that there exists an inherent gender bias in the movie business. Female actors usually have less income than male actors. There are fewer female protagonists in movies, and female characters are usually lack of serious development and depth compared to their male counterparts. How to quantify this gender bias is one of the topics of interest. Inspired by an article by [FiveThirtyEight](https://fivethirtyeight.com/features/the-dollar-and-cents-case-against-hollywoods-exclusion-of-women/) about the relationship between female prominence in movies, evaluated by the Bechdel test, and movie budget and box office, I decided to explore this topic further in this project. Specifically, I was wondering **what kind of movie will pass the Bechdel test**. Thus, in this project, I examined and discussed the relationship between a set of movie characteristics with passing the Bechdel test, including release decade, country of production, movie genre, crew gender, IMDb rating, budget, domestic and international box office and return of investment (ROI).

## Data Source
#### * Bechdel movie dataset from [BechdelTest.com](BechdelTest.com)
#### * Boxofficemojo dataset from [Kaggle](https://www.kaggle.com/igorkirko/wwwboxofficemojocom-movies-with-budget-listed?select=Mojo_budget_update.csv)
#### * IMDb movies extensive dataset from [Kaggle](https://www.kaggle.com/stefanoleone992/imdb-extensive-dataset)
#### * [CPI data from the Bureau of Labor Statistics at the U.S. Department of Labor](https://data.bls.gov/pdq/SurveyOutputServlet)

## Data Manipulation
### workflow
<img width="851" alt="workflow" src="https://user-images.githubusercontent.com/56980385/133186122-41d79bae-bdc9-485d-ab1c-8dff7e114582.png">

## Analysis and Visualization
**Some of the findings:  **

**💡For more analyses, including details of data preprocessing and manipulation, and more visualizations, please refer to the [final report](https://github.com/zhuoqunw/Big-data-analysis-on-movie-dataset/blob/main/SI%20618%20Project%201%20Report-Lea.pdf)**

1. Overall, there is an increasing trend in the percentage of movies passing the Bechdel test (represented by the green bars) over decades.

![time_trend](https://user-images.githubusercontent.com/56980385/133186561-664bdcfe-ccda-4513-8b0b-72264bfc857e.png)

2. There is an overall trend that the higher the IMDb rating, the lower the percentage of movies passing the Bechdel test.

![rating](https://user-images.githubusercontent.com/56980385/133186879-1fdf4d68-7920-4797-b0c5-b2bceb4dc8b6.png)

Another interesting finding is that, this negative relationship between IMDb rating and the percentage passing the Bechdel test doesn’t differ between male and female voters

![male_female_rating](https://user-images.githubusercontent.com/56980385/133186925-a1622d62-0834-4423-887d-58a12d416b3f.png)


3. Movies passing the test (represented by the highlighted orange bar) have the lowest median budget, but not the lowest box office and ROI
![budget](https://user-images.githubusercontent.com/56980385/133187126-b5e55a66-521a-4784-9e76-f5d95da2dc5b.png)

![box office](https://user-images.githubusercontent.com/56980385/133187192-5b7ecda5-3e7f-4fb1-9655-3b5d0f2490c6.png)

![roi](https://user-images.githubusercontent.com/56980385/133187203-65a7263f-513d-4bae-8c8e-637845d0e094.png)
