# Databricks notebook source
# MAGIC %md
# MAGIC ## Get list of Beginner courses
# MAGIC 
# MAGIC Develop logic to filter the courses which are suitable for `Beginners`. As part of this Spark Practice Test, we are primarily evaluating the ability to filter as well as sort the data using Spark Data Frames.
# MAGIC * We will provide course data in the form of a Data Frame.
# MAGIC * Get the courses which are suitable for Beginners - `suitable_for` should be equal to `Beginner`.
# MAGIC * Data should be sorted in descending order by `number_of_ratings`.
# MAGIC * Here is the data provided below in the form of Spark Data Frame.

# COMMAND ----------

courses = [{'course_id': 1,
  'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
  'suitable_for': 'Beginner',
  'enrollment': 1100093,
  'stars': 4.6,
  'number_of_ratings': 318066},
 {'course_id': 4,
  'course_name': 'Angular - The Complete Guide (2020 Edition)',
  'suitable_for': 'Intermediate',
  'enrollment': 422557,
  'stars': 4.6,
  'number_of_ratings': 129984},
 {'course_id': 12,
  'course_name': 'Automate the Boring Stuff with Python Programming',
  'suitable_for': 'Advanced',
  'enrollment': 692617,
  'stars': 4.6,
  'number_of_ratings': 70508},
 {'course_id': 10,
  'course_name': 'Complete C# Unity Game Developer 2D',
  'suitable_for': 'Advanced',
  'enrollment': 364934,
  'stars': 4.6,
  'number_of_ratings': 78989},
 {'course_id': 5,
  'course_name': 'Java Programming Masterclass for Software Developers',
  'suitable_for': 'Advanced',
  'enrollment': 502572,
  'stars': 4.6,
  'number_of_ratings': 123798},
 {'course_id': 15,
  'course_name': 'Learn Python Programming Masterclass',
  'suitable_for': 'Advanced',
  'enrollment': 240790,
  'stars': 4.5,
  'number_of_ratings': 58677},
 {'course_id': 3,
  'course_name': 'Machine Learning A-Z™: Hands-On Python & R In Data Science',
  'suitable_for': 'Intermediate',
  'enrollment': 692812,
  'stars': 4.5,
  'number_of_ratings': 132228},
 {'course_id': 14,
  'course_name': 'Modern React with Redux [2020 Update]',
  'suitable_for': 'Intermediate',
  'enrollment': 203214,
  'stars': 4.7,
  'number_of_ratings': 60835},
 {'course_id': 8,
  'course_name': 'Python for Data Science and Machine Learning Bootcamp',
  'suitable_for': 'Intermediate',
  'enrollment': 387789,
  'stars': 4.6,
  'number_of_ratings': 87403},
 {'course_id': 6,
  'course_name': 'React - The Complete Guide (incl Hooks, React Router, Redux)',
  'suitable_for': 'Intermediate',
  'enrollment': 304670,
  'stars': 4.6,
  'number_of_ratings': 90964},
 {'course_id': 18,
  'course_name': 'Selenium WebDriver with Java -Basics to Advanced+Frameworks',
  'suitable_for': 'Advanced',
  'enrollment': 148562,
  'stars': 4.6,
  'number_of_ratings': 49947},
 {'course_id': 21,
  'course_name': 'Spring & Hibernate for Beginners (includes Spring Boot)',
  'suitable_for': 'Advanced',
  'enrollment': 177053,
  'stars': 4.6,
  'number_of_ratings': 45329},
 {'course_id': 7,
  'course_name': 'The Complete 2020 Web Development Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 270656,
  'stars': 4.7,
  'number_of_ratings': 88098},
 {'course_id': 9,
  'course_name': 'The Complete JavaScript Course 2020: Build Real Projects!',
  'suitable_for': 'Intermediate',
  'enrollment': 347979,
  'stars': 4.6,
  'number_of_ratings': 83521},
 {'course_id': 16,
  'course_name': 'The Complete Node.js Developer Course (3rd Edition)',
  'suitable_for': 'Advanced',
  'enrollment': 202922,
  'stars': 4.7,
  'number_of_ratings': 50885},
 {'course_id': 13,
  'course_name': 'The Complete Web Developer Course 2.0',
  'suitable_for': 'Intermediate',
  'enrollment': 273598,
  'stars': 4.5,
  'number_of_ratings': 63175},
 {'course_id': 11,
  'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 325047,
  'stars': 4.5,
  'number_of_ratings': 76907},
 {'course_id': 20,
  'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
  'suitable_for': 'Beginner',
  'enrollment': 203366,
  'stars': 4.6,
  'number_of_ratings': 45382},
 {'course_id': 2,
  'course_name': 'The Web Developer Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 596726,
  'stars': 4.6,
  'number_of_ratings': 182997},
 {'course_id': 19,
  'course_name': 'Unreal Engine C++ Developer: Learn C++ and Make Video Games',
  'suitable_for': 'Advanced',
  'enrollment': 229005,
  'stars': 4.5,
  'number_of_ratings': 45860},
 {'course_id': 17,
  'course_name': 'iOS 13 & Swift 5 - The Complete iOS App Development Bootcamp',
  'suitable_for': 'Advanced',
  'enrollment': 179598,
  'stars': 4.8,
  'number_of_ratings': 49972}]

import pandas as pd
courses_df = spark.createDataFrame(pd.DataFrame(courses))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Preview the data
# MAGIC 
# MAGIC Let us first preview the data.

# COMMAND ----------

courses_df

# COMMAND ----------

type(courses_df)

# COMMAND ----------

display(courses_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Provide the solution
# MAGIC 
# MAGIC Now come up with the solution by developing the required logic. Once the function is developed, go to the next step to take care of the validation.
# MAGIC 
# MAGIC **The logic should be passed using Data Frame style APIs as part of relevant function to filter the data**

# COMMAND ----------

# The logic should go here. Make sure to use import out side the below function definition.
def get_courses_for_beginners(courses_df, suitable_for):
    return courses_for_beginners

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Validate the function
# MAGIC 
# MAGIC Let us validate the function by running below cells.
# MAGIC * Here is the expected output.
# MAGIC 
# MAGIC ```
# MAGIC [{'course_id': 1,
# MAGIC   'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
# MAGIC   'suitable_for': 'Beginner',
# MAGIC   'enrollment': 1100093,
# MAGIC   'stars': 4.6,
# MAGIC   'number_of_ratings': 318066},
# MAGIC  {'course_id': 2,
# MAGIC   'course_name': 'The Web Developer Bootcamp',
# MAGIC   'suitable_for': 'Beginner',
# MAGIC   'enrollment': 596726,
# MAGIC   'stars': 4.6,
# MAGIC   'number_of_ratings': 182997},
# MAGIC  {'course_id': 7,
# MAGIC   'course_name': 'The Complete 2020 Web Development Bootcamp',
# MAGIC   'suitable_for': 'Beginner',
# MAGIC   'enrollment': 270656,
# MAGIC   'stars': 4.7,
# MAGIC   'number_of_ratings': 88098},
# MAGIC  {'course_id': 11,
# MAGIC   'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
# MAGIC   'suitable_for': 'Beginner',
# MAGIC   'enrollment': 325047,
# MAGIC   'stars': 4.5,
# MAGIC   'number_of_ratings': 76907},
# MAGIC  {'course_id': 20,
# MAGIC   'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
# MAGIC   'suitable_for': 'Beginner',
# MAGIC   'enrollment': 203366,
# MAGIC   'stars': 4.6,
# MAGIC   'number_of_ratings': 45382}]
# MAGIC ```

# COMMAND ----------

courses_for_beginners = get_courses_for_beginners(courses_df, 'Beginner')

# COMMAND ----------

type(courses_for_beginners) # It should be DataFrame

# COMMAND ----------

courses_for_beginners.count() # It should be 5

# COMMAND ----------

display(courses_for_beginners)

# COMMAND ----------

courses_for_beginners.toPandas().to_dict(orient='records')

# COMMAND ----------


