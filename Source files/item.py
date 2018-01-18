import re
import sys
import math
import itertools
from operator import add
from itertools import combinations
from pyspark import SparkConf, SparkContext
from collections import defaultdict


# We write this function to calculate the BookId, rating tuple corresponding to each user.
def User_BookRating(dataLine):	
	# We use regex to extract data which has n characters and is ended by ')";'
	y = re.match(r'^"(.*?)";',dataLine).group(1)
	# We split the given input at ;
	yArr = y.split(";\"\"")
	# Stores all the values in a list
	userId = yArr[0]
	# this is used to extract bookId from Data using the similar regex pattern
	bookId = re.match(r'.*;"(.*?)";',dataLine).group(1)
	#ratingValue = re.match(r'.*;"(.*?)"$',dataLine)
	ratingValue = dataLine.split("\"\"")
	ratingstr = ratingValue[3].strip()
	try:
		ratingScore = float(ratingstr)
		# whenever we encounter a rating with 0, we give it a minimum rating to show it in recommendations of least priority
		if(ratingScore == 0.0):
			ratingScore = 1.0
	except ValueError:
		ratingScore = 3.0		
	return userId,(bookId,ratingScore)	

# This is used to calculate the book-book pairs of every userId
def getCombinations_User_Rating(bookRating):
        list_of_combinations =[]
		# we used itertools package to calculate combinations
        for x in itertools.combinations(bookRating,2):
			#Store all the combinatipons in a list.
               list_of_combinations.append(x)
        return list_of_combinations


# This is used to calculate the book, rating tuples of frequency 2.
def Item_RatingPair(user,bookRating):
	# This function returns a list of combinations
	bookRatingCombinations = getCombinations_User_Rating(bookRating)
	for item1,item2 in bookRatingCombinations:
		# This calculates the tuples of book and ratings correspondingly
	      return (item1[0],item2[0]),(item1[1],item2[1])
	return

# This function is used to calculate the cosine similarity between books.
def CosineSimilarity(book_pair, rating_pair_list):   	
	total_Score_Of_A = 0.0 
        total_ratings_product_AB = 0.0 
        total_Score_Of_B = 0.0
        num_Rating_Pairs = 0
	for rating_pair in rating_pair_list:
		a = float(rating_pair[0])
		b = float(rating_pair[1])
		total_Score_Of_A = total_Score_Of_A + a * a 
		total_Score_Of_B = total_Score_Of_B + b * b
		total_ratings_product_AB = total_ratings_product_AB + a * b
		num_Rating_Pairs = num_Rating_Pairs + 1
	base = (math.sqrt(total_Score_Of_A) * math.sqrt(total_Score_Of_B))
	if base == 0.0: 
		return book_pair, (0.0,num_Rating_Pairs)
	else:
		cosine_Similarity = total_ratings_product_AB / base
		# returns bookId, Cosine similarity and the number of pairs.
  		return book_pair, (cosine_Similarity, num_Rating_Pairs)

# Cosine similarity is calculated between book 1 and book 2
def Book_Cosine_Similarity(bookPair_CosineSimVal):
	bookPair = bookPair_CosineSimVal[0]
	cosine_similarity_value_n = bookPair_CosineSimVal[1]
	yield(bookPair[0],(bookPair[1],cosine_similarity_value_n))
	yield(bookPair[1],(bookPair[0],cosine_similarity_value_n))

# User specific book recommendations scores are generated using this function.
def generateBookRecommendations(user_ID,tuple_book_Rating,dictionary_of_similarity,n):	
	total_Similarity_With_Rating = defaultdict(int)
	total_Similarity = defaultdict(int)
	for (book,rating) in tuple_book_Rating:
       		# Nearest neighbors for every book is calculated here
       		nearest_neighbors = dictionary_of_similarity.get(book,None)
        	if nearest_neighbors:
           		for (nearest_neighbor,(cosine_similarity, count)) in nearest_neighbors:
                		if nearest_neighbor != book:
                    			# Rating data is used here to update total and sum.
                    			total_Similarity_With_Rating[nearest_neighbor] = total_Similarity_With_Rating[nearest_neighbor] + float((str(cosine_similarity)).replace("\"","")) * float((str(rating)).replace("\"",""))
                    			total_Similarity[nearest_neighbor] = total_Similarity[nearest_neighbor] + float((str(cosine_similarity)).replace("\"",""))

    # This is used to create a list of recommendation scores
	Recommendation_Scores_of_Books = []
   	for (book,total_Score) in total_Similarity.items():
		if total_Score == 0.0:
			Recommendation_Scores_of_Books.append((0, book))
		else:	
			Recommendation_Scores_of_Books.append((total_Similarity_With_Rating[book]/total_Score, book))

    	# sort the book Recommendation Scores in descending order
    	Recommendation_Scores_of_Books.sort(reverse=True)
    	return user_ID,Recommendation_Scores_of_Books[:n]
# this is used to calculate the error value which is the how accuracy value is obtained.
def generate_errdata(bookRecommendations, testData):
	ErrData = []
	for (recommendationScore, book) in bookRecommendations:
		for (testData_book, testData_rating) in testData:
			if str(testData_book.encode('ascii', 'ignore')) == str(book.encode('ascii', 'ignore')):
				ErrData.append((float(recommendationScore),float(testData_rating)))
	return ErrData			
# this is used to calculate the accuracy
def calculate_err(ErrData):
	n = float(len(ErrData))
	total = 0.0
	for (x,y) in ErrData:
		total += abs(x-y)
	if n == 0.0:
		return 0.0
	return total/n
# THE main execution starts here
if __name__ == "__main__":
	# we have to give 3 arguments else this returns an error and terminates the program
	if len(sys.argv) != 3:
		print >> sys.stderr, "Usage: program_file <Books_file> <Users_file> <User-Rating_file> <output_path>"
		exit(-1)
	# Configuration is used to setAPplication name, specify the memory allocation.		
	configuration = (SparkConf()
			.setMaster("local")
			.setAppName("item-item-collaboration")
			.set("spark.executor.memory", "6g")	
			.set("spark.driver.memory", "6g"))
	#sc = SparkContext(appName="item-item-collaboration")
	# Used to read data from the specified paths
	sc = SparkContext(conf = configuration)

	# The input file Book-ratings.csv file is readhere
	bookRatings = sc.textFile(sys.argv[1], 1)
	# the data is divided randomly into 80% and 20% data.
	train_Data, test_Data = bookRatings.randomSplit([0.80, 0.20], seed = 11L)
	
	# it contains data in the form UID,[(BID,Rating),(BID,Rating)..]	using filter.
	user_bookRating = train_Data.map(lambda x : User_BookRating(x)).filter(lambda p: len(p[1]) > 1).groupByKey().cache()

	# It is in the form (BID,BID)[(R,R),(R,R)]	
	Item_RatingPair = user_bookRating.map(lambda x: Item_RatingPair(x[0],x[1])).filter(lambda x: x is not None).groupByKey().cache()
	
	# It is in the form (BID,BID)(CS,numberPairs)
	bookPair_CosineSimilarity = Item_RatingPair.map(lambda x: CosineSimilarity(x[0],x[1])).filter(lambda x: x[1][0] >= 0)
	
	# This calculates cosine similarity between book and other books
	Book_Cosine_value = bookPair_CosineSimilarity.flatMap(lambda x : Book_Cosine_Similarity(x)).collect()
	# We use a dictionary to store the data 
	BookCosineDict = {}
	for (book, data) in Book_Cosine_value:
		if book in BookCosineDict:
			BookCosineDict[book].append(data)
		else:
			BookCosineDict[book] = [data]
	        NumOfBooks = 50
	
	# Genarates the list of all books whic are related to User
	user_book_recommendations = user_bookRating.map(lambda x: generateBookRecommendations(x[0], x[1], BookCosineDict, NumOfBooks))
	# This is used to change create a folder BookRecommendations in output path to store output.
	outputPath = sys.argv[2]+"/BookRecommendations"
	# This is used to save the output as a text file.
	user_book_recommendations.saveAsTextFile(outputPath)	
	
	# Recommendation scores of training data is compared with test data
	test_Data_User_BookRating = test_Data.map(lambda x : User_BookRating(x)).filter(lambda p: len(p[1]) > 1).groupByKey().cache()
	rec_data = user_book_recommendations.join(test_Data_User_BookRating).flatMap( lambda x : generate_errdata(x[1][0],list(x[1][1])))
	rec_dataScore = []
	for (recScore,testDataScore) in rec_data.collect():
		rec_dataScore.append((recScore,testDataScore))
	# this is used to calculate the accuracy value.
	error = calculate_err(rec_dataScore)
	error = sc.parallelize([error])
	# This is used to save the output as a text file.
	error.saveAsTextFile(sys.argv[2]+"/error")
	# Spark context is stopped.
	sc.stop()
	
