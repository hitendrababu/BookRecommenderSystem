import sys
import math
import os
import itertools
import re
from collections import defaultdict
from operator import add
from pyspark import SparkConf, SparkContext
from itertools import combinations

#This function is used to calculate the user user pairs of every book
def getCombinations_User_Rating(userRating):
#list of all the combinations is initialized        
		list_of_combinations =[]
#iterTools package is used to calculate combinations        
		for x in itertools.combinations(userRating,2):
#user rating of each user is appended to the combination list			
               list_of_combinations.append(x)
        return list_of_combinations


#function to group the similar users together
def User_RatingPair(book,user_rating):
#gets list of the combination of user ratings	
	combination_of_user_Rating = getCombinations_User_Rating(user_rating)
#groups the users that rated the book similar together	
	for user1,user2 in combination_of_user_Rating:
		return (user1[0],user2[0]),(user1[1],user2[1])
	return

#function to calculate bookID and corressponding userID and rating score 
def Book_UserRating(data):	
#used regex to match the regular expression to the data	
	ID = re.match(r'^"(.*?)";',data).group(1)
#the obtained expression is splitted across the ; and the parts are stored in an array	
	ID_Array = ID.split(";\"\"")
    userID = ID_Array[0]
#used regex to match the regular expression to the data	
	bookID = re.match(r'.*;"(.*?)";',data).group(1)
	rating_value = data.split("\"\"")
	rating_string = rating_value[3].strip()
	try:
		rating_score = float(rating_string)
		if(rating_score == 0.0):
			rating_score = 1.0
	except ValueError:
		rating_score = 3.0		
	return bookID,(userID,rating_score)

#The cosine similarity between two users is calculated and the ratings that they give to each book are used to get similarity
def UserCosineSimilarity(user_pair, rating_list):   	
	total_score_of_A = 0.0
    total_ratings_product_AB = 0.0
    total_score_of_B = 0.0
    number_of_Rating_Pairs = 0
	for rating_pair in rating_list:
		a = float(rating_pair[0])
		b = float(rating_pair[1])
		total_score_of_A = total_score_of_A + a * a 
		total_score_of_B = total_score_of_B + b * b
		total_ratings_product_AB += a * b
		number_of_Rating_Pairs = number_of_Rating_Pairs + 1
	base = (math.sqrt(total_score_of_A) * math.sqrt(total_score_of_B))
	if base == 0.0: 
		return book_pair, (0.0,number_of_Rating_Pairs)
	else:
		cosine_similarity = total_ratings_product_AB / base
 #returns the user pair, cosine similarity and the number of rating pairs 		
	return user_pair, (cosine_similarity, number_of_Rating_Pairs)
	
# Cosine similarity is assigned to the user pairs
def CosineSimilarity_userPair(userPair_cosine_similarity_value_n):
	cosine_similarity_value_n = userPair_cosine_similarity_value_n[1]
	userPair = userPair_cosine_similarity_value_n[0]
	yield(userPair[0],(userPair[1],cosine_similarity_value_n))
	yield(userPair[1],(userPair[0],cosine_similarity_value_n))
	
# calculate the user recommendations by calculating the nearest books using knn
def UserRecommendations(book_ID,tuple_User_Rating,dictionary_of_similarity,number):	
	total_similarity_with_rating = defaultdict(int)
	total_similarity = defaultdict(int)
	for (user,rating) in tuple_User_Rating:
		#The nearest neighbours of books in the dictionary are calculated 
		nearest_neighbors = dictionary_of_similarity.get(user,None)
		if nearest_neighbors is not None:
			for (nearest_neighbor,(cosine_similarity, count)) in nearest_neighbors:
				if nearest_neighbor != user:
					# adds up the similarity of users
					total_similarity_with_rating[nearest_neighbor] = total_similarity_with_rating[nearest_neighbor] + float((str(cosine_similarity)).replace("\"","")) * float((str(rating)).replace("\"",""))
					total_similarity[nearest_neighbor] = total_similarity[nearest_neighbor] + float((str(cosine_similarity)).replace("\"",""))
	# create the normalized list of recommendation scores
	Recommendation_Scores_Of_User = []
   	for value in total_similarity.items():		
		if (value is not None) and (value[0] is not None) and (value[1] is not None):	
			user = str(value[0])
			total_score = float(value[1])
			if total_score == 0.0:
				Recommendation_Scores_Of_User.append((0.0, user))
			else:	
				Recommendation_Scores_Of_User.append((total_similarity_with_rating[user]/total_score, user))
			# Here we sort the Recommendation Scores in descending order
			Recommendation_Scores_Of_User.sort(reverse=True)
#returns the book id with the list of recommendation scores			
	return book_ID,Recommendation_Scores_Of_User[:number]		
	
# Book specific user recommendations scores are generated using this function.
def generateBookRecommendations	(bookID_user_recommendation_scores):
	book_ID = bookID_user_recommendation_scores[0]
	user_recommendation_scores = bookID_user_recommendation_scores[1]
	Book_Recommendation_scores_for_each_user = []
	if (user_recommendation_scores is not None) and (len(user_recommendation_scores) > 0):
		for user_recommendation_score in user_recommendation_scores:
			try:
				user_ID = user_recommendation_score[1]
				rating = user_recommendation_score[0]
				Book_Recommendation_scores_for_each_user.append((user_ID, (rating, book_ID)))
			except ValueError:
				a = 1
	return Book_Recommendation_scores_for_each_user		
#Used to calculate the book recommendation lists
def UserBookRecommendationLists(book_user_recommendation):
	user_Book_recommendation_scores = []
	book_ID = book_user_recommendation[0]
	user_recommendation_scores = book_user_recommendation[1]
	
	if (user_recommendation_scores is not None) and (len(user_recommendation_scores) > 0):
		for user_recommendation_score in user_recommendation_scores:
			if (user_recommendation_score is not None) and (len(user_recommendation_score) > 0):
				for user_recommendation_score_tuple in user_recommendation_score:
					try:
						rating = user_recommendation_score_tuple[0]
						user_ID = user_recommendation_score_tuple[1]
						user_Book_recommendation_scores.append((user_ID, (rating, book_ID)))
					except ValueError:
						a = 1
	return(1,user_Book_recommendation_scores)
#creates the user recommendations dictionary	
def UserBookRecommendationDict(user_book_recommendations_List_Of_Lists):
	user_book_recommendations_MainList = []
	dictionary_User_Book_Recommendation = {}
	tupleList_User_Book_Recommendation = []
	
	for user_book_recommendations_Lists in user_book_recommendations_List_Of_Lists[1]:
		user_book_recommendations_MainList += user_book_recommendations_Lists
	
	for (user, book_rating) in user_book_recommendations_MainList:
		if user in dictionary_User_Book_Recommendation:
			dictionary_User_Book_Recommendation[user].append(book_rating)
		else:
			dictionary_User_Book_Recommendation[user] = [book_rating]
	
	for key , value in dictionary_User_Book_Recommendation.items():
	    tupleList_User_Book_Recommendation.append((key,value))
		
	return tupleList_User_Book_Recommendation	
		

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print >> sys.stderr, "Usage: program_file <Book-Rating_file> <output_path>"
		exit(-1)
	# reading input from the path mentioned			
	configuration= (SparkConf()
			.setMaster("local")
			.setAppName("user-user-collaboration")
			.set("spark.executor.memory", "16g")	
			.set("spark.driver.memory", "16g")
			.set('spark.kryoserializer.buffer.max', '1g'))
	sc = SparkContext(conf = configuration)	

	# The book ratings contain the data file
	bookRatings = sc.textFile(sys.argv[1], 1)
	train_Data, test_Data = bookRatings.randomSplit([0.80, 0.20], seed = 11L)
	
	# the book user ratingis calculated by calling Book_User rating function	
	Book_UserRating = train_Data.map(lambda x : Book_UserRating(x)).filter(lambda p: len(p[1]) > 1).groupByKey().cache()

	# the possible pairs for user-user pair for given book and the ratings of the corresponding booksare stored	
	User_RatingPair = Book_UserRating.map(lambda x: User_RatingPair(x[0],x[1])).filter(lambda x: x is not None).groupByKey().cache()
	
	#the cosine similarity between two users is calculated
	userCosineSimilarity = User_RatingPair.map(lambda x: UserCosineSimilarity(x[0],x[1])).filter(lambda x: x[1][0] >= 0)
	
	# values that contain user Id as key and the value is a list of tuples that has other user ids and their cosine similarity with the key user
	cosineSimilarity_userPair = userCosineSimilarity.flatMap(lambda x : CosineSimilarity_userPair(x)).collect()
	#creates a dictionary for the cosine similarity
	CosineDict = {}
	for (user, data) in cosineSimilarity_userPair:
		if user in CosineDict: 
			CosineDict[user].append(data)
		else:
			CosineDict[user] = [data]
	    #no of books to recommended    
			BooksRecommended = 50
	
	# list of users with their recomendation score for each book is calculated
	    
	book_user_recommendations = Book_UserRating.map(lambda x: UserRecommendations(x[0], x[1], CosineDict, BooksRecommended)).filter(lambda x: x is not None).groupByKey().cache()
	
	user_book_recommendations_Lists = book_user_recommendations.map(lambda x: UserBookRecommendationLists(x)).groupByKey()
	
	user_bookRecomendations_List = user_book_recommendations_Lists.map(lambda x: UserBookRecommendationDict(x)).filter(lambda x: x is not None)
	#output path is specified
	outputPath = sys.argv[2]+"/BookRecomendations"
	user_bookRecomendations_List.saveAsTextFile(outputPath)

        
    #context is stopped
	sc.stop()

