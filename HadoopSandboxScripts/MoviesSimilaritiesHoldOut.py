# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 06:34:35 2015

@author: c202aco
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 11:50:17 2015

@author: C202ACO
"""

#-*-coding: utf-8 -*-

'''
This module calculates similarities between movies using cosine similarity

1a) First the mapper emits the the userId, movieId, rating, timestamp using userId as key
1b) The reducer emits the userId and potings, also how many movies the user have watched and the sum rating of those movies

2a) Removes userId, emits rating pairs
2b) Calculate cosine similarities of all pairs

3a) Emits item x and similarity measusre as key and other movies and number of coratings as value
3b) Emitsitem x, item y, cosine sim and number of co ratings. If a csv file is passed than it emits predicted values from the recommender 
    and compares the true value


'''
__author__ = 'Unsupervised Learners'


#Preprocessing

from mrjob.job import MRJob
import csv
import math
try:
    from itertools import combinations
except ImportError:
    from metrics import combinations

PRIOR_COUNT = 10
PRIOR_CORRELATION = 0

class MoviesSimilarities(MRJob):
    
    ##If one would only like to compare one movie with other movies 
    def configure_options(self):
        super(MoviesSimilarities, self).configure_options()
        
        #Movie id comparison parameter
        self.add_passthrough_option('--movieId-to-compare',dest='movieid_to_compare', type='int', default=0, help='Select the movieId to compare, if 0 inputed it compares all movies')
        
        #Normalize ratings parameter
        self.add_passthrough_option('--normalize',dest='normalize', type='int', default=1, help='Should the ratings be normalized, meaning subtracting the average rating of each user for every rating the user made')
        
        #Minimum coratingsparameter        
        self.add_passthrough_option('--minimum-nr-of-coratings',dest='min_co_ratings', type='int', default=0, help='Select the minimum number of coratings a movie-pair needs in order to be recommended')
        
        #Hold out sample from csv file 
        self.add_file_option('--hold-out-sample-file',default='C:\\temp\\HoldOutSample2000.csv', dest="csv_Hold_Out_Sample", help='file with containing a hold out sample to evaluate the recommender system')    

    def steps(self):
        return [self.mr(mapper_init=self.group_by_user_rating_init, mapper=self.group_by_user_rating,
                         reducer=self.count_ratings_users_freq),
                self.mr(mapper_init=self.pairwise_items_init, mapper=self.pairwise_items, reducer=self.calculate_similarity),
                self.mr(mapper=self.calculate_ranking,  reducer_init=self.top_similar_items_init, reducer=self.top_similar_items)
                ]
                
    def group_by_user_rating_init(self):
        #Set csv file for hold out sample 
        csv_Hold_Out_Sample = self.options.csv_Hold_Out_Sample        
        
        #Read csv file and choose movieratings to iclude if a hold out data set is used
        if csv_Hold_Out_Sample <> 'None':
            rownum=0
            content = []
            with open('C:\\temp\\HoldOutSample2000.csv', 'rb') as f:
                reader = csv.reader(f, delimiter=",")
                for row in reader:
                    if rownum<>0: 
                        content.append(row)
                    rownum=+1
            #Extract last seen movie and second last movie seen from hold out set  
            lastmovieId = []
            secondlastmovieId = []
            for element in content:
                lastmovieId.append(int(element[1]))
                secondlastmovieId.append(int(element[2]))
            #Merge movies
            self.movieIdsToInclude = lastmovieId + secondlastmovieId
            self.holdout = content   
        

    def group_by_user_rating(self, key, line):
        """
        Emit the user_id and group by their ratings (item and rating), ratingCount will be used 
        for later calculating the number of counts
        
        17  70,3
        35  21,1
        49  19,2
        49  21,1
        49  70,4
        87  19,1
        87  21,2
        98  19,2
        """               
        
        #Check if hold out sample is used        
        csv_Hold_Out_Sample = self.options.csv_Hold_Out_Sample           
        
        #Handle the lines in from the movie in table    
        userId, movieId, rating, timestamp = line.split(',')
        
        #Set ratings count to 1
        ratingCount=1 
        
        #Output userId, movieId, rating, ratings_count, check if top is header'
        if csv_Hold_Out_Sample <> 'None':
            if not any(c.isalpha() for c in userId):
                if int(movieId) in self.movieIdsToInclude:
                    yield  int(userId), (int(movieId), float(rating), int(ratingCount))
                
        else:
            if not any(c.isalpha() for c in userId):
                yield  int(userId), (int(movieId), float(rating), int(ratingCount))
            

    def count_ratings_users_freq(self, userId, values):
        """
        For each user, emit a row containing their "postings"
        (movie,rating pairs)
        Also emit user rating sum and count for use in later steps.
        17    1,3,(70,3)
        35    1,1,(21,1)
        49    3,7,(19,2 21,1 70,4)
        87    2,3,(19,1 21,2)
        98    1,2,(19,2)
        """
        #Check movie id to compare        
        movieIdToCompare = self.options.movieid_to_compare         
        
        movieCount = 0
        movieSum = 0
        final = []
        hasSeenMovieIdtoCompare=False
        
        for movieId, rating, ratingCount in values:
            movieCount += 1
            movieSum += rating
            if movieIdToCompare <> 0:
                if movieId == movieIdToCompare:
                    hasSeenMovieIdtoCompare=True
            final.append((movieId, rating, ratingCount))

        #Only yield output if user has seen a specific movie
        if movieIdToCompare <> 0: 
            if hasSeenMovieIdtoCompare:
                yield userId, (movieCount, movieSum, final)
        else:
            yield userId, (movieCount, movieSum, final)
    
    def pairwise_items_init(self):
        #Set csv file for hold out sample 
        csv_Hold_Out_Sample = self.options.csv_Hold_Out_Sample        
        
        #Read csv file and choose movieratings to iclude if a hold out data set is used
        if csv_Hold_Out_Sample <> 'None':
            rownum=0
            content = []
            with open('C:\\temp\\HoldOutSample2000.csv', 'rb') as f:
                reader = csv.reader(f, delimiter=",")
                for row in reader:
                    if rownum<>0: 
                        content.append(row)
                    rownum=+1
            #Extract last seen movie and second last movie seen from hold out set  
            lastmovieId = []
            secondlastmovieId = []
            for element in content:
                lastmovieId.append(int(element[1]))
                secondlastmovieId.append(int(element[2]))
            #Merge movies
            self.movieIdsToInclude = lastmovieId + secondlastmovieId
            self.lastmovieId = lastmovieId
            self.secondlastmovieId = secondlastmovieId
            self.holdout = content   
        
    def pairwise_items(self, userId, values):
        '''
        The output drops the user from the key entirely, instead it emits
        the pair of items as the key:
        19,21  2,1
        19,70  2,4
        21,70  1,4
        19,21  1,2
        This mapper is quite slow.  One improvement
        would be to create a java Combiner to aggregate the
        outputs by key before writing to hdfs, another would be to use
        a vector format and SequenceFiles instead of streaming text
        for the matrix data.
        '''
        movieCount, movieSum, ratings = values
        #print item_count, item_sum, [r for r in combinations(ratings, 2)]
        #bottleneck at combinations
        movieIdToCompare = self.options.movieid_to_compare 
        normalize = self.options.normalize
        
        #Check if hold out sample is used
        csv_Hold_Out_Sample = self.options.csv_Hold_Out_Sample  

        if movieIdToCompare<>0:   
            #Set combiner
            combinerIterator=combinations(ratings,2)
            for item1, item2 in combinerIterator: #filter(lambda x: ((x[0][0]==movieIdToCompare) or (x[1][0]==movieIdToCompare))  , list(combinations(ratings,2))) :
                if item1[0]==movieIdToCompare or item2[0]==movieIdToCompare:
                    if normalize <> 0:
                        yield (item1[0], item2[0]), \
                                (item1[1]-(movieSum/movieCount), item2[1]-(movieSum/movieCount), item1[2], item2[2])     
                    else:
                        
                        yield (item1[0], item2[0]), \
                                (item1[1], item2[1], item1[2], item2[2])     
                        
        else:    
            if csv_Hold_Out_Sample <> 'None':
                for item1, item2 in combinations(ratings,2) :
                    #Check if movie pair exist in hold at data set 
                    try:
                        if(self.lastmovieId.index(int(item1[0])) == self.secondlastmovieId.index(int(item2[0]))):
                            #if ((item1[0] in self.lastmovieId) and (item2[0] in self.secondlastmovieId)) or ((item1[0] in self.secondlastmovieId) and (item2[0] in self.lastmovieId)):
                                if normalize <> 0:
                                    yield (item1[0], item2[0]), \
                                            (item1[1]-(movieSum/movieCount), item2[1]-(movieSum/movieCount), item1[2], item2[2])
                                else:
                                    yield (item1[0], item2[0]), \
                                            (item1[1], item2[1], item1[2], item2[2])
                    except ValueError:
                        pass
                    #Must use this one as well to see if item is on the other side
                    try:
                        if(self.lastmovieId.index(int(item2[0])) == self.secondlastmovieId.index(int(item1[0]))):
                            #if ((item1[0] in self.lastmovieId) and (item2[0] in self.secondlastmovieId)) or ((item1[0] in self.secondlastmovieId) and (item2[0] in self.lastmovieId)):
                                if normalize <> 0:
                                    yield (item1[0], item2[0]), \
                                            (item1[1]-(movieSum/movieCount), item2[1]-(movieSum/movieCount), item1[2], item2[2])
                                else:
                                    yield (item1[0], item2[0]), \
                                            (item1[1], item2[1], item1[2], item2[2])
                    except ValueError:
                        pass
                    
            else:
                #If hold-out sample is not used, emit this one
                for item1, item2 in combinations(ratings,2) :
                    if normalize <> 0:
                        yield (item1[0], item2[0]), \
                                (item1[1]-(movieSum/movieCount), item2[1]-(movieSum/movieCount), item1[2], item2[2])
                    else:
                        yield (item1[0], item2[0]), \
                                (item1[1], item2[1], item1[2], item2[2])
        
    def calculate_similarity(self, moviePairKey, lines):
        '''
        Sum components of each corating pair across all users who rated both
        item x and item y, then calculate cosine similarity and
        counts.  The similarities are normalized to the [0,1] scale
        because we do a numerical sort.
        
        19,21   0.4,2
        21,19   0.4,2
        19,70   0.6,1
        70,19   0.6,1
        21,70   0.1,1
        70,21   0.1,1
        '''
        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        item_pair, co_ratings = moviePairKey, lines
        item_xname, item_yname = item_pair
        for item_x, item_y, nx_count, ny_count in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1

        if int(n) >= self.options.min_co_ratings:
            #TODO, calculate other similarity measures
            cos_sim = sum_xy/math.sqrt(sum_xx*sum_yy)
            if n > 0:
                if math.sqrt((sum_xx-((math.pow(sum_x,2))/n))*(sum_yy-((math.pow(sum_y,2))/n)))<>0:
                    corr_sim = ((sum_xy) - (sum_x*sum_y)/n) / math.sqrt((sum_xx-((math.pow(sum_x,2))/n))*(sum_yy-((math.pow(sum_y,2))/n)))
                else:
                    corr_sim=0
            else:
                 corr_sim=0
                 
            rating_diff = (sum_y-sum_x)/n 
            
            #Output movie x and movie y 
            yield (item_xname, item_yname), (cos_sim, corr_sim, rating_diff, n)
        
    def calculate_ranking(self, moviePairKey, values):
        '''
        Emit items with similarity in key for ranking:
        19,0.4    70,1
        19,0.6    21,2
        21,0.6    19,2
        21,0.9    70,1
        70,0.4    19,1
        70,0.9    21,1
        '''        
        cos_sim, corr_sim, rating_diff, n = values
        item_x, item_y = moviePairKey
        
        #Emits similar items
        yield (item_x, cos_sim,corr_sim, rating_diff), (item_y, n)

    def top_similar_items_init(self):
        #Set csv file for hold out sample 
        csv_Hold_Out_Sample = self.options.csv_Hold_Out_Sample        
        
        #Read csv file and choose movieratings to iclude if a hold out data set is used
        if csv_Hold_Out_Sample <> 'None':
            rownum=0
            content = []
            with open('C:\\temp\\HoldOutSample2000.csv', 'rb') as f:
                reader = csv.reader(f, delimiter=",")
                for row in reader:
                    if rownum<>0: 
                        content.append(row)
                    rownum=+1
            #Extract last seen movie and second last movie seen from hold out set  
            lastmovieId = []
            secondlastmovieId = []
            for element in content:
                lastmovieId.append(int(element[1]))
                secondlastmovieId.append(int(element[2]))
            #Merge movies
            self.movieIdsToInclude = lastmovieId + secondlastmovieId
            self.lastmovieId = lastmovieId
            self.secondlastmovieId = secondlastmovieId
            self.holdout = content       
    
    def top_similar_items(self, movieSim, similar_ns):
        '''
        For each item emit K closest items in comma separated file:
        1;2;0.6;1
        1;4;0.4;2
        '''
        
        #Check if hold out sample is used
        csv_Hold_Out_Sample = self.options.csv_Hold_Out_Sample          
        
        item_x, cos_sim, corr_sim, rating_diff = movieSim
        for item_y, n in similar_ns:
            #Yield hold-out data if exist:
            if csv_Hold_Out_Sample <> 'None':
                #Check if combination is in holdout sample, if so the result is emitted
                try:
                    if(self.lastmovieId.index(int(item_x)) == self.secondlastmovieId.index(int(item_y))):
                        yield '%d;%d;%f;%f;%f;%d;%f;%f;%f;%f;' % (item_x, item_y, cos_sim, corr_sim, float(rating_diff)*-1, n,
                                                      float(self.holdout[self.lastmovieId.index(int(item_x))][3]),  
                                                                   float(self.holdout[self.lastmovieId.index(int(item_x))][4]),
                                                                                float(self.holdout[self.lastmovieId.index(int(item_x))][5]),
                                                                                             float(self.holdout[self.lastmovieId.index(int(item_x))][6])), None
                except ValueError:
                    pass
                
                try:
                    if(self.lastmovieId.index(int(item_y)) == self.secondlastmovieId.index(int(item_x))):
                        yield '%d;%d;%f;%f;%f;%d;%f;%f;%f;%f;' % (item_x, item_y, cos_sim, corr_sim, rating_diff, n,
                                                      float(self.holdout[self.lastmovieId.index(int(item_x))][3]),  
                                                                   float(self.holdout[self.lastmovieId.index(int(item_x))][4]),
                                                                                float(self.holdout[self.lastmovieId.index(int(item_x))][5]),
                                                                                             float(self.holdout[self.lastmovieId.index(int(item_x))][6])), None
                except ValueError:
                    pass                                                        
            else:
                yield '%d;%d;%f;%f;%f;%d;' % (item_x, item_y, cos_sim, corr_sim, rating_diff, n), None
            
if __name__ == '__main__':
    MoviesSimilarities.run()
    
    