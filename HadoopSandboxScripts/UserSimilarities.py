# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 11:50:17 2015

@author: C202ACO
"""

#-*-coding: utf-8 -*-

'''
This module calculates similarities between movies using cosine similarity

1a) First the mapper emits the the userId, movieId, rating, timestamp using userId as key
1b) The reducer emits the userId and potings

'''
__author__ = 'Unsupervised Learners'

from mrjob.job import MRJob
import math
try:
    from itertools import combinations
except ImportError:
    from metrics import combinations

PRIOR_COUNT = 10
PRIOR_CORRELATION = 0

class UserSimilarities(MRJob):

    def steps(self):
        return [self.mr(self.group_by_movie_rating,
                         self.count_ratings_items_freq),
                self.mr(self.pairwise_users, self.calculate_similarity),
                self.mr(self.calculate_ranking, self.top_similar_users)
                ]

    def group_by_movie_rating(self, key, line):
        """
        Emit the movie_id and group by their ratings (user and rating), ratingCount will be used 
        for later calculation.
        
        17  70,3
        35  21,1
        49  19,2
        49  21,1
        49  70,4
        87  19,1
        87  21,2
        98  19,2
        """

        #Handle the lines in from the movie in table    
        userId, movieId, rating, timestamp = line.split(',')
        
        #Set ratings count to 1
        ratingCount=1 
        
        #Outut userId, movieId, rating, ratings_count 
        yield  int(movieId), (int(userId), float(rating), int(ratingCount))

    def count_ratings_items_freq(self, movieId, values):
        """
        For each movie, emit a row containing their "postings"
        (user,rating pairs)
        Also emit user rating sum and count for use in later steps.
        17    1,3,(70,3,1) (71,4,1)
        35    1,1,(21,1,1) (1,2,1)

        """
        movieCount = 0
        movieSum = 0
        final = []
        for userId, rating, ratingCount in values:
            movieCount += 1
            movieSum += rating
            final.append((userId, rating, ratingCount))

        yield movieId, (movieCount, movieSum, final)
        
        
    def pairwise_users(self, movieId, values):
        '''
        The output drops the movieId from the key entirely, instead it emits
        the pair of user as the key:
        1,2  (1,1,3,1
        19,70  (3,1,4,1)

        This mapper is quite slow.  One improvement
        would be to create a java Combiner to aggregate the
        outputs by key before writing to hdfs.
        '''
        movieCount, movieSum, ratings = values
        #print item_count, item_sum, [r for r in combinations(ratings, 2)]
        #bottleneck at combinations
        for user1, user2 in combinations(ratings, 2):
            yield (user1[0], user2[0]), \
                    (user1[1], user2[1], user1[2], user2[2])
        
    def calculate_similarity(self, userPairKey, lines):
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
        n_x, n_y = 0, 0
        user_pair, co_ratings = userPairKey, lines
        user_xname, user_yname = user_pair
        for user_x, user_y, nx_count, ny_count in lines:
            sum_xx += user_x * user_x
            sum_yy += user_y * user_y
            sum_xy += user_x * user_y
            sum_y += user_y
            sum_x += user_x
            n += 1
            n_x = int(ny_count)
            n_y = int(nx_count)

        #To do, calculate other similarity measures
        cos_sim = sum_xy/math.sqrt(sum_xx*sum_yy)
        
        #Output movie x and movie y 
        yield (user_xname, user_yname), (cos_sim, n)
        
    def calculate_ranking(self, userPairKey, values):
        '''
        Emit items with similarity in key for ranking:
        19,0.4    70,1
        19,0.6    21,2
        21,0.6    19,2
        21,0.9    70,1
        70,0.4    19,1
        70,0.9    21,1
        '''
        cos_sim, n = values
        user_x, user_y = userPairKey
        if int(n) > 0:
            yield (user_x, cos_sim), (user_y, n)

    def top_similar_users(self, userSim, similar_ns):
        '''
        For each item emit K closest items in comma separated file:
        1;2;0.6;1
        1;4;0.4;2
        '''
        user_x, cos_sim = userSim
        for user_y, n in similar_ns:
            yield '%d;%d;%f;%d;' % (user_x, user_y, cos_sim, n), None

if __name__ == '__main__':
    UserSimilarities.run()
