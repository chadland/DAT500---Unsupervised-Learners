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
3b) Emits CSV item x, item y, cosine sim and number of co ratings. in CSV file

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

class MoviesSimilarities(MRJob):

    def steps(self):
        return [self.mr(self.group_by_user_rating,
                         self.count_ratings_users_freq),
                self.mr(self.pairwise_items, self.calculate_similarity),
                self.mr(self.calculate_ranking, self.top_similar_items)
                ]

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

        #Handle the lines in from the movie in table    
        userId, movieId, rating, timestamp = line.split(',')
        
        #Set ratings count to 1
        ratingCount=1 
        
        #Outut userId, movieId, rating, ratings_count 
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
        movieCount = 0
        movieSum = 0
        final = []
        for movieId, rating, ratingCount in values:
            movieCount += 1
            movieSum += rating
            final.append((movieId, rating, ratingCount))

        yield userId, (movieCount, movieSum, final)
        
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
        for item1, item2 in combinations(ratings, 2):
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
        n_x, n_y = 0, 0
        item_pair, co_ratings = moviePairKey, lines
        item_xname, item_yname = item_pair
        for item_x, item_y, nx_count, ny_count in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1
            n_x = int(ny_count)
            n_y = int(nx_count)

        #TODO, calculate other similarity measures
        cos_sim = sum_xy/math.sqrt(sum_xx*sum_yy)
        
        #Output movie x and movie y 
        yield (item_xname, item_yname), (cos_sim, n)
        
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
        cos_sim, n = values
        item_x, item_y = moviePairKey
        if int(n) > 0:
            yield (item_x, cos_sim), (item_y, n)

    def top_similar_items(self, movieSim, similar_ns):
        '''
        For each item emit K closest items in comma separated file:
        1;2;0.6;1
        1;4;0.4;2
        '''
        item_x, cos_sim = movieSim
        for item_y, n in similar_ns:
            yield '%d;%d;%f;%d;' % (item_x, item_y, cos_sim, n), None
if __name__ == '__main__':
    MoviesSimilarities.run()
