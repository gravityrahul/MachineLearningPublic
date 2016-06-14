# from mrjob.job import MRJob
# from mrjob.step import MRStep
# import re

 
# WORD_RE = re.compile(r"[\w']+")
 
# class MRWordFreqCount(MRJob):
#     def __init__(self, *args, **kwargs):
#         super(MRWordCountUtility, self).__init__(*args, **kwargs)
#         self.words = 0

#     def mapper(self, _, line):
#         # yield each word in the line
#         self.words += sum(1 for word in line.split() if word.strip())

# #     def combiner(self, word, counts):
# #         # sum the words we've seen so far
# #         yield (word, sum(counts))
        
# if __name__ == '__main__':
#     MRWordFreqCount.run()
    
    
from mrjob.job import MRJob


class MRWordCountUtility(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRWordCountUtility, self).__init__(*args, **kwargs)
        self.words = 0

    def mapper(self, _, line):
        # Don't actually yield anything for each line. Instead, collect them
        # and yield the sums when all lines have been processed. The results
        # will be collected by the reducer.
        self.words += sum(1 for word in line.split() if word.strip())

    def mapper_final(self):
        yield('words', self.words)

    def reducer(self, key, values):
        yield(key, sum(values))


if __name__ == '__main__':
    MRWordCountUtility.run()