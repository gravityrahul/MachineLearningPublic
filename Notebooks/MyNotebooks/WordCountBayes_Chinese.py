    
from mrjob.job import MRJob

class NaiveBayes(MRJob):

    def __init__(self, *args, **kwargs):
        super(NaiveBayes, self).__init__(*args, **kwargs)
        self.words = 0
        self.classLengthYes = 0
        self.classLengthNo = 0
    def mapper(self, _, line):
        # Don't actually yield anything for each line. Instead, collect them
        # and yield the sums when all lines have been processed. The results
        # will be collected by the reducer.
        token=line.split("\t")    #("d1", "1", "Chinese Japan", "addd...")
        numberOfTokensInSubjBody = len(token[2].split())+len(token[3].split())
        if token[0] == "D5":
            return
        
        if token[1] == "1":
            self.classLengthYes +=numberOfTokensInSubjBody
        else: 
            self.classLengthNo +=numberOfTokensInSubjBody
        
    def mapper_final(self):
        yield('classLengthYes', self.classLengthYes)
        yield('classLengthNo', self.classLengthNo)
        yield('words', self.words)
        
    def reducer(self, key, values):
        yield(key, sum(values))


if __name__ == '__main__':
    NaiveBayes.run()