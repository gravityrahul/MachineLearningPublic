    
from mrjob.job import MRJob

class NaiveBayesTrainer(MRJob):

    def __init__(self, *args, **kwargs):
        super(NaiveBayesTrainer, self).__init__(*args, **kwargs)
        self.chars = 0
        self.words = 0
        self.lines = 0
        self.classLengthYes = 0
        self.classLengthNo = 0
    def mapper(self, _, line):
        tokens=line.split("\t") #[D1, "1", "Chinese Beijing Chinese"]
        numberTokensInSubjectBody = len(tokens[2].split())+len(tokens[3].split())
        if tokens[0] == "D5": #Skip test record
            return
        
        if tokens[1]=="1":
            self.classLengthYes +=numberTokensInSubjectBody
        else :
            self.classLengthNo +=numberTokensInSubjectBody

        self.words += sum(1 for word in line.split() if word.strip())

        
    def mapper_final(self):
        yield('TotalWords', self.words)
        yield('TotalClassYes', self.classLengthYes)
        yield('TotalClassNo', self.classLengthNo)


    def reducer(self, key, values):
        yield(key, sum(values))


if __name__ == '__main__':
    NaiveBayesTrainer.run()