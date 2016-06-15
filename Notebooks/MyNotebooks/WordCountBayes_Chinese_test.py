    
from mrjob.job import MRJob

class NaiveBayes(MRJob):

    def __init__(self, *args, **kwargs):
        super(NaiveBayes, self).__init__(*args, **kwargs)
#         self.totWords = {}
#         self.eachWord = {}
        self.yesChinese = dict()
        self.noChinese = dict()
    
    def mapper(self, _, line):
        # Don't actually yield anything for each line. Instead, collect them
        # and yield the sums when all lines have been processed. The results
        # will be collected by the reducer.
        
        token=line.split("\t")    #("d1", "1", "Chinese Japan", "addd...")

        if token[0] == "D5":
            return
        
        tokens = token[2].split() + token[3].split()
        print(tokens)
        
        target_dict = self.yesChinese if token[1] == '1' else self.noChinese
        
        for t in tokens:
            if t not in target_dict:
                target_dict[t] = 0
            target_dict[t] += 1
       
    def mapper_final(self):
        yield('yesChinese', self.yesChinese)
        yield('noChinese', self.noChinese)
        
    def reducer(self, key, values):
        out = dict()
        total = 0
        for data in values:
            for k, count in data.items():
                if k not in out:
                    out[k] = 0
                out[k]+= count
                total += count
                
        for k in out.keys():
            out[k] /= total
        
        yield(key, out)
        

if __name__ == '__main__':
    NaiveBayes.run()